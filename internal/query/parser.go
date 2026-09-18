package query

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

func Parse(input string) (result *Query, err error) {
	defer func() {
		if err == nil && result != nil {
			err = validateQuery(result)
			if err != nil {
				result = nil
			}
		}
	}()
	if strings.ContainsAny(input, "\r\n\x00\x1f") {
		return nil, fmt.Errorf("invalid query framing or unterminated quote")
	}
	operationID := ""
	if pos := strings.LastIndex(input, " __op="); pos >= 0 {
		candidate := input[pos+6:]
		if len(candidate) == 32 && strings.Trim(candidate, "0123456789abcdef") == "" {
			operationID = candidate
			input = input[:pos]
		}
	}
	tokens := tokenize(input)
	for _, token := range tokens {
		if strings.Contains(token, `"`) {
			if _, err := strconv.Unquote(token); err != nil {
				return nil, fmt.Errorf("invalid quoted value")
			}
		}
	}
	if len(tokens) == 0 {
		return nil, fmt.Errorf("empty query")
	}

	q := &Query{
		OperationID: operationID,
		Where:       make(map[string]string),
		Tags:        make(map[string]string),
		Limit:       10,
		Offset:      0,
		Order:       "DESC",
	}

	switch strings.ToUpper(tokens[0]) {
	case "STATS":
		q.Type = QueryTypeStats
		if len(tokens) < 2 {
			return nil, fmt.Errorf("STATS requires metric")
		}
		q.Metric = tokens[1]
		// The remaining arguments are tag keys.
		if len(tokens) > 2 && strings.ToUpper(tokens[2]) == "TAGS" {
			for _, token := range tokens[3:] {
				if strings.ToUpper(token) == "__LOCAL" {
					q.ForceLocal = true
					continue
				}
				q.TagKeys = append(q.TagKeys, token)
			}
		}
		return q, nil
	case "GROUP_LEADERBOARD":
		q.Type = QueryTypeGroupLeaderboard
		if len(tokens) < 2 {
			return nil, fmt.Errorf("missing metric")
		}
		q.Metric = tokens[1]
		i := 2
		var groups []GroupDef
		for i < len(tokens) {
			switch strings.ToUpper(tokens[i]) {
			case "__LOCAL":
				q.ForceLocal = true
				i++
			case "GROUP":
				if i+1 >= len(tokens) {
					return nil, fmt.Errorf("missing GROUP spec")
				}
				spec := tokenValue(tokens[i+1])
				name, members, err := parseGroupSpec(spec)
				if err != nil {
					return nil, err
				}
				groups = append(groups, GroupDef{Name: name, Members: members})
				i += 2
			case "FROM":
				if i+1 >= len(tokens) {
					return nil, fmt.Errorf("missing FROM value")
				}
				ts, err := parseTimeValue(tokens[i+1])
				if err != nil {
					return nil, err
				}
				q.From = ts
				i += 2
			case "TO":
				if i+1 >= len(tokens) {
					return nil, fmt.Errorf("missing TO value")
				}
				ts, err := parseTimeValue(tokens[i+1])
				if err != nil {
					return nil, err
				}
				q.To = ts
				i += 2
			case "LIMIT":
				if i+1 >= len(tokens) {
					return nil, fmt.Errorf("missing LIMIT value")
				}
				n, err := strconv.Atoi(tokens[i+1])
				if err != nil {
					return nil, fmt.Errorf("invalid LIMIT: %s", tokens[i+1])
				}
				q.Limit = n
				i += 2
			case "OFFSET":
				if i+1 >= len(tokens) {
					return nil, fmt.Errorf("missing OFFSET value")
				}
				n, err := strconv.Atoi(tokens[i+1])
				if err != nil {
					return nil, fmt.Errorf("invalid OFFSET: %s", tokens[i+1])
				}
				q.Offset = n
				i += 2
			case "ORDER":
				if i+1 >= len(tokens) {
					return nil, fmt.Errorf("missing ORDER value")
				}
				q.Order = strings.ToUpper(tokens[i+1])
				i += 2
			case "ENTITY":
				if i+1 >= len(tokens) {
					return nil, fmt.Errorf("missing ENTITY value")
				}
				q.EntityTag = tokenValue(tokens[i+1])
				i += 2
			default:
				return nil, fmt.Errorf("unknown keyword: %s", tokens[i])
			}
		}
		q.Groups = groups
		return q, nil
	case "WRITE":
		q.Type = QueryTypeWrite
		if len(tokens) < 3 {
			return nil, fmt.Errorf("WRITE requires metric and value")
		}
		q.Metric = tokens[1]
		val, err := strconv.ParseFloat(tokens[2], 64)
		if err != nil {
			return nil, fmt.Errorf("invalid value: %s", tokens[2])
		}
		q.Value = val

		i := 3
		for i < len(tokens) {
			key := tokens[i]

			// Check standalone flags first (no equals sign required).
			switch strings.ToUpper(key) {
			case "QUORUM":
				q.Quorum = true
				i++
				continue
			case "__REPLICA":
				q.IsReplica = true
				i++
				continue
			case "__LOCAL":
				q.ForceLocal = true
				i++
				continue
			}

			// rest braucht key = value format
			if i+2 >= len(tokens) {
				return nil, fmt.Errorf("incomplete WRITE argument")
			}
			if tokens[i+1] != "=" {
				return nil, fmt.Errorf("expected =")
			}
			value := tokenValue(tokens[i+2])

			switch key {
			case "lb":
				q.UpdateLB = true
				q.LBEntityID = value
			case "ts":
				ts, err := strconv.ParseInt(value, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid ts: %s", value)
				}
				q.Timestamp = ts
			default:
				q.Tags[key] = value
			}
			i += 3
		}
		return q, nil
	case "SET":
		q.Type = QueryTypeSet
		if len(tokens) < 3 {
			return nil, fmt.Errorf("SET requires metric and value")
		}
		q.Metric = tokens[1]
		val, err := strconv.ParseFloat(tokens[2], 64)
		if err != nil {
			return nil, fmt.Errorf("invalid value: %s", tokens[2])
		}
		q.Value = val
		q.Tags = make(map[string]string)

		i := 3
		for i < len(tokens) {
			switch strings.ToUpper(tokens[i]) {
			case "__REPLICA":
				q.IsReplica = true
				i++
				continue
			case "__LOCAL":
				q.ForceLocal = true
				i++
				continue
			}
			if i+2 >= len(tokens) {
				return nil, fmt.Errorf("incomplete SET argument")
			}
			key := tokens[i]
			if i+1 >= len(tokens) || tokens[i+1] != "=" {
				break
			}
			value := tokenValue(tokens[i+2])
			if key == "lb" {
				q.UpdateLB = true
				q.LBEntityID = value
			} else {
				q.Tags[key] = value
			}
			i += 3
		}
		return q, nil

	case "DELETE":
		q.Type = QueryTypeDelete
		if len(tokens) < 2 {
			return nil, fmt.Errorf("DELETE requires metric")
		}
		q.Metric = tokens[1]
		q.Tags = make(map[string]string)

		i := 2
		for i < len(tokens) {
			switch strings.ToUpper(tokens[i]) {
			case "__REPLICA":
				q.IsReplica = true
				i++
			case "__LOCAL":
				q.ForceLocal = true
				i++
			case "FROM":
				if i+1 >= len(tokens) {
					return nil, fmt.Errorf("missing FROM value")
				}
				ts, err := parseTimeValue(tokens[i+1])
				if err != nil {
					return nil, err
				}
				q.From = ts
				i += 2
			case "TO":
				if i+1 >= len(tokens) {
					return nil, fmt.Errorf("missing TO value")
				}
				ts, err := parseTimeValue(tokens[i+1])
				if err != nil {
					return nil, err
				}
				q.To = ts
				i += 2
			default:
				// tag
				if i+2 < len(tokens) && tokens[i+1] == "=" {
					key := tokens[i]
					value := tokenValue(tokens[i+2])
					if key == "lb" {
						q.UpdateLB = true
						q.LBEntityID = value
					} else {
						q.Tags[key] = value
					}
					i += 3
				} else {
					i++
				}
			}
		}
		return q, nil

	case "GET":
		q.Type = QueryTypeGet
	case "LEADERBOARD":
		q.Type = QueryTypeLeaderboard
	default:
		return nil, fmt.Errorf("unknown command: %s", tokens[0])
	}

	if len(tokens) < 2 {
		return nil, fmt.Errorf("missing metric")
	}
	q.Metric = tokens[1]

	if q.Type == QueryTypeGet {
		q.Metrics = splitMetrics(tokens[1])
		if len(q.Metrics) == 0 {
			return nil, fmt.Errorf("missing metric")
		}
		q.Metric = q.Metrics[0]
	} else if strings.Contains(tokens[1], ",") {
		return nil, fmt.Errorf("multiple metrics only supported for GET")
	}

	// Process keywords only for GET and LEADERBOARD.
	i := 2
	for i < len(tokens) {
		switch strings.ToUpper(tokens[i]) {
		case "__LOCAL":
			q.ForceLocal = true
			i++
		case "GROUP":
			// expect: GROUP BY <duration>
			if i+1 >= len(tokens) || strings.ToUpper(tokens[i+1]) != "BY" {
				return nil, fmt.Errorf("expected GROUP BY")
			}
			if i+2 >= len(tokens) {
				return nil, fmt.Errorf("missing GROUP BY value")
			}
			spec := tokenValue(tokens[i+2])
			q.GroupBySpec = spec
			dur, err := parseDurationSpec(spec)
			if err != nil {
				return nil, err
			}
			q.GroupBy = dur
			i += 3
		case "WHERE":
			// WHERE key = "value" AND key2 = "value2" AND ...
			i++
			for i < len(tokens) {
				if i+2 >= len(tokens) {
					return nil, fmt.Errorf("invalid WHERE clause")
				}
				key := tokens[i]
				// tokens[i+1] sollte "=" sein
				if tokens[i+1] != "=" {
					return nil, fmt.Errorf("expected = after %s", key)
				}
				value := tokenValue(tokens[i+2])
				q.Where[key] = value
				i += 3

				// Check whether AND follows.
				if i < len(tokens) && strings.ToUpper(tokens[i]) == "AND" {
					i++ // Skip AND; the next iteration handles the next key=value pair.
				} else {
					break // No AND; the WHERE clause is complete.
				}
			}
		case "FROM":
			if i+1 >= len(tokens) {
				return nil, fmt.Errorf("missing FROM value")
			}
			ts, err := parseTimeValue(tokens[i+1])
			if err != nil {
				return nil, err
			}
			q.From = ts
			i += 2
		case "COUNT", "SUM", "AVG":
			if q.Type != QueryTypeGet {
				return nil, fmt.Errorf("%s is only valid for GET", strings.ToUpper(tokens[i]))
			}
			q.Aggregate = AggType(strings.ToUpper(tokens[i]))
			i++
		case "TO":
			if i+1 >= len(tokens) {
				return nil, fmt.Errorf("missing TO value")
			}
			ts, err := parseTimeValue(tokens[i+1])
			if err != nil {
				return nil, err
			}
			q.To = ts
			i += 2
		case "LIMIT":
			if i+1 >= len(tokens) {
				return nil, fmt.Errorf("missing LIMIT value")
			}
			n, err := strconv.Atoi(tokens[i+1])
			if err != nil {
				return nil, fmt.Errorf("invalid LIMIT: %s", tokens[i+1])
			}
			q.Limit = n
			i += 2
		case "OFFSET":
			if i+1 >= len(tokens) {
				return nil, fmt.Errorf("missing OFFSET value")
			}
			n, err := strconv.Atoi(tokens[i+1])
			if err != nil {
				return nil, fmt.Errorf("invalid OFFSET: %s", tokens[i+1])
			}
			q.Offset = n
			i += 2
		case "ORDER":
			if i+1 >= len(tokens) {
				return nil, fmt.Errorf("missing ORDER value")
			}
			q.Order = strings.ToUpper(tokens[i+1])
			i += 2
		case "ENTITY":
			if q.Type != QueryTypeLeaderboard && q.Type != QueryTypeGroupLeaderboard {
				return nil, fmt.Errorf("ENTITY is only valid for LEADERBOARD")
			}
			if i+1 >= len(tokens) {
				return nil, fmt.Errorf("missing ENTITY value")
			}
			q.EntityTag = tokenValue(tokens[i+1])
			i += 2
		default:
			return nil, fmt.Errorf("unknown keyword: %s", tokens[i])
		}
	}

	return q, nil
}

func ParseWriteBatch(lines []string) ([]*Query, []BatchItemError) {
	queries := make([]*Query, 0, len(lines))
	var errors []BatchItemError
	for i, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		q, err := Parse(line)
		if err != nil {
			errors = append(errors, BatchItemError{Index: i, Error: err.Error()})
			continue
		}
		if q.Type != QueryTypeWrite {
			errors = append(errors, BatchItemError{Index: i, Error: "WRITE_BATCH only supports WRITE items"})
			continue
		}
		queries = append(queries, q)
	}
	return queries, errors
}

func tokenize(input string) []string {
	var tokens []string
	var current strings.Builder
	inQuote := false
	escaped := false

	for _, ch := range input {
		if escaped {
			current.WriteRune(ch)
			escaped = false
			continue
		}
		if inQuote && ch == '\\' {
			current.WriteRune(ch)
			escaped = true
			continue
		}
		switch {
		case ch == '"':
			inQuote = !inQuote
			current.WriteRune(ch)
		case (ch == ' ' || ch == '\n' || ch == '\t') && !inQuote:
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
		case ch == '=' && !inQuote:
			// = als eigener token
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
			tokens = append(tokens, "=")
		default:
			current.WriteRune(ch)
		}
	}
	if current.Len() > 0 {
		tokens = append(tokens, current.String())
	}

	return tokens
}

func parseTimeValue(token string) (int64, error) {
	token = tokenValue(token)
	if strings.HasPrefix(token, "now") {
		base := time.Now().UTC()
		if token == "now" {
			return base.UnixNano(), nil
		}
		if len(token) < 5 {
			return 0, fmt.Errorf("invalid relative time: %s", token)
		}
		op := token[3]
		if op != '-' && op != '+' {
			return 0, fmt.Errorf("invalid relative time: %s", token)
		}
		spec := token[4:]
		d, err := parseCalendarSpec(spec)
		if err != nil {
			return 0, err
		}
		return applyCalendarOffset(base, d, op).UnixNano(), nil
	}

	if t, err := time.Parse(time.RFC3339Nano, token); err == nil {
		return t.UnixNano(), nil
	}
	// fallback: YYYY-MM-DD
	t, err := time.Parse("2006-01-02", token)
	if err != nil {
		return 0, fmt.Errorf("invalid date: %s", token)
	}
	return t.UnixNano(), nil
}

func applyCalendarOffset(base time.Time, spec durSpec, op byte) time.Time {
	sign := 1
	if op == '-' {
		sign = -1
	}
	if spec.years != 0 || spec.months != 0 {
		base = base.AddDate(sign*spec.years, sign*spec.months, 0)
	}
	if spec.dur != 0 {
		base = base.Add(time.Duration(sign) * spec.dur)
	}
	return base
}

func parseGroupSpec(spec string) (string, []string, error) {
	parts := strings.SplitN(spec, ":", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" {
		return "", nil, fmt.Errorf("invalid GROUP spec: %s", spec)
	}
	groupName := strings.TrimSpace(parts[0])
	rawMembers := strings.Split(parts[1], ",")
	var members []string
	for _, m := range rawMembers {
		m = strings.TrimSpace(m)
		if m != "" {
			members = append(members, m)
		}
	}
	if len(members) == 0 {
		return "", nil, fmt.Errorf("GROUP has no members: %s", groupName)
	}
	return groupName, members, nil
}

func parseDurationSpec(spec string) (time.Duration, error) {
	d, err := parseCalendarSpec(spec)
	return d.dur, err
}

func validateQuery(q *Query) error {
	if q.Limit < 0 || q.Offset < 0 || q.Limit > 1000000 {
		return fmt.Errorf("invalid pagination (LIMIT must be between 0 and 1000000)")
	}
	if q.Order != "ASC" && q.Order != "DESC" {
		return fmt.Errorf("invalid ORDER")
	}
	if math.IsNaN(q.Value) || math.IsInf(q.Value, 0) {
		return fmt.Errorf("value must be finite")
	}
	if q.Timestamp < 0 || q.From < 0 || q.To < 0 || (q.To != 0 && q.From > q.To) {
		return fmt.Errorf("invalid time range")
	}
	for _, m := range append([]string{q.Metric}, q.Metrics...) {
		if m == "" || strings.ContainsAny(m, ",=\" \t\r\n") || m == "idx" || m == "lb" || m == "lb-entity" || m == "card" || m == "card-count" || m == "repl-applied" || m == "repl-outbox" {
			return fmt.Errorf("invalid or reserved metric: %s", m)
		}
	}
	for _, tags := range []map[string]string{q.Tags, q.Where} {
		for k := range tags {
			if k == "" || strings.ContainsAny(k, "=\" \t") {
				return fmt.Errorf("invalid tag key")
			}
		}
	}
	if q.EntityTag != "" && strings.ContainsAny(q.EntityTag, "=\" \t") {
		return fmt.Errorf("invalid entity tag")
	}
	if q.Type == QueryTypeDelete && len(q.Tags) > 0 {
		return fmt.Errorf("DELETE supports lb and time ranges only")
	}
	if q.Type == QueryTypeSet && (!q.UpdateLB || q.LBEntityID == "") {
		return fmt.Errorf("SET requires lb")
	}
	return nil
}

func splitMetrics(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func tokenValue(token string) string {
	if strings.HasPrefix(token, `"`) {
		value, err := strconv.Unquote(token)
		if err == nil {
			return value
		}
	}
	return token
}
