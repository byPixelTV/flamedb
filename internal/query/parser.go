package query

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

func Parse(input string) (*Query, error) {
	tokens := tokenize(input)
	if len(tokens) == 0 {
		return nil, fmt.Errorf("empty query")
	}

	q := &Query{
		Where:  make(map[string]string),
		Tags:   make(map[string]string),
		Limit:  10,
		Offset: 0,
		Order:  "DESC",
	}

	switch strings.ToUpper(tokens[0]) {
	case "STATS":
		q.Type = QueryTypeStats
		if len(tokens) < 2 {
			return nil, fmt.Errorf("STATS requires metric")
		}
		q.Metric = tokens[1]
		// rest sind tag keys
		if len(tokens) > 2 && strings.ToUpper(tokens[2]) == "TAGS" {
			q.TagKeys = tokens[3:]
		}
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

			// standalone flags zuerst checken (kein = nötig)
			switch strings.ToUpper(key) {
			case "QUORUM":
				q.Quorum = true
				i++
				continue
			case "__REPLICA":
				q.IsReplica = true
				i++
				continue
			}

			// rest braucht key = value format
			if i+2 >= len(tokens) {
				break
			}
			if tokens[i+1] != "=" {
				break
			}
			value := strings.Trim(tokens[i+2], `"`)

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
			if i+2 > len(tokens) {
				break
			}
			key := tokens[i]
			if i+1 >= len(tokens) || tokens[i+1] != "=" {
				break
			}
			value := strings.Trim(tokens[i+2], `"`)
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
			case "FROM":
				t, err := time.Parse("2006-01-02", tokens[i+1])
				if err != nil {
					return nil, fmt.Errorf("invalid FROM date: %s", tokens[i+1])
				}
				q.From = t.UnixNano()
				i += 2
			case "TO":
				t, err := time.Parse("2006-01-02", tokens[i+1])
				if err != nil {
					return nil, fmt.Errorf("invalid TO date: %s", tokens[i+1])
				}
				q.To = t.UnixNano()
				i += 2
			default:
				// tag
				if i+2 < len(tokens) && tokens[i+1] == "=" {
					key := tokens[i]
					value := strings.Trim(tokens[i+2], `"`)
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

	// keyword loop nur für GET und LEADERBOARD
	i := 2
	for i < len(tokens) {
		switch strings.ToUpper(tokens[i]) {
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
				value := strings.Trim(tokens[i+2], `"`)
				q.Where[key] = value
				i += 3

				// check ob AND folgt
				if i < len(tokens) && strings.ToUpper(tokens[i]) == "AND" {
					i++ // AND überspringen, nächste iteration macht nächstes key=value
				} else {
					break // kein AND, WHERE clause fertig
				}
			}
		case "FROM":
			if i+1 >= len(tokens) {
				return nil, fmt.Errorf("missing FROM value")
			}
			t, err := time.Parse("2006-01-02", tokens[i+1])
			if err != nil {
				return nil, fmt.Errorf("invalid FROM date: %s", tokens[i+1])
			}
			q.From = t.UnixNano()
			i += 2
		case "TO":
			if i+1 >= len(tokens) {
				return nil, fmt.Errorf("missing TO value")
			}
			t, err := time.Parse("2006-01-02", tokens[i+1])
			if err != nil {
				return nil, fmt.Errorf("invalid TO date: %s", tokens[i+1])
			}
			q.To = t.UnixNano()
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
		default:
			return nil, fmt.Errorf("unknown keyword: %s", tokens[i])
		}
	}

	return q, nil
}

func tokenize(input string) []string {
	var tokens []string
	var current strings.Builder
	inQuote := false

	for _, ch := range input {
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
