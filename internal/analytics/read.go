package analytics

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"

	"github.com/byPixelTV/flamedb/internal/query"
	"github.com/byPixelTV/flamedb/internal/storage"
	"github.com/byPixelTV/flamedb/internal/types"
)

func (e *Engine) listMetrics(ctx context.Context, where map[string]string) ([]string, error) {
	tags, _ := json.Marshal(EncodeTags(where))
	if where == nil {
		tags = []byte(`{}`)
	}
	sql := `SELECT metric FROM flame_metrics m WHERE EXISTS(SELECT 1 FROM flame_events e WHERE e.metric=m.metric) OR EXISTS(SELECT 1 FROM flame_scores s WHERE s.metric=m.metric) ORDER BY metric`
	args := []any{}
	if len(where) > 0 {
		sql = `SELECT metric FROM flame_metrics m WHERE EXISTS(SELECT 1 FROM flame_events e WHERE e.metric=m.metric AND e.tags @> $1::jsonb) ORDER BY metric`
		args = append(args, string(tags))
	}
	rows, err := e.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []string{}
	bytes := 0
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			return nil, err
		}
		bytes += len(name) + 32
		if bytes > e.maxBytes {
			return nil, fmt.Errorf("metric listing exceeds configured byte limit")
		}
		name, err = DecodeString(name)
		if err != nil {
			return nil, err
		}
		out = append(out, name)
		if len(out) > e.max {
			return nil, fmt.Errorf("metric result exceeds configured limit %d", e.max)
		}
	}
	sort.Strings(out)
	return out, rows.Err()
}
func (e *Engine) stats(ctx context.Context, q *query.Query) (*query.Result, error) {
	if len(q.TagKeys) > e.max {
		return nil, fmt.Errorf("tag stats exceed configured limit")
	}
	bytes := len(q.Metric) + 64
	if bytes > e.maxBytes {
		return nil, fmt.Errorf("tag stats exceed configured byte limit")
	}
	out := &query.StatsResult{Metric: q.Metric, TagStats: []storage.TagStats{}}
	for _, key := range q.TagKeys {
		bytes += len(key) + 64
		if bytes > e.maxBytes {
			return nil, fmt.Errorf("tag stats exceed configured byte limit")
		}
		var count uint64
		if err := e.db.QueryRow(ctx, `SELECT count(*) FROM flame_tag_values WHERE flame_identity_hash(metric)=flame_identity_hash($1) AND metric=$1 AND flame_identity_hash(tag)=flame_identity_hash($2) AND tag=$2`, EncodeString(q.Metric), EncodeString(key)).Scan(&count); err != nil {
			return nil, err
		}
		out.TagStats = append(out.TagStats, storage.TagStats{TagKey: key, Cardinality: count})
	}
	return &query.Result{Stats: out}, nil
}
func (e *Engine) events(ctx context.Context, q *query.Query, metric string, aggregate bool) ([]storage.Event, error) {
	to := q.To
	if to == 0 {
		to = math.MaxInt64
	}
	tags, _ := json.Marshal(EncodeTags(q.Where))
	if q.Where == nil {
		tags = []byte(`{}`)
	}
	order := "ASC"
	if q.Order == "DESC" && !aggregate {
		order = "DESC"
	}
	limit, offset := q.Limit, q.Offset
	if aggregate {
		limit, offset = 0, 0
	}
	if offset > e.max {
		return nil, fmt.Errorf("offset exceeds configured scan limit %d", e.max)
	}
	if limit == 0 || limit > e.max {
		limit = e.max + 1
	}
	rows, err := e.db.Query(ctx, `SELECT ts,value,tags FROM flame_events WHERE flame_identity_hash(metric)=flame_identity_hash($1) AND metric=$1 AND ts >= $2 AND ts < $3 AND tags @> $4::jsonb ORDER BY ts `+order+` LIMIT $5 OFFSET $6`, EncodeString(metric), q.From, to, string(tags), limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []storage.Event{}
	bytes := 0
	for rows.Next() {
		event := storage.Event{Metric: metric}
		var raw []byte
		if err = rows.Scan(&event.Timestamp, &event.Value, &raw); err != nil {
			return nil, err
		}
		bytes += len(raw) + len(metric) + 64
		if bytes > e.maxBytes {
			return nil, fmt.Errorf("event result exceeds configured byte limit %d", e.maxBytes)
		}
		if err = json.Unmarshal(raw, &event.Tags); err != nil {
			return nil, err
		}
		event.Tags, err = decodeTags(event.Tags)
		if err != nil {
			return nil, err
		}
		out = append(out, event)
		if len(out) > e.max {
			return nil, fmt.Errorf("event scan exceeds configured limit %d; narrow the range", e.max)
		}
	}
	return out, rows.Err()
}
func (e *Engine) get(ctx context.Context, q *query.Query) (*query.Result, error) {
	metrics := q.Metrics
	if len(metrics) == 0 {
		metrics = []string{q.Metric}
	}
	if len(metrics) > e.max {
		return nil, fmt.Errorf("metric count exceeds configured limit %d", e.max)
	}
	r := &query.Result{}
	if len(metrics) > 1 {
		if q.GroupBySpec != "" {
			r.SeriesByMetric = map[string][]query.SeriesPoint{}
		} else if q.Aggregate != "" {
			r.Aggregates = map[string]*query.AggregateResult{}
		} else {
			r.Metrics = map[string][]storage.Event{}
		}
	}
	total := 0
	bytes := 0
	for _, metric := range metrics {
		bytes += len(metric) + 128
		if bytes > e.maxBytes {
			return nil, fmt.Errorf("combined result exceeds configured byte limit")
		}
		if q.GroupBySpec != "" {
			series, err := e.series(ctx, q, metric)
			if err != nil {
				return nil, err
			}
			if len(metrics) == 1 {
				r.Series = series
			} else {
				r.SeriesByMetric[metric] = series
			}
			bytes += len(series) * 128
			if bytes > e.maxBytes {
				return nil, fmt.Errorf("combined series exceeds configured byte limit")
			}
			continue
		}
		if q.Aggregate != "" {
			agg, err := e.aggregate(ctx, q, metric)
			if err != nil {
				return nil, err
			}
			if len(metrics) == 1 {
				r.Aggregate = agg
			} else {
				r.Aggregates[metric] = agg
			}
			continue
		}
		events, err := e.events(ctx, q, metric, false)
		if err != nil {
			return nil, err
		}
		total += len(events)
		for _, event := range events {
			bytes += len(event.Metric) + 64
			for key, value := range event.Tags {
				bytes += len(key) + len(value) + 16
			}
		}
		if bytes > e.maxBytes {
			return nil, fmt.Errorf("combined event result exceeds configured byte limit %d", e.maxBytes)
		}
		if total > e.max {
			return nil, fmt.Errorf("combined event scan exceeds configured limit %d", e.max)
		}
		if len(metrics) == 1 {
			r.Events = events
		} else {
			r.Metrics[metric] = events
		}
	}
	return r, nil
}

// series streams history in bounded chunks. The shared calendar helper identifies
// bucket boundaries; values accumulate in original timestamp order even when a
// bucket crosses a chunk boundary, preserving float64 and AVG semantics exactly.
func (e *Engine) series(ctx context.Context, q *query.Query, metric string) ([]query.SeriesPoint, error) {
	to := q.To
	if to == 0 {
		to = math.MaxInt64
	}
	tags, _ := json.Marshal(EncodeTags(q.Where))
	rows, err := e.db.Query(ctx, `SELECT ts,value FROM flame_events WHERE flame_identity_hash(metric)=flame_identity_hash($1) AND metric=$1 AND ts >= $2 AND ts < $3 AND tags @> $4::jsonb ORDER BY ts`, EncodeString(metric), q.From, to, string(tags))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	points := []query.SeriesPoint{}
	sums := []float64{}
	chunk := make([]storage.Event, 0, 512)
	flush := func() error {
		buckets := query.AggregateSeriesUTC(chunk, q.GroupBySpec, query.AggCount)
		index := 0
		for _, bucket := range buckets {
			if len(points) == 0 || points[len(points)-1].TS != bucket.TS {
				points = append(points, query.SeriesPoint{TS: bucket.TS})
				sums = append(sums, 0)
				if len(points) > e.max {
					return fmt.Errorf("series points exceed configured limit %d", e.max)
				}
				if len(points) > e.maxBytes/128 {
					return fmt.Errorf("series exceeds configured byte limit")
				}
			}
			last := len(points) - 1
			for j := 0; j < bucket.Count; j++ {
				sums[last] += chunk[index].Value
				points[last].Count++
				index++
			}
		}
		chunk = chunk[:0]
		return nil
	}
	for rows.Next() {
		var event storage.Event
		if err = rows.Scan(&event.Timestamp, &event.Value); err != nil {
			return nil, err
		}
		chunk = append(chunk, event)
		if len(chunk) == cap(chunk) {
			if err = flush(); err != nil {
				return nil, err
			}
		}
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if len(chunk) > 0 {
		if err = flush(); err != nil {
			return nil, err
		}
	}
	for i := range points {
		value := sums[i]
		if q.Aggregate == query.AggCount {
			value = float64(points[i].Count)
		} else if q.Aggregate == query.AggAvg && points[i].Count > 0 {
			value /= float64(points[i].Count)
		}
		if math.IsInf(value, 0) || math.IsNaN(value) {
			return nil, fmt.Errorf("response serialization failed")
		}
		points[i].Value = value
	}
	return points, nil
}
func sortEntries(entries []types.LeaderboardEntry, legacy bool) {
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Value != entries[j].Value {
			return entries[i].Value > entries[j].Value
		}
		if legacy && entries[i].Value < 0 {
			return entries[i].EntityID > entries[j].EntityID
		}
		return entries[i].EntityID < entries[j].EntityID
	})
}
func (e *Engine) window(ctx context.Context, q *query.Query) ([]types.LeaderboardEntry, error) {
	if q.EntityTag == "" {
		return nil, fmt.Errorf("%s mit FROM/TO erfordert ENTITY <tag-key>", q.Type)
	}
	// Ordered SUM preserves the reference's exact float64 accumulation order.
	// PostgreSQL aggregates history; only entity results cross the network.
	to := q.To
	if to == 0 {
		to = math.MaxInt64
	}
	limit, offset := e.max+1, 0
	if q.Type == query.QueryTypeLeaderboard {
		offset = q.Offset
		if q.Limit > 0 {
			limit = q.Limit
		}
	}
	var selected []string
	if q.Type == query.QueryTypeGroupLeaderboard {
		selected = []string{}
		seen := map[string]bool{}
		members := 0
		for _, group := range q.Groups {
			for _, id := range group.Members {
				members++
				if members > e.max {
					return nil, fmt.Errorf("group members exceed configured limit")
				}
				if !seen[id] {
					seen[id] = true
					selected = append(selected, EncodeString(id))
				}
			}
		}
	}
	rows, err := e.db.Query(ctx, `SELECT entity,total FROM (SELECT tags->>$4 AS entity,sum(value ORDER BY ts) AS total FROM flame_events WHERE flame_identity_hash(metric)=flame_identity_hash($1) AND metric=$1 AND ts >= $2 AND ts < $3 AND tags ? $4 AND (($6::text[] IS NULL AND tags->>$4 <> '') OR tags->>$4=ANY($6::text[])) GROUP BY tags->>$4) scores ORDER BY total DESC,flame_entity_bytes(entity) LIMIT $5 OFFSET $7`, EncodeString(q.Metric), q.From, to, EncodeString(q.EntityTag), limit, selected, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	entries := []types.LeaderboardEntry{}
	bytes := 0
	for rows.Next() {
		var entry types.LeaderboardEntry
		if err = rows.Scan(&entry.EntityID, &entry.Value); err != nil {
			return nil, err
		}
		bytes += len(entry.EntityID) + 64
		if bytes > e.maxBytes {
			return nil, fmt.Errorf("window result exceeds configured byte limit")
		}
		entry.EntityID, err = DecodeString(entry.EntityID)
		if err != nil {
			return nil, err
		}
		if math.IsNaN(entry.Value) || math.IsInf(entry.Value, 0) {
			return nil, fmt.Errorf("response serialization failed")
		}
		entries = append(entries, entry)
		if len(entries) > e.max {
			return nil, fmt.Errorf("window entities exceed configured limit %d", e.max)
		}
	}
	return entries, rows.Err()
}

func (e *Engine) aggregate(ctx context.Context, q *query.Query, metric string) (*query.AggregateResult, error) {
	to := q.To
	if to == 0 {
		to = math.MaxInt64
	}
	tags, _ := json.Marshal(EncodeTags(q.Where))
	if q.Where == nil {
		tags = []byte(`{}`)
	}
	expression := `COALESCE(sum(value ORDER BY ts),0)`
	if q.Aggregate == query.AggCount {
		expression = `0::double precision`
	}
	var count int
	var value float64
	err := e.db.QueryRow(ctx, `SELECT count(*),`+expression+` FROM flame_events WHERE flame_identity_hash(metric)=flame_identity_hash($1) AND metric=$1 AND ts >= $2 AND ts < $3 AND tags @> $4::jsonb`, EncodeString(metric), q.From, to, string(tags)).Scan(&count, &value)
	if err != nil {
		return nil, err
	}
	if q.Aggregate == query.AggCount {
		value = float64(count)
	} else if q.Aggregate == query.AggAvg && count > 0 {
		value /= float64(count)
	}
	if value == 0 {
		value = 0
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return nil, fmt.Errorf("response serialization failed")
	}
	return &query.AggregateResult{Type: string(q.Aggregate), Value: value, Count: count}, nil
}
func page(entries []types.LeaderboardEntry, limit, offset int) []types.LeaderboardEntry {
	if offset >= len(entries) {
		return []types.LeaderboardEntry{}
	}
	entries = entries[offset:]
	if limit > 0 && len(entries) > limit {
		entries = entries[:limit]
	}
	return entries
}
func rankingResult(q *query.Query, entries []types.LeaderboardEntry) (*query.Result, error) {
	rank := &query.RankingResult{Count: int64(len(entries))}
	if q.Type == query.QueryTypeAround {
		rank.Around = []query.RankedEntry{}
	}
	var index = -1
	for i, entry := range entries {
		if entry.EntityID == q.EntityID {
			index = i
			value := entry.Value
			position := int64(i + 1)
			rank.Score = &value
			rank.Rank = &position
			break
		}
	}
	if q.Type == query.QueryTypeAround && index >= 0 {
		start, end := index-q.Radius, index+q.Radius+1
		if start < 0 {
			start = 0
		}
		if end > len(entries) {
			end = len(entries)
		}
		for i := start; i < end; i++ {
			rank.Around = append(rank.Around, query.RankedEntry{EntityID: entries[i].EntityID, Value: entries[i].Value, Rank: int64(i + 1)})
		}
	}
	return &query.Result{Ranking: rank}, nil
}
