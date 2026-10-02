package query

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/byPixelTV/flamedb/internal/aggregates"
	"github.com/byPixelTV/flamedb/internal/keyspace"
	"github.com/byPixelTV/flamedb/internal/storage"
	"github.com/byPixelTV/flamedb/internal/types"
	"github.com/cockroachdb/pebble/v2"
)

type Executor struct {
	clock atomic.Int64
	store *storage.Storage
	lb    *aggregates.Leaderboard
	cache *storage.LeaderboardCache
}

func NewExecutor(store *storage.Storage, lb *aggregates.Leaderboard) *Executor {
	return &Executor{
		store: store,
		lb:    lb,
		cache: storage.NewLeaderboardCache(1 * time.Second),
	}
}

func (e *Executor) Execute(q *Query) (*Result, error) {
	switch q.Type {
	case QueryTypeMetrics:
		keys, err := e.ListMetrics(q.Where)
		return &Result{MetricKeys: keys}, err
	case QueryTypeWrite:
		return e.executeWrite(q)
	case QueryTypeSet:
		return e.executeSet(q)
	case QueryTypeDelete:
		return e.executeDelete(q)
	case QueryTypeLeaderboard:
		return e.executeLeaderboard(q)
	case QueryTypeGet:
		return e.executeGet(q)
	case QueryTypeStats:
		return e.executeStats(q)
	case QueryTypeGroupLeaderboard:
		return e.executeGroupLeaderboard(q)
	}
	return nil, nil
}

func (e *Executor) ExecuteBatch(queries []*Query) (*BatchResult, error) {
	result := &BatchResult{OK: true}
	var events []storage.Event
	var receipts []string
	var indices []int
	fail := func(index int, err error) {
		result.Failed++
		result.Errors = append(result.Errors, BatchItemError{Index: index, Error: err.Error()})
	}
	flush := func() {
		if len(events) == 0 {
			return
		}
		if err := e.store.WriteEventsWithReceipts(events, receipts, true); err != nil {
			for _, i := range indices {
				fail(i, err)
			}
		} else {
			result.Accepted += len(events)
		}
		events = nil
		receipts = nil
		indices = nil
	}
	for i, q := range queries {
		if q == nil || q.Type != QueryTypeWrite {
			fail(i, fmt.Errorf("batch only supports WRITE"))
			continue
		}
		if math.IsNaN(q.Value) || math.IsInf(q.Value, 0) || q.Timestamp < 0 {
			fail(i, fmt.Errorf("invalid write value or timestamp"))
			continue
		}
		if q.Timestamp == 0 {
			q.Timestamp = e.nextTimestamp()
		}
		if q.UpdateLB || len(q.LBEntities) > 0 {
			flush()
			if _, err := e.executeMutation(q); err != nil {
				fail(i, err)
			} else {
				result.Accepted++
			}
			continue
		}
		events = append(events, storage.Event{Metric: q.Metric, Timestamp: q.Timestamp, Value: q.Value, Tags: q.Tags})
		receipt := ""
		if q.IsReplica {
			receipt = q.OperationID
		}
		receipts = append(receipts, receipt)
		indices = append(indices, i)
	}
	flush()
	result.OK = result.Failed == 0
	return result, nil
}

func (e *Executor) executeGroupLeaderboard(q *Query) (*Result, error) {
	if len(q.Groups) == 0 {
		return &Result{Leaderboard: []types.LeaderboardEntry{}}, nil
	}

	// Windowed query: FROM or TO is set; read entity sums from the index.
	if q.From != 0 || q.To != 0 {
		entityTag := q.EntityTag
		if entityTag == "" {
			return nil, fmt.Errorf("GROUP_LEADERBOARD mit FROM/TO erfordert ENTITY <tag-key>")
		}
		to := q.To
		if to == 0 {
			to = math.MaxInt64
		}

		// Collect and deduplicate member IDs across all groups.
		allMembers := make([]string, 0)
		seen := make(map[string]struct{})
		for _, g := range q.Groups {
			for _, m := range g.Members {
				if _, ok := seen[m]; !ok {
					seen[m] = struct{}{}
					allMembers = append(allMembers, m)
				}
			}
		}

		// Targeted index scans per member using the known IDs.
		memberSums, err := e.store.WindowedEntitySums(q.Metric, entityTag, allMembers, q.From, to)
		if err != nil {
			return nil, err
		}

		entries := make([]types.LeaderboardEntry, 0, len(q.Groups))
		for _, g := range q.Groups {
			var sum float64
			for _, member := range g.Members {
				sum += memberSums[member]
			}
			entries = append(entries, types.LeaderboardEntry{
				EntityID: g.Name,
				Value:    sum,
			})
		}

		sort.SliceStable(entries, func(i, j int) bool {
			if entries[i].Value != entries[j].Value {
				return entries[i].Value > entries[j].Value
			}
			return entries[i].EntityID < entries[j].EntityID
		})

		if q.Offset >= len(entries) {
			return &Result{Leaderboard: []types.LeaderboardEntry{}}, nil
		}
		entries = entries[q.Offset:]
		if q.Limit > 0 && len(entries) > q.Limit {
			entries = entries[:q.Limit]
		}
		return &Result{Leaderboard: entries}, nil
	}

	// All-time query: pre-aggregierter Index
	entries := make([]types.LeaderboardEntry, 0, len(q.Groups))
	for _, g := range q.Groups {
		var sum float64
		for _, member := range g.Members {
			v, _ := e.lb.Get(aggregates.BoardMetric(q.Metric, q.Board), member)
			sum += v
		}
		entries = append(entries, types.LeaderboardEntry{
			EntityID: g.Name,
			Value:    sum,
		})
	}

	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].Value != entries[j].Value {
			return entries[i].Value > entries[j].Value
		}
		return entries[i].EntityID < entries[j].EntityID
	})

	if q.Offset >= len(entries) {
		return &Result{Leaderboard: []types.LeaderboardEntry{}}, nil
	}
	entries = entries[q.Offset:]
	if q.Limit > 0 && len(entries) > q.Limit {
		entries = entries[:q.Limit]
	}
	return &Result{Leaderboard: entries}, nil
}

func (e *Executor) executeStats(q *Query) (*Result, error) {
	stats := e.store.GetTagStats(q.Metric, q.TagKeys)
	return &Result{
		Stats: &StatsResult{
			Metric:   q.Metric,
			TagStats: stats,
		},
	}, nil
}

func (e *Executor) GetAllMetrics() []string {
	metrics, _ := e.listAllMetrics()
	return metrics
}

// ListMetrics returns sorted metric names with at least one matching event when filtered.
func (e *Executor) ListMetrics(where map[string]string) ([]string, error) {
	metrics, err := e.listAllMetrics()
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(metrics))
	for _, metric := range metrics {
		if len(where) > 0 {
			result, err := e.executeGet(&Query{Metric: metric, Where: where, Limit: 1, Order: "DESC"})
			if err != nil {
				return nil, err
			}
			if len(result.Events) == 0 {
				continue
			}
		}
		out = append(out, metric)
	}
	sort.Strings(out)
	return out, nil
}

func (e *Executor) listAllMetrics() ([]string, error) {
	// Scan all keys and extract metric names.
	iter, err := e.store.DB().NewIter(&pebble.IterOptions{})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	seen := make(map[string]bool)
	var metrics []string

	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		if strings.HasPrefix(key, "lb-entity:") {
			parts := strings.SplitN(strings.TrimPrefix(key, "lb-entity:"), ":", 2)
			if len(parts) == 2 {
				parts[0] = keyspace.DecodeComponent(parts[0])
				parts[0] = strings.SplitN(parts[0], "\x1f", 2)[0]
				if !seen[parts[0]] {
					seen[parts[0]] = true
					metrics = append(metrics, parts[0])
				}
			}
		}
		// Skip index and leaderboard keys.
		if strings.HasPrefix(key, "idx:") ||
			strings.HasPrefix(key, "lb:") ||
			strings.HasPrefix(key, "card:") || strings.HasPrefix(key, "card-count:") || strings.HasPrefix(key, "lb-entity:") || strings.HasPrefix(key, "repl-outbox:") || strings.HasPrefix(key, "repl-applied:") {
			continue
		}
		// The metric name precedes the first colon.
		parts := strings.SplitN(key, ":", 2)
		parts[0] = keyspace.DecodeComponent(parts[0])
		if len(parts) > 0 && !seen[parts[0]] {
			seen[parts[0]] = true
			metrics = append(metrics, parts[0])
		}
	}
	return metrics, iter.Error()
}

func (e *Executor) executeSet(q *Query) (*Result, error)    { return e.executeMutation(q) }
func (e *Executor) executeDelete(q *Query) (*Result, error) { return e.executeMutation(q) }
func (e *Executor) executeWrite(q *Query) (*Result, error)  { return e.executeMutation(q) }

func (e *Executor) executeMutation(q *Query) (*Result, error) {
	if q.Timestamp == 0 && q.Type == QueryTypeWrite {
		q.Timestamp = e.nextTimestamp()
	}
	event := storage.Event{Metric: q.Metric, Timestamp: q.Timestamp, Value: q.Value, Tags: q.Tags}
	deleted := &DeleteResult{DryRun: q.DryRun}
	applied := false
	boards := make(map[string]string, len(q.LBEntities)+1)
	if q.UpdateLB {
		boards[""] = q.LBEntityID
	}
	for board, entity := range q.LBEntities {
		boards[board] = entity
	}
	locks := make([]string, 0, len(boards))
	for board, entity := range boards {
		locks = append(locks, aggregates.BoardMetric(q.Metric, board)+":"+entity)
	}
	if len(locks) == 0 {
		locks = append(locks, q.Metric+":")
	}
	err := e.lb.AtomicMutationMany(locks, func(batch *pebble.Batch) error {
		receipt := []byte("repl-applied:" + q.OperationID)
		if q.IsReplica && q.OperationID != "" {
			_, closer, err := e.store.DB().Get(receipt)
			if err == nil {
				closer.Close()
				return nil
			}
			if err != pebble.ErrNotFound {
				return err
			}
		}
		switch q.Type {
		case QueryTypeWrite:
			if err := e.store.StageEvent(batch, event); err != nil {
				return err
			}
			for board, entity := range boards {
				metric := aggregates.BoardMetric(q.Metric, board)
				current, err := e.lb.Get(metric, entity)
				if err != nil {
					return err
				}
				if err := e.lb.StageValue(batch, metric, entity, current+q.Value, false); err != nil {
					return err
				}
			}
		case QueryTypeSet:
			for board, entity := range boards {
				if err := e.lb.StageValue(batch, aggregates.BoardMetric(q.Metric, board), entity, q.Value, false); err != nil {
					return err
				}
			}
		case QueryTypeDelete:
			for board, entity := range boards {
				metric := aggregates.BoardMetric(q.Metric, board)
				exists, err := e.lb.Exists(metric, entity)
				if err != nil {
					return err
				}
				if exists {
					deleted.LeaderboardEntries++
				}
				if !q.DryRun {
					if err := e.lb.StageValue(batch, metric, entity, 0, true); err != nil {
						return err
					}
				}
			}
			if len(boards) == 0 || len(q.Where) > 0 {
				to := q.To
				if to == 0 {
					to = math.MaxInt64
				}
				target := batch
				if q.DryRun {
					target = nil
				}
				count, err := e.store.ScanDeleteWhere(target, q.Metric, q.From, to, q.Where)
				if err != nil {
					return err
				}
				deleted.Events = count
			}
		}
		if q.IsReplica && q.OperationID != "" {
			if err := batch.Set(receipt, []byte{1}, nil); err != nil {
				return err
			}
		}
		applied = true
		return nil
	})
	if err != nil {
		return nil, err
	}
	if applied && !q.DryRun {
		if q.Type == QueryTypeWrite {
			e.store.RecordCommittedEvent(event)
		}
		e.cache.Invalidate(q.Metric)
	}
	if q.Type == QueryTypeDelete {
		return &Result{Delete: deleted}, nil
	}
	return &Result{}, nil
}

func (e *Executor) executeLeaderboard(q *Query) (*Result, error) {
	// Windowed query: FROM or TO is set; compute from the secondary index.
	if q.From != 0 || q.To != 0 {
		entityTag := q.EntityTag
		if entityTag == "" {
			return nil, fmt.Errorf("LEADERBOARD mit FROM/TO erfordert ENTITY <tag-key>")
		}
		to := q.To
		if to == 0 {
			to = math.MaxInt64
		}
		entries, err := e.store.WindowedLeaderboard(q.Metric, entityTag, q.From, to, q.Limit, q.Offset)
		if err != nil {
			return nil, err
		}
		return &Result{Leaderboard: entries}, nil
	}

	// All-time query: pre-aggregierter Index → cache

	entries, err := e.lb.TopN(aggregates.BoardMetric(q.Metric, q.Board), q.Limit, q.Offset)
	if err != nil {
		return nil, err
	}
	if entries == nil {
		entries = []types.LeaderboardEntry{}
	}

	return &Result{Leaderboard: entries}, nil
}

func (e *Executor) executeGet(q *Query) (*Result, error) {
	to := q.To
	if to == 0 {
		to = math.MaxInt64
	}
	metrics := q.Metrics
	if len(metrics) == 0 {
		metrics = []string{q.Metric}
	}
	result := &Result{}
	if len(metrics) > 1 {
		switch {
		case q.GroupBySpec != "":
			result.SeriesByMetric = make(map[string][]SeriesPoint)
		case q.Aggregate != "":
			result.Aggregates = make(map[string]*AggregateResult)
		default:
			result.Metrics = make(map[string][]storage.Event)
		}
	}
	for _, metric := range metrics {
		limit, offset, desc := q.Limit, q.Offset, q.Order == "DESC"
		if q.GroupBySpec != "" || q.Aggregate != "" {
			limit = 0
			offset = 0
			desc = false
		}
		events, err := e.store.ReadPage(metric, q.From, to, q.Where, limit, offset, desc)
		if err != nil {
			return nil, err
		}
		switch {
		case q.GroupBySpec != "":
			series := aggregateSeriesUTC(events, q.GroupBySpec, q.Aggregate)
			if len(metrics) == 1 {
				result.Series = series
			} else {
				result.SeriesByMetric[metric] = series
			}
		case q.Aggregate != "":
			var sum float64
			for _, event := range events {
				sum += event.Value
			}
			value := sum
			if q.Aggregate == AggCount {
				value = float64(len(events))
			} else if q.Aggregate == AggAvg && len(events) > 0 {
				value = sum / float64(len(events))
			}
			agg := &AggregateResult{Type: string(q.Aggregate), Value: value, Count: len(events)}
			if len(metrics) == 1 {
				result.Aggregate = agg
			} else {
				result.Aggregates[metric] = agg
			}
		default:
			if len(metrics) == 1 {
				result.Events = events
			} else {
				result.Metrics[metric] = events
			}
		}
	}
	return result, nil
}

type durSpec struct {
	years  int
	months int
	dur    time.Duration
}

func parseCalendarSpec(spec string) (durSpec, error) {
	var out durSpec
	hasCalendar := strings.Contains(spec, "y") || strings.Contains(spec, "mo")
	for i := 0; i < len(spec); {
		j := i
		for j < len(spec) && spec[j] >= '0' && spec[j] <= '9' {
			j++
		}
		if j == i || j == len(spec) {
			return out, fmt.Errorf("invalid duration: %s", spec)
		}
		num, err := strconv.ParseInt(spec[i:j], 10, 64)
		if err != nil {
			return out, err
		}
		unit := spec[j : j+1]
		j++
		if strings.HasPrefix(spec[j-1:], "min") {
			unit = "min"
			j += 2
		} else if strings.HasPrefix(spec[j-1:], "mo") {
			unit = "mo"
			j++
		}
		var scale time.Duration
		switch unit {
		case "y":
			if num > 290 {
				return out, fmt.Errorf("duration too large")
			}
			out.years += int(num)
		case "mo":
			if num > 3480 {
				return out, fmt.Errorf("duration too large")
			}
			out.months += int(num)
		case "m":
			if hasCalendar {
				if num > 3480 {
					return out, fmt.Errorf("duration too large")
				}
				out.months += int(num)
			} else {
				scale = time.Minute
			}
		case "min":
			scale = time.Minute
		case "w":
			scale = 7 * 24 * time.Hour
		case "d":
			scale = 24 * time.Hour
		case "h":
			scale = time.Hour
		case "s":
			scale = time.Second
		default:
			return out, fmt.Errorf("invalid duration unit: %s", unit)
		}
		if scale > 0 {
			if num > (math.MaxInt64-int64(out.dur))/int64(scale) {
				return out, fmt.Errorf("duration overflow")
			}
			out.dur += time.Duration(num) * scale
		}
		i = j
	}
	if out.years == 0 && out.months == 0 && out.dur == 0 {
		return out, fmt.Errorf("duration must be positive")
	}
	return out, nil
}

func addCalendar(t time.Time, spec durSpec) time.Time {
	if spec.years != 0 || spec.months != 0 {
		t = t.AddDate(spec.years, spec.months, 0)
	}
	if spec.dur != 0 {
		t = t.Add(spec.dur)
	}
	return t
}

func aggregateSeriesUTC(events []storage.Event, spec string, agg AggType) []SeriesPoint {
	if len(events) == 0 {
		return []SeriesPoint{}
	}

	dur, err := parseCalendarSpec(spec)
	if err != nil {
		return []SeriesPoint{}
	}

	// align to epoch (UTC)
	start := time.Unix(0, 0).UTC()
	end := start

	series := []SeriesPoint{}
	i := 0

	for i < len(events) {
		// Jump directly to fixed-duration buckets instead of scanning from 1970.
		if dur.years == 0 && dur.months == 0 {
			ts := events[i].Timestamp
			start = time.Unix(0, ts-ts%int64(dur.dur)).UTC()
			end = start.Add(dur.dur)
		}
		// advance calendar bucket until it contains event
		for end.Before(time.Unix(0, events[i].Timestamp).UTC()) || end.Equal(time.Unix(0, events[i].Timestamp).UTC()) {
			start = end
			end = addCalendar(start, dur)
		}

		var sum float64
		count := 0
		for i < len(events) {
			evTime := time.Unix(0, events[i].Timestamp).UTC()
			if evTime.Before(start) || !evTime.Before(end) {
				break
			}
			sum += events[i].Value
			count++
			i++
		}

		value := sum
		switch agg {
		case AggCount:
			value = float64(count)
		case AggAvg:
			if count > 0 {
				value = sum / float64(count)
			}
		default:
			// AggSum or empty = sum
		}

		series = append(series, SeriesPoint{
			TS:    start.UnixNano(),
			Value: value,
			Count: count,
		})

		if end.Equal(start) {
			// guard against zero duration
			break
		}
	}

	return series
}

func (e *Executor) nextTimestamp() int64 {
	for {
		previous := e.clock.Load()
		now := time.Now().UnixNano()
		if now <= previous {
			now = previous + 1
		}
		if e.clock.CompareAndSwap(previous, now) {
			return now
		}
	}
}
