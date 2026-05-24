package query

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/byPixelTV/flamedb/internal/aggregates"
	"github.com/byPixelTV/flamedb/internal/storage"
	"github.com/cockroachdb/pebble"
)

type Executor struct {
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

func (e *Executor) executeGroupLeaderboard(q *Query) (*Result, error) {
	if len(q.Groups) == 0 {
		return &Result{Leaderboard: []aggregates.LeaderboardEntry{}}, nil
	}

	entries := make([]aggregates.LeaderboardEntry, 0, len(q.Groups))
	for _, g := range q.Groups {
		var sum float64
		for _, member := range g.Members {
			v, _ := e.lb.Get(q.Metric, member)
			sum += v
		}
		entries = append(entries, aggregates.LeaderboardEntry{
			EntityID: g.Name,
			Value:    sum,
		})
	}

	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].Value == entries[j].Value {
			return entries[i].EntityID < entries[j].EntityID
		}
		return entries[i].Value > entries[j].Value
	})

	// pagination
	if q.Offset >= len(entries) {
		return &Result{Leaderboard: []aggregates.LeaderboardEntry{}}, nil
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
	// scan über alle keys, metric names extrahieren
	iter, err := e.store.DB().NewIter(&pebble.IterOptions{})
	if err != nil {
		return nil
	}
	defer iter.Close()

	seen := make(map[string]bool)
	var metrics []string

	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		// skip index und leaderboard keys
		if strings.HasPrefix(key, "idx:") ||
			strings.HasPrefix(key, "lb:") ||
			strings.HasPrefix(key, "card") {
			continue
		}
		// metric name ist alles vor dem ersten :
		parts := strings.SplitN(key, ":", 2)
		if len(parts) > 0 && !seen[parts[0]] {
			seen[parts[0]] = true
			metrics = append(metrics, parts[0])
		}
	}
	return metrics
}

func (e *Executor) executeSet(q *Query) (*Result, error) {
	if q.UpdateLB {
		current, _ := e.lb.Get(q.Metric, q.LBEntityID)
		delta := q.Value - current
		if err := e.lb.Increment(q.Metric, q.LBEntityID, delta); err != nil {
			return nil, err
		}
		e.cache.Invalidate(q.Metric) // ← neu
	}
	return &Result{}, nil
}

func (e *Executor) executeDelete(q *Query) (*Result, error) {
	if q.UpdateLB && q.LBEntityID != "" {
		if err := e.lb.Delete(q.Metric, q.LBEntityID); err != nil {
			return nil, err
		}
		e.cache.Invalidate(q.Metric) // ← neu
	}
	return &Result{}, nil
}

func (e *Executor) executeWrite(q *Query) (*Result, error) {
	ts := q.Timestamp
	if ts == 0 {
		ts = time.Now().UnixNano()
	}

	event := storage.Event{
		Timestamp: ts,
		Metric:    q.Metric,
		Value:     q.Value,
		Tags:      q.Tags,
	}

	// sync=true wenn QUORUM, sonst async batching
	if err := e.store.WriteEvent(event, q.Quorum); err != nil {
		return nil, err
	}

	if q.UpdateLB {
		if err := e.lb.Increment(q.Metric, q.LBEntityID, q.Value); err != nil {
			return nil, err
		}
		e.cache.Invalidate(q.Metric)
	}

	return &Result{}, nil
}

func (e *Executor) executeLeaderboard(q *Query) (*Result, error) {
	// cache check
	if cached, ok := e.cache.Get(q.Metric, q.Limit, q.Offset); ok {
		return &Result{Leaderboard: cached}, nil
	}

	entries, err := e.lb.TopN(q.Metric, q.Limit, q.Offset)
	if err != nil {
		return nil, err
	}

	// in cache schreiben
	e.cache.Set(q.Metric, q.Limit, q.Offset, entries)

	return &Result{Leaderboard: entries}, nil
}

func (e *Executor) executeGet(q *Query) (*Result, error) {
	from := q.From
	to := q.To
	if from == 0 {
		from = 0
	}
	if to == 0 {
		to = math.MaxInt64
	}

	getEvents := func(metric string) ([]storage.Event, error) {
		if len(q.Where) > 0 {
			return e.store.ReadRangeWithTags(metric, from, to, q.Where)
		}
		return e.store.ReadRange(metric, from, to)
	}

	metricNames := q.Metrics
	if len(metricNames) == 0 {
		metricNames = []string{q.Metric}
	}

	if len(metricNames) > 1 {
		if q.GroupBySpec != "" {
			out := make(map[string][]SeriesPoint, len(metricNames))
			for _, m := range metricNames {
				events, err := getEvents(m)
				if err != nil {
					return nil, err
				}
				out[m] = aggregateSeriesUTC(events, q.GroupBySpec, q.Aggregate)
			}
			return &Result{SeriesByMetric: out}, nil
		}

		if q.Aggregate != "" {
			out := make(map[string]*AggregateResult, len(metricNames))
			for _, m := range metricNames {
				events, err := getEvents(m)
				if err != nil {
					return nil, err
				}
				count := len(events)
				var sum float64
				for _, e := range events {
					sum += e.Value
				}

				var value float64
				switch q.Aggregate {
				case AggCount:
					value = float64(count)
				case AggSum:
					value = sum
				case AggAvg:
					if count > 0 {
						value = sum / float64(count)
					}
				}

				out[m] = &AggregateResult{
					Type:  string(q.Aggregate),
					Value: value,
					Count: count,
				}
			}
			return &Result{Aggregates: out}, nil
		}

		out := make(map[string][]storage.Event, len(metricNames))
		for _, m := range metricNames {
			events, err := getEvents(m)
			if err != nil {
				return nil, err
			}
			if events == nil {
				events = []storage.Event{}
			}

			// ordering vor pagination
			switch q.Order {
			case "DESC":
				for i, j := 0, len(events)-1; i < j; i, j = i+1, j-1 {
					events[i], events[j] = events[j], events[i]
				}
			case "ASC":
			}

			// pagination danach
			if q.Offset >= len(events) {
				out[m] = []storage.Event{}
				continue
			}
			events = events[q.Offset:]
			if q.Limit > 0 && len(events) > q.Limit {
				events = events[:q.Limit]
			}

			out[m] = events
		}
		return &Result{Metrics: out}, nil
	}

	// single metric flow
	events, err := getEvents(q.Metric)
	if err != nil {
		return nil, err
	}
	if events == nil {
		events = []storage.Event{}
	}

	if q.GroupBySpec != "" {
		series := aggregateSeriesUTC(events, q.GroupBySpec, q.Aggregate)
		return &Result{Series: series}, nil
	}

	if q.Aggregate != "" {
		count := len(events)
		var sum float64
		for _, e := range events {
			sum += e.Value
		}

		var value float64
		switch q.Aggregate {
		case AggCount:
			value = float64(count)
		case AggSum:
			value = sum
		case AggAvg:
			if count > 0 {
				value = sum / float64(count)
			}
		}

		return &Result{
			Aggregate: &AggregateResult{
				Type:  string(q.Aggregate),
				Value: value,
				Count: count,
			},
		}, nil
	}

	// ordering vor pagination
	switch q.Order {
	case "DESC":
		for i, j := 0, len(events)-1; i < j; i, j = i+1, j-1 {
			events[i], events[j] = events[j], events[i]
		}
	case "ASC":
	}

	// pagination danach
	if q.Offset >= len(events) {
		return &Result{Events: []storage.Event{}}, nil
	}
	events = events[q.Offset:]
	if q.Limit > 0 && len(events) > q.Limit {
		events = events[:q.Limit]
	}

	return &Result{Events: events}, nil
}

type durSpec struct {
	years  int
	months int
	dur    time.Duration
}

func parseCalendarSpec(spec string) (durSpec, error) {
	var out durSpec
	if spec == "" {
		return out, fmt.Errorf("invalid duration: empty")
	}

	// If spec contains years or explicit months, treat "m" as months.
	hasCalendar := strings.Contains(spec, "y") || strings.Contains(spec, "mo")

	i := 0
	for i < len(spec) {
		j := i
		for j < len(spec) && unicode.IsDigit(rune(spec[j])) {
			j++
		}
		if j == i {
			return out, fmt.Errorf("invalid duration: %s", spec)
		}
		num, err := strconv.Atoi(spec[i:j])
		if err != nil {
			return out, err
		}

		unit := ""
		// check "mo" and "min" before single-char units
		if j+2 < len(spec) && spec[j:j+3] == "min" {
			unit = "min"
			j += 3
		} else if j+1 < len(spec) && spec[j:j+2] == "mo" {
			unit = "mo"
			j += 2
		} else {
			unit = spec[j : j+1]
			j++
		}

		switch unit {
		case "y":
			out.years += num
			hasCalendar = true
		case "mo":
			out.months += num
			hasCalendar = true
		case "m":
			if hasCalendar {
				out.months += num
			} else {
				out.dur += time.Duration(num) * time.Minute
			}
		case "min":
			out.dur += time.Duration(num) * time.Minute
		case "w":
			out.dur += time.Duration(num) * 7 * 24 * time.Hour
		case "d":
			out.dur += time.Duration(num) * 24 * time.Hour
		case "h":
			out.dur += time.Duration(num) * time.Hour
		case "s":
			out.dur += time.Duration(num) * time.Second
		default:
			return out, fmt.Errorf("invalid unit: %s", unit)
		}

		i = j
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
		// advance bucket until it contains event
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
