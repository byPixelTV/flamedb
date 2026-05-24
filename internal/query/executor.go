package query

import (
	"math"
	"sort"
	"strings"
	"time"

	"github.com/byPixelTV/flamedb/internal/aggregates"
	"github.com/byPixelTV/flamedb/internal/storage"
	"github.com/cockroachdb/pebble"
)

type Executor struct {
	store *storage.Storage
	lb    *aggregates.Leaderboard
}

func NewExecutor(store *storage.Storage, lb *aggregates.Leaderboard) *Executor {
	return &Executor{store: store, lb: lb}
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
		// get current, dann diff berechnen
		current, _ := e.lb.Get(q.Metric, q.LBEntityID)
		delta := q.Value - current
		if err := e.lb.Increment(q.Metric, q.LBEntityID, delta); err != nil {
			return nil, err
		}
	}
	return &Result{}, nil
}

func (e *Executor) executeDelete(q *Query) (*Result, error) {
	if q.UpdateLB && q.LBEntityID != "" {
		if err := e.lb.Delete(q.Metric, q.LBEntityID); err != nil {
			return nil, err
		}
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

	if err := e.store.WriteEvent(event); err != nil {
		return nil, err
	}

	if q.UpdateLB {
		if err := e.lb.Increment(q.Metric, q.LBEntityID, q.Value); err != nil {
			return nil, err
		}
	}

	return &Result{}, nil
}

func (e *Executor) executeLeaderboard(q *Query) (*Result, error) {
	entries, err := e.lb.TopN(q.Metric, q.Limit, q.Offset)
	if err != nil {
		return nil, err
	}
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

	var events []storage.Event
	var err error

	if len(q.Where) > 0 {
		events, err = e.store.ReadRangeWithTags(q.Metric, from, to, q.Where)
	} else {
		events, err = e.store.ReadRange(q.Metric, from, to)
	}
	if err != nil {
		return nil, err
	}

	if events == nil {
		events = []storage.Event{}
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
		// default order is ASC
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
