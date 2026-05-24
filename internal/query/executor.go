package query

import (
	"math"
	"time"

	"github.com/byPixelTV/flamedb/internal/aggregates"
	"github.com/byPixelTV/flamedb/internal/storage"
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
	}
	return nil, nil
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

	// pagination
	if q.Offset >= len(events) {
		return &Result{Events: []storage.Event{}}, nil
	}
	events = events[q.Offset:]
	if q.Limit > 0 && len(events) > q.Limit {
		events = events[:q.Limit]
	}

	return &Result{Events: events}, nil
}
