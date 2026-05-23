package query

import (
	"time"

	"github.com/byPixelTV/flamedb/internal/aggregates"
	"github.com/byPixelTV/flamedb/internal/storage"
)

type Result struct {
	Events      []storage.Event               `json:"events,omitempty"`
	Leaderboard []aggregates.LeaderboardEntry `json:"leaderboard,omitempty"`
}

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
	}
	return nil, nil
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
	events, err := e.store.ReadRange(q.Metric, q.From, q.To)
	if err != nil {
		return nil, err
	}
	if events == nil {
		events = []storage.Event{}
	}

	// filter by tags
	if len(q.Where) > 0 {
		var filtered []storage.Event
		for _, ev := range events {
			match := true
			for k, v := range q.Where {
				if ev.Tags[k] != v {
					match = false
					break
				}
			}
			if match {
				filtered = append(filtered, ev)
			}
		}
		events = filtered
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
