package query

import (
	"fmt"
	"math"

	"github.com/byPixelTV/flamedb/internal/aggregates"
	"github.com/byPixelTV/flamedb/internal/storage"
	"github.com/cockroachdb/pebble/v2"
)

// executeWriteBatch commits events, score indexes and replica receipts together.
// One durable WAL commit replaces one synchronous commit per leaderboard event.
// All score stripes remain locked through that commit, including concurrent SETs.
func (e *Executor) executeWriteBatch(queries []*Query) (*BatchResult, error) {
	r := &BatchResult{OK: true}
	failed := make(map[int]bool)
	fail := func(i int, err error) {
		if failed[i] {
			return
		}
		failed[i] = true
		r.Failed++
		r.Errors = append(r.Errors, BatchItemError{Index: i, Error: err.Error()})
	}
	type scoreKey struct{ metric, entity string }
	boards := make([]map[string]string, len(queries))
	locks := []string{}
	valid := []int{}
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
		boards[i] = make(map[string]string, len(q.LBEntities)+1)
		if q.UpdateLB {
			boards[i][""] = q.LBEntityID
		}
		for board, entity := range q.LBEntities {
			boards[i][board] = entity
		}
		for board, entity := range boards[i] {
			locks = append(locks, aggregates.BoardMetric(q.Metric, board)+":"+entity)
		}
		if len(boards[i]) == 0 {
			locks = append(locks, q.Metric+":")
		}
		if q.IsReplica && q.OperationID != "" {
			locks = append(locks, "repl-applied:"+q.OperationID)
		}
		valid = append(valid, i)
	}
	if len(valid) == 0 {
		r.OK = r.Failed == 0
		return r, nil
	}
	accepted := []int{}
	committed := []storage.Event{}
	err := e.lb.AtomicMutationMany(locks, func(batch *pebble.Batch) error {
		type scoreState struct {
			initial, current float64
			changed          bool
		}
		scores := map[scoreKey]scoreState{}
		for _, i := range valid {
			q := queries[i]
			receipt := []byte("repl-applied:" + q.OperationID)
			if q.IsReplica && q.OperationID != "" {
				_, closer, err := batch.Get(receipt)
				if err == nil {
					closer.Close()
					accepted = append(accepted, i)
					continue
				}
				if err != pebble.ErrNotFound {
					return err
				}
			}
			// Validate every board before staging any part of this event. An
			// overflowing board fails only its item, with no partial named scores.
			next := make(map[scoreKey]float64, len(boards[i]))
			for board, entity := range boards[i] {
				key := scoreKey{aggregates.BoardMetric(q.Metric, board), entity}
				state, ok := scores[key]
				if !ok {
					current, err := e.lb.Get(key.metric, key.entity)
					if err != nil {
						return err
					}
					state = scoreState{initial: current, current: current}
					scores[key] = state
				}
				value := state.current + q.Value
				if math.IsNaN(value) || math.IsInf(value, 0) {
					fail(i, fmt.Errorf("leaderboard value must be finite"))
					break
				}
				if value == 0 {
					value = 0
				}
				next[key] = value
			}
			if failed[i] {
				continue
			}
			event := storage.Event{Metric: q.Metric, Timestamp: q.Timestamp, Value: q.Value, Tags: q.Tags}
			if err := e.store.StageEvent(batch, event); err != nil {
				return err
			}
			for key, value := range next {
				state := scores[key]
				state.current, state.changed = value, true
				scores[key] = state
			}
			if q.IsReplica && q.OperationID != "" {
				if err := batch.Set(receipt, []byte{1}, nil); err != nil {
					return err
				}
			}
			accepted = append(accepted, i)
			committed = append(committed, event)
		}
		// Scores are invisible until the shared commit. Keep validation and
		// floating-point addition in input order, but write each final score
		// once instead of writing and deleting every intermediate sort key.
		for key, state := range scores {
			if !state.changed {
				continue
			}
			if err := e.lb.StageValueFromCurrent(batch, key.metric, key.entity, state.initial, state.current, false); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		// No result is acknowledged before the shared synchronous commit.
		for _, i := range valid {
			fail(i, err)
		}
	} else {
		r.Accepted = len(accepted)
		for _, event := range committed {
			e.store.RecordCommittedEvent(event)
		}
	}
	r.OK = r.Failed == 0
	return r, nil
}
