package storage

import (
	"context"

	"github.com/cockroachdb/pebble/v2"
)

// WalkEvents keeps one Pebble snapshot and does not retain the raw history.
func (s *Storage) WalkEvents(ctx context.Context, metric string, from, to int64, tags map[string]string, visit func(Event) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	lower, upper := eventKey(metric, from), eventKey(metric, to)
	indexed := len(tags) > 0
	if indexed {
		k, v := s.BestIndexTag(metric, tags)
		lower, upper = indexKey(metric, k, v, from), indexKey(metric, k, v, to)
	}
	snapshot := s.db.NewSnapshot()
	defer snapshot.Close()
	iter, err := snapshot.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	defer iter.Close()
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		var event Event
		if indexed {
			raw, closer, getErr := snapshot.Get(iter.Value())
			if getErr == pebble.ErrNotFound {
				continue
			}
			if getErr != nil {
				return getErr
			}
			event, err = decodeEventValue(raw, metric)
			closer.Close()
		} else {
			event, err = decodeEventValue(iter.Value(), metric)
		}
		if err != nil {
			return err
		}
		matches := true
		for key, value := range tags {
			if actual, ok := event.Tags[key]; !ok || actual != value {
				matches = false
				break
			}
		}
		if matches {
			if err := visit(event); err != nil {
				return err
			}
		}
	}
	return iter.Error()
}
