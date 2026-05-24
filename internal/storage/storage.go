package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/cockroachdb/pebble"
)

type Storage struct {
	db *pebble.DB
}

func eventKey(metric string, timestamp int64) []byte {
	ts := make([]byte, 8)
	binary.BigEndian.PutUint64(ts, uint64(timestamp))
	key := append([]byte(metric+":"), ts...)
	return key
}

func (s *Storage) WriteEvent(e Event) error {
	ts := make([]byte, 8)
	binary.BigEndian.PutUint64(ts, uint64(e.Timestamp))

	primaryKey := eventKey(e.Metric, e.Timestamp)

	batch := s.db.NewBatch()

	val, err := json.Marshal(e)
	if err != nil {
		return err
	}
	batch.Set(primaryKey, val, pebble.Sync)

	for k, v := range e.Tags {
		idxKey := indexKey(e.Metric, k, v, e.Timestamp)
		batch.Set(idxKey, primaryKey, pebble.Sync)
	}

	// cardinality tracking
	if err := s.updateCardinality(batch, e.Metric, e.Tags); err != nil {
		return err
	}

	return batch.Commit(pebble.Sync)
}

func indexKey(metric, tagKey, tagValue string, timestamp int64) []byte {
	ts := make([]byte, 8)
	binary.BigEndian.PutUint64(ts, uint64(timestamp))
	// format: idx:metric:tagkey:tagvalue:timestamp
	prefix := fmt.Sprintf("idx:%s:%s:%s:", metric, tagKey, tagValue)
	return append([]byte(prefix), ts...)
}

func Open(path string) (*Storage, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &Storage{db: db}, nil
}

func (s *Storage) DB() *pebble.DB {
	return s.db
}

func (s *Storage) Close() error {
	return s.db.Close()
}

func (s *Storage) ReadRange(metric string, from, to int64) ([]Event, error) {
	lower := eventKey(metric, from)
	upper := eventKey(metric, to)

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var events []Event
	for iter.First(); iter.Valid(); iter.Next() {
		var e Event
		if err := json.Unmarshal(iter.Value(), &e); err != nil {
			return nil, err
		}
		events = append(events, e)
	}

	return events, iter.Error()
}

func (s *Storage) ReadRangeWithTags(metric string, from, to int64, tags map[string]string) ([]Event, error) {
	if len(tags) == 0 {
		return s.ReadRange(metric, from, to)
	}

	// besten index tag via cardinality wählen
	primaryKey, primaryVal := s.BestIndexTag(metric, tags)

	lower := indexKey(metric, primaryKey, primaryVal, from)
	upper := indexKey(metric, primaryKey, primaryVal, to)

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var events []Event
	for iter.First(); iter.Valid(); iter.Next() {
		pk := make([]byte, len(iter.Value()))
		copy(pk, iter.Value())

		data, closer, err := s.db.Get(pk)
		if err != nil {
			continue
		}
		var e Event
		err = json.Unmarshal(data, &e)
		closer.Close()
		if err != nil {
			continue
		}

		// restliche tags filtern
		match := true
		for k, v := range tags {
			if k == primaryKey {
				continue
			}
			if e.Tags[k] != v {
				match = false
				break
			}
		}
		if match {
			events = append(events, e)
		}
	}

	return events, iter.Error()
}
