package storage

import (
	"encoding/binary"
	"encoding/json"

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
	key := eventKey(e.Metric, e.Timestamp)
	val, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return s.db.Set(key, val, pebble.Sync)
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
