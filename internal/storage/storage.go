package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/byPixelTV/flamedb/internal/cluster"
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

func Open(path, compression string) (*Storage, error) {
	opts := &pebble.Options{}
	opts.EnsureDefaults()

	comp := parseCompression(compression)
	for i := range opts.Levels {
		opts.Levels[i].Compression = comp
	}

	db, err := pebble.Open(path, opts)
	if err != nil {
		return nil, err
	}
	return &Storage{db: db}, nil
}

func parseCompression(s string) pebble.Compression {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "snappy":
		return pebble.SnappyCompression
	case "zstd":
		return pebble.ZstdCompression
	case "none", "no":
		return pebble.NoCompression
	default:
		return pebble.SnappyCompression
	}
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

// ExportMetric gibt alle raw pebble keys für eine metric zurück
func (s *Storage) ExportMetric(metric string) ([]RawKV, error) {
	lower := []byte(metric + ":")
	upper := []byte(metric + ";") // ; ist ein zeichen nach : in ASCII

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var kvs []RawKV
	for iter.First(); iter.Valid(); iter.Next() {
		key := make([]byte, len(iter.Key()))
		val := make([]byte, len(iter.Value()))
		copy(key, iter.Key())
		copy(val, iter.Value())
		kvs = append(kvs, RawKV{Key: key, Value: val})
	}
	return kvs, iter.Error()
}

// ExportLeaderboard gibt alle leaderboard entries für eine metric zurück
func (s *Storage) ExportLeaderboard(metric string) ([]RawKV, error) {
	var kvs []RawKV

	prefixes := [][]byte{
		[]byte("lb:" + metric + ":"),
		[]byte("lb-entity:" + metric + ":"),
	}

	for _, prefix := range prefixes {
		upper := append(append([]byte{}, prefix...), 0xFF)
		iter, err := s.db.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: upper,
		})
		if err != nil {
			return nil, err
		}
		for iter.First(); iter.Valid(); iter.Next() {
			key := make([]byte, len(iter.Key()))
			val := make([]byte, len(iter.Value()))
			copy(key, iter.Key())
			copy(val, iter.Value())
			kvs = append(kvs, RawKV{Key: key, Value: val})
		}
		if err := iter.Error(); err != nil {
			iter.Close()
			return nil, err
		}
		iter.Close()
	}

	return kvs, nil
}

// ImportRawKVs schreibt raw keys direkt in pebble
func (s *Storage) ImportRawKVs(kvs []RawKV) error {
	batch := s.db.NewBatch()
	for _, kv := range kvs {
		batch.Set(kv.Key, kv.Value, pebble.Sync)
	}
	return batch.Commit(pebble.Sync)
}

type RawKV struct {
	Key   []byte `json:"k"`
	Value []byte `json:"v"`
}

func (s *Storage) HasMetric(metric string) bool {
	lower := []byte(metric + ":")
	upper := []byte(metric + ";")
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return false
	}
	defer iter.Close()
	return iter.First()
}

func (s *Storage) ExportMetricData(metric string) (cluster.RebalanceData, error) {
	events, err := s.ExportMetric(metric)
	if err != nil {
		return cluster.RebalanceData{}, err
	}
	lb, err := s.ExportLeaderboard(metric)
	if err != nil {
		return cluster.RebalanceData{}, err
	}

	data := cluster.RebalanceData{Metric: metric}
	for _, kv := range events {
		data.Events = append(data.Events, cluster.RawEvent{Key: kv.Key, Value: kv.Value})
	}
	for _, kv := range lb {
		data.Leaderboard = append(data.Leaderboard, cluster.LeaderboardEntry{Key: kv.Key, Value: kv.Value})
	}
	return data, nil
}

func (s *Storage) ImportRebalanceData(data cluster.RebalanceData) error {
	var kvs []RawKV
	for _, e := range data.Events {
		kvs = append(kvs, RawKV{Key: e.Key, Value: e.Value})
	}
	for _, lb := range data.Leaderboard {
		kvs = append(kvs, RawKV{Key: lb.Key, Value: lb.Value})
	}
	return s.ImportRawKVs(kvs)
}
