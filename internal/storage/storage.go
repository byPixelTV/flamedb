package storage

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/byPixelTV/flamedb/internal/cluster"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/sstable"
)

const (
	cardCacheMaxEntries = 250000
	cardCacheTTL        = 10 * time.Minute
	cardFlushInterval   = 50 * time.Millisecond
	cardMaxPending      = 100000

	blockCacheSizeBytes = 256 << 20
	memTableSizeBytes   = 64 << 20
	blockSizeBytes      = 32 << 10
	indexBlockSizeBytes = 256 << 10
	targetFileSizeBytes = 64 << 20
	l0CompactionThresh  = 8
	l0StopWritesThresh  = 32
	lbaseMaxBytes       = 512 << 20
	bytesPerSync        = 1 << 20
	walBytesPerSync     = 8 << 20
)

type Storage struct {
	db          *pebble.DB
	cardCache   *cardCache
	cardUpdater *cardUpdater
	cache       *pebble.Cache
}

func Open(path, compression string) (*Storage, error) {
	if err := prepareLegacy(path); err != nil {
		return nil, err
	}
	opts := &pebble.Options{}
	opts.EnsureDefaults()
	applyPerfOptions(opts)

	comp := parseCompression(compression)
	for i := range opts.Levels {
		opts.Levels[i].Compression = func() *sstable.CompressionProfile { return comp }
	}

	blockCache := pebble.NewCache(blockCacheSizeBytes)
	if blockCache != nil {
		opts.Cache = blockCache
	}

	db, err := pebble.Open(path, opts)
	if err != nil {
		if blockCache != nil {
			blockCache.Unref()
		}
		return nil, err
	}
	store := &Storage{
		db:        db,
		cardCache: newCardCache(cardCacheMaxEntries, cardCacheTTL),
		cache:     blockCache,
	}
	store.cardUpdater = newCardUpdater(db, store.cardCache, cardFlushInterval, cardMaxPending)
	return store, nil
}

func applyPerfOptions(opts *pebble.Options) {
	if opts == nil {
		return
	}

	opts.MemTableSize = memTableSizeBytes
	opts.MemTableStopWritesThreshold = 4
	opts.L0CompactionThreshold = l0CompactionThresh
	opts.L0CompactionFileThreshold = l0CompactionThresh
	opts.L0StopWritesThreshold = l0StopWritesThresh
	opts.LBaseMaxBytes = lbaseMaxBytes
	opts.BytesPerSync = bytesPerSync
	opts.WALBytesPerSync = walBytesPerSync
	opts.WALMinSyncInterval = func() time.Duration { return 200 * time.Microsecond }
	opts.CompactionConcurrencyRange = func() (int, int) {
		cpus := runtime.GOMAXPROCS(0)
		if cpus < 2 {
			return 1, 2
		}
		n := cpus / 2
		if n > 8 {
			n = 8
		}
		if n < 2 {
			n = 2
		}
		return 1, n
	}

	filter := bloom.FilterPolicy(10)
	for i := range opts.Levels {
		opts.Levels[i].BlockSize = blockSizeBytes
		opts.Levels[i].IndexBlockSize = indexBlockSizeBytes
		opts.TargetFileSizes[i] = targetFileSizeBytes
		opts.Levels[i].FilterPolicy = filter
		opts.Levels[i].FilterType = pebble.TableFilter
	}

}

func appendUint64(dst []byte, v uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	return append(dst, buf[:]...)
}

func eventKey(metric string, timestamp int64) []byte {
	key := make([]byte, 0, len(metric)+1+8)
	key = append(key, metric...)
	key = append(key, ':')
	return appendUint64(key, uint64(timestamp))
}

func (s *Storage) WriteEvent(e Event, sync bool) error {
	return s.WriteEvents([]Event{e}, sync)
}

func (s *Storage) WriteEvents(events []Event, sync bool) error {
	return s.WriteEventsWithReceipts(events, nil, sync)
}

func (s *Storage) WriteEventsWithReceipts(events []Event, receipts []string, sync bool) error {
	if len(events) == 0 {
		return nil
	}

	batch := s.db.NewBatch()
	defer batch.Close()

	for i, e := range events {
		if i < len(receipts) && receipts[i] != "" {
			key := []byte("repl-applied:" + receipts[i])
			_, closer, err := s.db.Get(key)
			if err == nil {
				closer.Close()
				continue
			}
			if err != pebble.ErrNotFound {
				return err
			}
			if err := batch.Set(key, []byte{1}, nil); err != nil {
				return err
			}
		}
		primaryKey := eventKey(e.Metric, e.Timestamp)

		val, err := encodeEventValue(e)
		if err != nil {
			return err
		}

		if err := batch.Set(primaryKey, val, nil); err != nil {
			return err
		}

		for k, v := range e.Tags {
			idxKey := indexKey(e.Metric, k, v, e.Timestamp)
			if err := batch.Set(idxKey, primaryKey, nil); err != nil {
				return err
			}
		}

	}
	opts := pebble.NoSync
	if sync {
		opts = pebble.Sync
	}
	if err := batch.Commit(opts); err != nil {
		return err
	}
	for _, e := range events {
		if s.cardUpdater != nil {
			s.cardUpdater.Add(e.Metric, e.Tags)
		}
	}
	return nil
}

func indexKey(metric, tagKey, tagValue string, timestamp int64) []byte {
	// format: idx:metric:tagkey:tagvalue:timestamp
	key := make([]byte, 0, len("idx:")+len(metric)+1+len(tagKey)+1+len(tagValue)+1+8)
	key = append(key, 'i', 'd', 'x', ':')
	key = append(key, metric...)
	key = append(key, ':')
	key = append(key, tagKey...)
	key = append(key, ':')
	key = append(key, tagValue...)
	key = append(key, ':')
	return appendUint64(key, uint64(timestamp))
}

func parseCompression(s string) *sstable.CompressionProfile {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "snappy":
		return sstable.SnappyCompression
	case "zstd":
		return sstable.ZstdCompression
	case "none", "no":
		return sstable.NoCompression
	default:
		return sstable.SnappyCompression
	}
}

func (s *Storage) DB() *pebble.DB {
	return s.db
}

func (s *Storage) Close() error {
	if s.cardUpdater != nil {
		_ = s.cardUpdater.Close()
	}

	err := s.db.Close()
	if s.cache != nil {
		s.cache.Unref()
	}
	return err
}

func (s *Storage) ReadRange(metric string, from, to int64) ([]Event, error) {
	return s.ReadPage(metric, from, to, nil, 0, 0, false)
}
func (s *Storage) ReadRangeDesc(metric string, from, to int64, limit, offset int) ([]Event, error) {
	return s.ReadPage(metric, from, to, nil, limit, offset, true)
}
func (s *Storage) ReadRangeWithTags(metric string, from, to int64, tags map[string]string) ([]Event, error) {
	return s.ReadPage(metric, from, to, tags, 0, 0, false)
}
func (s *Storage) ReadRangeWithTagsDesc(metric string, from, to int64, tags map[string]string, limit, offset int) ([]Event, error) {
	return s.ReadPage(metric, from, to, tags, limit, offset, true)
}

// ReadPage bounds scans by both time and pagination. A snapshot keeps secondary
// index references consistent with primary values during concurrent overwrites.
func (s *Storage) ReadPage(metric string, from, to int64, tags map[string]string, limit, offset int, desc bool) ([]Event, error) {
	if limit < 0 || offset < 0 {
		return nil, fmt.Errorf("invalid pagination")
	}
	lower, upper := eventKey(metric, from), eventKey(metric, to)
	indexed := len(tags) > 0
	if indexed {
		k, v := s.BestIndexTag(metric, tags)
		lower = indexKey(metric, k, v, from)
		upper = indexKey(metric, k, v, to)
	}
	snapshot := s.db.NewSnapshot()
	defer snapshot.Close()
	iter, err := snapshot.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	events := []Event{}
	valid := iter.First()
	if desc {
		valid = iter.Last()
	}
	for valid {
		var e Event
		if indexed {
			data, closer, getErr := snapshot.Get(iter.Value())
			if getErr == pebble.ErrNotFound {
				if desc {
					valid = iter.Prev()
				} else {
					valid = iter.Next()
				}
				continue
			}
			if getErr != nil {
				return nil, getErr
			}
			e, err = decodeEventValue(data, metric)
			closer.Close()
		} else {
			e, err = decodeEventValue(iter.Value(), metric)
		}
		if err != nil {
			return nil, err
		}
		match := true
		for k, v := range tags {
			if actual, exists := e.Tags[k]; !exists || actual != v {
				match = false
				break
			}
		}
		if match {
			if offset > 0 {
				offset--
			} else {
				events = append(events, e)
				if limit > 0 && len(events) >= limit {
					break
				}
			}
		}
		if desc {
			valid = iter.Prev()
		} else {
			valid = iter.Next()
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
	defer batch.Close()
	for _, kv := range kvs {
		if err := batch.Set(kv.Key, kv.Value, nil); err != nil {
			return err
		}
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
	// Rebuild tag indexes, which are not part of the export payload.
	for _, raw := range data.Events {
		e, err := decodeEventValue(raw.Value, data.Metric)
		if err != nil {
			return err
		}
		for k, v := range e.Tags {
			kvs = append(kvs, RawKV{Key: indexKey(e.Metric, k, v, e.Timestamp), Value: raw.Key})
		}
	}
	return s.ImportRawKVs(kvs)
}

// StageEvent adds the event and its indexes to the caller's atomic mutation.
func (s *Storage) StageEvent(batch *pebble.Batch, e Event) error {
	key := eventKey(e.Metric, e.Timestamp)
	val, err := encodeEventValue(e)
	if err != nil {
		return err
	}
	if old, closer, err := s.db.Get(key); err == nil {
		previous, decodeErr := decodeEventValue(old, e.Metric)
		closer.Close()
		if decodeErr != nil {
			return decodeErr
		}
		for k, v := range previous.Tags {
			if err := batch.Delete(indexKey(e.Metric, k, v, e.Timestamp), nil); err != nil {
				return err
			}
		}
	} else if err != pebble.ErrNotFound {
		return err
	}
	if err := batch.Set(key, val, nil); err != nil {
		return err
	}
	for k, v := range e.Tags {
		if err := batch.Set(indexKey(e.Metric, k, v, e.Timestamp), key, nil); err != nil {
			return err
		}
	}
	return nil
}

func (s *Storage) RecordCommittedEvent(e Event) {
	if s.cardUpdater != nil {
		s.cardUpdater.Add(e.Metric, e.Tags)
	}
}

func (s *Storage) StageDeleteRange(batch *pebble.Batch, metric string, from, to int64) error {
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: eventKey(metric, from), UpperBound: eventKey(metric, to)})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		e, err := decodeEventValue(iter.Value(), metric)
		if err != nil {
			return err
		}
		for k, v := range e.Tags {
			if err := batch.Delete(indexKey(metric, k, v, e.Timestamp), nil); err != nil {
				return err
			}
		}
		if err := batch.Delete(iter.Key(), nil); err != nil {
			return err
		}
	}
	return iter.Error()
}
