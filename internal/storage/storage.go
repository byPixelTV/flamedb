package storage

import (
	"encoding/binary"
	"runtime"
	"strings"
	"time"

	"github.com/byPixelTV/flamedb/internal/cluster"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
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
	batcher     *WriteBatcher
	cardCache   *cardCache
	cardUpdater *cardUpdater
	cache       *pebble.Cache
}

func Open(path, compression string) (*Storage, error) {
	opts := &pebble.Options{}
	opts.EnsureDefaults()
	applyPerfOptions(opts)

	comp := parseCompression(compression)
	for i := range opts.Levels {
		opts.Levels[i].Compression = comp
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
		batcher:   NewWriteBatcher(db),
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
	opts.MaxConcurrentCompactions = func() int {
		cpus := runtime.GOMAXPROCS(0)
		if cpus < 2 {
			return 2
		}
		n := cpus / 2
		if n > 8 {
			n = 8
		}
		if n < 2 {
			n = 2
		}
		return n
	}

	filter := bloom.FilterPolicy(10)
	for i := range opts.Levels {
		opts.Levels[i].BlockSize = blockSizeBytes
		opts.Levels[i].IndexBlockSize = indexBlockSizeBytes
		opts.Levels[i].TargetFileSize = targetFileSizeBytes
		opts.Levels[i].FilterPolicy = filter
		opts.Levels[i].FilterType = pebble.TableFilter
	}

	if opts.Experimental.MaxWriterConcurrency <= 0 {
		opts.Experimental.MaxWriterConcurrency = runtime.GOMAXPROCS(0)
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
	primaryKey := eventKey(e.Metric, e.Timestamp)

	val, err := encodeEventValue(e)
	if err != nil {
		return err
	}

	// primary event
	s.batcher.Add(primaryKey, val)

	// secondary index pro tag
	for k, v := range e.Tags {
		idxKey := indexKey(e.Metric, k, v, e.Timestamp)
		s.batcher.Add(idxKey, primaryKey)
	}

	// cardinality tracking — async batcher for write throughput
	if s.cardUpdater != nil {
		s.cardUpdater.Add(e.Metric, e.Tags)
	}

	// QUORUM = sofort flushen, sonst async
	if sync {
		return s.batcher.Flush()
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
	if s.cardUpdater != nil {
		_ = s.cardUpdater.Flush(true)
	}

	err := s.db.Close()
	if s.cache != nil {
		s.cache.Unref()
	}
	return err
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
		e, err := decodeEventValue(iter.Value(), metric)
		if err != nil {
			return nil, err
		}
		events = append(events, e)
	}

	return events, iter.Error()
}

func (s *Storage) ReadRangeDesc(metric string, from, to int64, limit, offset int) ([]Event, error) {
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

	if !iter.Last() {
		return []Event{}, nil
	}

	for skipped := 0; skipped < offset && iter.Valid(); skipped++ {
		iter.Prev()
	}
	if !iter.Valid() {
		return []Event{}, nil
	}

	capHint := 0
	if limit > 0 {
		capHint = limit
	}
	var events []Event
	if capHint > 0 {
		events = make([]Event, 0, capHint)
	}

	for iter.Valid() {
		e, err := decodeEventValue(iter.Value(), metric)
		if err != nil {
			return nil, err
		}
		events = append(events, e)
		if limit > 0 && len(events) >= limit {
			break
		}
		iter.Prev()
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
		data, closer, err := s.db.Get(iter.Value())
		if err != nil {
			continue
		}
		e, err := decodeEventValue(data, metric)
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

func (s *Storage) ReadRangeWithTagsDesc(metric string, from, to int64, tags map[string]string, limit, offset int) ([]Event, error) {
	if len(tags) == 0 {
		return s.ReadRangeDesc(metric, from, to, limit, offset)
	}

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

	if !iter.Last() {
		return []Event{}, nil
	}

	capHint := 0
	if limit > 0 {
		capHint = limit
	}
	var events []Event
	if capHint > 0 {
		events = make([]Event, 0, capHint)
	}

	skipped := 0

	for iter.Valid() {
		data, closer, err := s.db.Get(iter.Value())
		if err != nil {
			iter.Prev()
			continue
		}
		e, err := decodeEventValue(data, metric)
		closer.Close()
		if err != nil {
			iter.Prev()
			continue
		}

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
			if skipped < offset {
				skipped++
			} else {
				events = append(events, e)
				if limit > 0 && len(events) >= limit {
					break
				}
			}
		}

		iter.Prev()
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
