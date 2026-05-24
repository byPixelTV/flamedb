package storage

import (
	"encoding/binary"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
)

type cardEntry struct {
	metric   string
	tagKey   string
	tagValue string
}

type cardFlushRequest struct {
	sync bool
	done chan error
}

type cardUpdater struct {
	db         *pebble.DB
	cache      *cardCache
	mu         sync.Mutex
	pending    map[string]cardEntry
	flushCh    chan cardFlushRequest
	interval   time.Duration
	maxPending int
}

func newCardUpdater(db *pebble.DB, cache *cardCache, interval time.Duration, maxPending int) *cardUpdater {
	if db == nil || interval <= 0 || maxPending <= 0 {
		return nil
	}

	u := &cardUpdater{
		db:         db,
		cache:      cache,
		pending:    make(map[string]cardEntry, maxPending),
		flushCh:    make(chan cardFlushRequest, 1),
		interval:   interval,
		maxPending: maxPending,
	}
	go u.run()
	return u
}

func (u *cardUpdater) Add(metric string, tags map[string]string) {
	if u == nil || len(tags) == 0 {
		return
	}

	shouldFlush := false

	for tagKey, tagValue := range tags {
		ckey := cardCacheKey(metric, tagKey, tagValue)
		if u.cache != nil && u.cache.seen(ckey) {
			continue
		}

		u.mu.Lock()
		if _, ok := u.pending[ckey]; !ok {
			u.pending[ckey] = cardEntry{metric: metric, tagKey: tagKey, tagValue: tagValue}
			if len(u.pending) >= u.maxPending {
				shouldFlush = true
			}
		}
		u.mu.Unlock()
	}

	if shouldFlush {
		u.signalFlush(false)
	}
}

func (u *cardUpdater) Flush(sync bool) error {
	if u == nil {
		return nil
	}
	done := make(chan error, 1)
	u.flushCh <- cardFlushRequest{sync: sync, done: done}
	return <-done
}

func (u *cardUpdater) signalFlush(sync bool) {
	if u == nil {
		return
	}

	select {
	case u.flushCh <- cardFlushRequest{sync: sync}:
	default:
	}
}

func (u *cardUpdater) run() {
	ticker := time.NewTicker(u.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			_ = u.flush(false)
		case req := <-u.flushCh:
			err := u.flush(req.sync)
			if req.done != nil {
				req.done <- err
			}
		}
	}
}

func (u *cardUpdater) flush(sync bool) error {
	entries := u.drain()
	if len(entries) == 0 {
		return nil
	}

	batch := u.db.NewBatch()
	countBase := make(map[string]uint64)
	countDelta := make(map[string]uint64)

	for _, e := range entries {
		ckey := cardCacheKey(e.metric, e.tagKey, e.tagValue)
		if u.cache != nil && u.cache.seen(ckey) {
			continue
		}

		ck := cardKey(e.metric, e.tagKey, e.tagValue)
		_, closer, err := u.db.Get(ck)
		if err == pebble.ErrNotFound {
			countKey := cardCountKey(e.metric, e.tagKey)
			countKeyStr := string(countKey)
			if _, ok := countBase[countKeyStr]; !ok {
				countBase[countKeyStr] = getCardCountFromDB(u.db, countKey)
			}
			countDelta[countKeyStr]++
			batch.Set(ck, []byte{1}, nil)
			if u.cache != nil {
				u.cache.add(ckey)
			}
			continue
		}
		if err == nil {
			closer.Close()
			if u.cache != nil {
				u.cache.add(ckey)
			}
			continue
		}
		return err
	}

	for countKeyStr, delta := range countDelta {
		base := countBase[countKeyStr]
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, base+delta)
		batch.Set([]byte(countKeyStr), buf, nil)
	}

	opts := pebble.NoSync
	if sync {
		opts = pebble.Sync
	}
	return batch.Commit(opts)
}

func (u *cardUpdater) drain() []cardEntry {
	u.mu.Lock()
	if len(u.pending) == 0 {
		u.mu.Unlock()
		return nil
	}

	entries := make([]cardEntry, 0, len(u.pending))
	for _, e := range u.pending {
		entries = append(entries, e)
	}
	u.pending = make(map[string]cardEntry, u.maxPending)
	u.mu.Unlock()

	return entries
}

func getCardCountFromDB(db *pebble.DB, countKey []byte) uint64 {
	data, closer, err := db.Get(countKey)
	if err != nil {
		return 0
	}
	defer closer.Close()
	if len(data) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(data)
}
