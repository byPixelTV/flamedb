package storage

import (
	"sync"
	"time"
)

// Bound retained key data as well as entry count (tag values can be large).
const cardCacheMaxBytes = 16 << 20
const cardCacheEntryOverhead = 64

type cardCache struct {
	mu         sync.Mutex
	entries    map[string]int64
	maxEntries int
	ttl        time.Duration
	bytes      int
	peak       int
	nextSweep  time.Time
}

func newCardCache(maxEntries int, ttl time.Duration) *cardCache {
	if maxEntries <= 0 {
		return nil
	}
	c := &cardCache{
		entries:    make(map[string]int64),
		maxEntries: maxEntries,
		ttl:        ttl,
		nextSweep:  time.Now().Add(ttl / 2),
	}

	return c
}

func (c *cardCache) seen(key string) bool {
	if c == nil {
		return false
	}
	now := time.Now().UnixNano()

	c.mu.Lock()
	defer c.mu.Unlock()

	ts, ok := c.entries[key]
	if !ok {
		return false
	}
	if c.ttl > 0 && now-ts > int64(c.ttl) {
		delete(c.entries, key)
		c.bytes -= len(key) + cardCacheEntryOverhead
		return false
	}
	c.entries[key] = now
	return true
}

func (c *cardCache) add(key string) {
	if c == nil {
		return
	}

	now := time.Now().UnixNano()

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.entries[key]; exists {
		c.entries[key] = now
		return
	}
	size := len(key) + cardCacheEntryOverhead
	if size > cardCacheMaxBytes {
		return
	}
	if len(c.entries) >= c.maxEntries || c.bytes+size > cardCacheMaxBytes {
		c.entries = make(map[string]int64)
		c.bytes, c.peak = 0, 0
	}
	c.bytes += size
	c.entries[key] = now
	if len(c.entries) > c.peak {
		c.peak = len(c.entries)
	}
}

// Called by the storage-owned updater: idle entries expire without a leaked
// cleanup goroutine, and map capacity shrinks after a high-cardinality burst.
func (c *cardCache) expire(now time.Time) {
	if c == nil || c.ttl <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if now.Before(c.nextSweep) {
		return
	}
	c.nextSweep = now.Add(c.ttl / 2)
	for key, ts := range c.entries {
		if now.UnixNano()-ts > int64(c.ttl) {
			delete(c.entries, key)
			c.bytes -= len(key) + cardCacheEntryOverhead
		}
	}
	if len(c.entries) <= c.peak/4 {
		compact := make(map[string]int64, len(c.entries))
		for key, ts := range c.entries {
			compact[key] = ts
		}
		c.entries = compact
		c.peak = len(compact)
	}
}
