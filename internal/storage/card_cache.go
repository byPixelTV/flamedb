package storage

import (
	"sync"
	"time"
)

type cardCache struct {
	mu         sync.Mutex
	entries    map[string]int64
	maxEntries int
	ttl        time.Duration
}

func newCardCache(maxEntries int, ttl time.Duration) *cardCache {
	if maxEntries <= 0 {
		return nil
	}
	c := &cardCache{
		entries:    make(map[string]int64, maxEntries),
		maxEntries: maxEntries,
		ttl:        ttl,
	}
	if ttl > 0 {
		go c.cleanup()
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

	if c.maxEntries > 0 && len(c.entries) >= c.maxEntries {
		c.entries = make(map[string]int64, c.maxEntries)
	}
	c.entries[key] = now
}

func (c *cardCache) cleanup() {
	interval := c.ttl / 2
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now().UnixNano()

		c.mu.Lock()
		for k, ts := range c.entries {
			if now-ts > int64(c.ttl) {
				delete(c.entries, k)
			}
		}
		c.mu.Unlock()
	}
}
