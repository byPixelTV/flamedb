package storage

import (
	"fmt"
	"sync"
	"time"

	"github.com/byPixelTV/flamedb/internal/aggregates"
)

type cacheEntry struct {
	entries   []aggregates.LeaderboardEntry
	expiresAt time.Time
}

type LeaderboardCache struct {
	mu  sync.RWMutex
	ttl time.Duration
	// key: "metric:limit:offset"
	entries map[string]*cacheEntry
}

func NewLeaderboardCache(ttl time.Duration) *LeaderboardCache {
	c := &LeaderboardCache{
		ttl:     ttl,
		entries: make(map[string]*cacheEntry),
	}
	go c.cleanup()
	return c
}

func cacheKey(metric string, limit, offset int) string {
	return fmt.Sprintf("%s:%d:%d", metric, limit, offset)
}

func (c *LeaderboardCache) Get(metric string, limit, offset int) ([]aggregates.LeaderboardEntry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, ok := c.entries[cacheKey(metric, limit, offset)]
	if !ok || time.Now().After(entry.expiresAt) {
		return nil, false
	}
	return entry.entries, true
}

func (c *LeaderboardCache) Set(metric string, limit, offset int, entries []aggregates.LeaderboardEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries[cacheKey(metric, limit, offset)] = &cacheEntry{
		entries:   entries,
		expiresAt: time.Now().Add(c.ttl),
	}
}

func (c *LeaderboardCache) Invalidate(metric string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	prefix := metric + ":"
	for k := range c.entries {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			delete(c.entries, k)
		}
	}
}

func (c *LeaderboardCache) cleanup() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		c.mu.Lock()
		now := time.Now()
		for k, e := range c.entries {
			if now.After(e.expiresAt) {
				delete(c.entries, k)
			}
		}
		c.mu.Unlock()
	}
}
