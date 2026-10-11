package storage

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

func TestCardinalityMemory(t *testing.T) {
	s := windowTestStore(t)
	u := &cardUpdater{db: s.db, pending: make(map[string]cardEntry), maxPending: cardMaxPending}
	// Flushing a handful of new tags must not allocate a 100k-entry map each
	// time. Measure actual heap allocation, including the Pebble batch work.
	flush := func(i int) {
		e := cardEntry{"m", "clan", fmt.Sprintf("clan-%d", i)}
		u.pending[cardCacheKey(e.metric, e.tagKey, e.tagValue)] = e
		if err := u.flush(false); err != nil {
			t.Fatal(err)
		}
	}
	flush(0)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 1; i <= 50; i++ {
		flush(i)
	}
	runtime.ReadMemStats(&after)
	perFlush := (after.TotalAlloc - before.TotalAlloc) / 50
	t.Logf("allocated bytes/small cardinality flush: %d", perFlush)
	if perFlush > 128<<10 {
		t.Fatalf("excessive bookkeeping allocation: %d bytes/flush", perFlush)
	}
	if got := s.GetCardinality("m", "clan"); got != 51 {
		t.Fatalf("lost cardinality updates: %d", got)
	}

	c := newCardCache(cardCacheMaxEntries, time.Minute)
	for i := 0; i < 40000; i++ {
		c.add(fmt.Sprintf("%d:%s", i, strings.Repeat("x", 512)))
		if c.bytes > cardCacheMaxBytes || len(c.entries) > c.maxEntries {
			t.Fatal("unbounded retained cache")
		}
	}
	c.add(strings.Repeat("x", cardCacheMaxBytes+1))
	if c.bytes > cardCacheMaxBytes {
		t.Fatal("oversized value retained")
	}
	c.expire(time.Now().Add(2 * time.Minute))
	if len(c.entries) != 0 || c.bytes != 0 || c.peak != 0 {
		t.Fatalf("idle cache retained data/capacity: entries=%d bytes=%d peak=%d", len(c.entries), c.bytes, c.peak)
	}

	// A burst must not leave a large spare map alive after flush.
	for i := 0; i < 5000; i++ {
		e := cardEntry{"burst", "clan", fmt.Sprint(i)}
		u.pending[cardCacheKey(e.metric, e.tagKey, e.tagValue)] = e
	}
	if err := u.flush(false); err != nil {
		t.Fatal(err)
	}
	if u.spare != nil || len(u.pending) != 0 {
		t.Fatal("retained burst-sized pending map")
	}
}

func TestCardinalityConcurrentFlush(t *testing.T) {
	s := windowTestStore(t)
	c := newCardCache(128, time.Minute)
	u := newCardUpdater(s.db, c, time.Millisecond, 32)
	defer u.Close()
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				u.Add("m", map[string]string{"clan": fmt.Sprint(i)})
			}
		}()
	}
	wg.Wait()
	if err := u.Flush(true); err != nil {
		t.Fatal(err)
	}
	if got := s.GetCardinality("m", "clan"); got != 300 {
		t.Fatalf("cardinality=%d, want 300", got)
	}
}

func BenchmarkCardinalityChurn(b *testing.B) {
	for _, legacy := range []bool{true, false} {
		name := "bounded"
		if legacy {
			name = "legacy_capacity"
		}
		b.Run(name, func(b *testing.B) {
			s := windowTestStore(b)
			u := &cardUpdater{db: s.db, pending: make(map[string]cardEntry), maxPending: cardMaxPending}
			e := cardEntry{"m", "clan", "a"}
			if err := s.db.Set(cardKey(e.metric, e.tagKey, e.tagValue), []byte{1}, pebble.NoSync); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				u.pending["m\x00clan\x00a"] = e
				if err := u.flush(false); err != nil {
					b.Fatal(err)
				}
				if legacy {
					// Reproduce the old drain's eager allocation on every flush;
					// all other DB work is identical between the two variants.
					u.pending = make(map[string]cardEntry, cardMaxPending)
				}
			}
		})
	}
}
