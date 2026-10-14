package storage

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/byPixelTV/flamedb/internal/types"
	"github.com/cockroachdb/pebble/v2"
)

// No asynchronous cardinality work: isolate query measurements from ingestion.
func windowTestStore(tb testing.TB) *Storage {
	tb.Helper()
	cache := pebble.NewCache(32 << 20)
	db, err := pebble.Open(tb.TempDir(), &pebble.Options{Cache: cache, MemTableSize: 8 << 20})
	cache.Unref()
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		if err := db.Close(); err != nil {
			tb.Error(err)
		}
	})
	return &Storage{db: db}
}

func TestWindowedLeaderboardSemantics(t *testing.T) {
	s := windowTestStore(t)
	metric := "craftattack:clans:joins"
	events := []Event{
		{metricTimestamp(9), metric, 999, map[string]string{"clan": "old"}},
		{metricTimestamp(10), metric, 5, map[string]string{"clan": "b"}},
		{metricTimestamp(11), metric, -2, map[string]string{"clan": "b"}},
		{metricTimestamp(12), metric, 3, map[string]string{"clan": "a:1"}},
		{metricTimestamp(13), metric, -4, map[string]string{"clan": "negative"}},
		{metricTimestamp(14), metric, 0, map[string]string{"clan": "zero"}},
		{metricTimestamp(15), metric, 123, map[string]string{"other": "ignored"}},
		{metricTimestamp(16), metric, 123, map[string]string{"clan": ""}},
		{metricTimestamp(20), metric, 999, map[string]string{"clan": "future"}},
		{metricTimestamp(12), metric + ":other", 999, map[string]string{"clan": "other-metric"}},
	}
	if err := s.WriteEvents(events, false); err != nil {
		t.Fatal(err)
	}
	// Existing JSON values and unflushed writes must participate equally.
	legacy := Event{metricTimestamp(17), metric, 2, map[string]string{"clan": "legacy"}}
	data, _ := json.Marshal(legacy)
	if err := s.db.Set(eventKey(metric, legacy.Timestamp), data, pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	want := []types.LeaderboardEntry{{EntityID: "a:1", Value: 3}, {EntityID: "b", Value: 3}, {EntityID: "legacy", Value: 2}, {EntityID: "zero", Value: 0}, {EntityID: "negative", Value: -4}}
	for _, page := range [][2]int{{0, 0}, {2, 1}, {10, 5}, {math.MaxInt, 2}} {
		got, err := s.WindowedLeaderboard(metric, "clan", metricTimestamp(10), metricTimestamp(20), page[0], page[1])
		end := len(want)
		if page[0] > 0 && page[0] < end-page[1] {
			end = page[0] + page[1]
		}
		if err != nil || !reflect.DeepEqual(got, want[page[1]:end]) {
			t.Fatalf("page %v: got %v, err %v, want %v", page, got, err, want[page[1]:end])
		}
		if cap(got) != len(got) {
			t.Fatalf("page retains extra entries: len=%d cap=%d", len(got), cap(got))
		}
	}
	for _, bounds := range [][2]int64{{20, 10}, {10, 10}} {
		got, err := s.WindowedLeaderboard(metric, "clan", bounds[0], bounds[1], 10, 0)
		if err != nil || len(got) != 0 {
			t.Fatalf("empty interval: %v %v", got, err)
		}
	}
	if _, err := s.WindowedLeaderboard(metric, "clan", 0, 0, 1, -1); err == nil {
		t.Fatal("negative offset accepted")
	}
	got, err := s.WindowedLeaderboard(metric, "clan", metricTimestamp(20), 0, 0, 0)
	if err != nil || len(got) != 1 || got[0].EntityID != "future" {
		t.Fatalf("unbounded upper range: %v %v", got, err)
	}
}

func metricTimestamp(day int) int64 { return int64(day) * int64(24*time.Hour) }

func TestWindowedOverwriteDeleteAndGroup(t *testing.T) {
	s := windowTestStore(t)
	for _, e := range []Event{
		{1, "m", 9, map[string]string{"clan": "old"}},
		{1, "m", 3, map[string]string{"clan": "new"}},
		{2, "m", 2, map[string]string{"clan": "new"}},
		{3, "m", 8, map[string]string{"clan": "new:child"}},
	} {
		if err := s.WriteEvent(e, false); err != nil {
			t.Fatal(err)
		}
	}
	check := func(wantNew float64) {
		t.Helper()
		got, err := s.WindowedLeaderboard("m", "clan", 0, 0, 0, 0)
		want := []types.LeaderboardEntry{{EntityID: "new:child", Value: 8}, {EntityID: "new", Value: wantNew}}
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("leaderboard %v %v", got, err)
		}
		sums, err := s.WindowedEntitySums("m", "clan", []string{"new", "old", "new:child", "new", "missing"}, 0, 0)
		if err != nil || !reflect.DeepEqual(sums, map[string]float64{"new": wantNew, "old": 0, "new:child": 8, "missing": 0}) {
			t.Fatalf("group %v %v", sums, err)
		}
	}
	check(5)
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := s.StageDeleteRange(batch, "m", 2, 3); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	check(3)
}

func TestWindowedLeaderboardConcurrentSnapshot(t *testing.T) {
	s := windowTestStore(t)
	write := func(v float64) error {
		return s.WriteEvents([]Event{{1, "m", v, map[string]string{"clan": "a"}}, {2, "m", 100 - v, map[string]string{"clan": "b"}}}, false)
	}
	if err := write(25); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			if err := write(float64(i % 100)); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for i := 0; i < 100; i++ {
		got, err := s.WindowedLeaderboard("m", "clan", 0, 10, 0, 0)
		if err != nil || len(got) != 2 || got[0].Value+got[1].Value != 100 {
			t.Fatalf("inconsistent leaderboard: %v %v", got, err)
		}
		sums, err := s.WindowedEntitySums("m", "clan", []string{"a", "b"}, 0, 10)
		if err != nil || sums["a"]+sums["b"] != 100 {
			t.Fatalf("inconsistent group: %v %v", sums, err)
		}
	}
}

func TestWindowedLeaderboardCorruption(t *testing.T) {
	s := windowTestStore(t)
	if err := s.WriteEvent(Event{1, "m", 1, map[string]string{"clan": "a"}}, false); err != nil {
		t.Fatal(err)
	}
	if err := s.db.Set(eventKey("m", 1), []byte{1, 2}, pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WindowedLeaderboard("m", "clan", 0, 10, 10, 0); err == nil {
		t.Fatal("corruption silently returned a partial leaderboard")
	}
	if _, err := s.WindowedEntitySums("m", "clan", []string{"a"}, 0, 10); err == nil {
		t.Fatal("corruption silently returned a partial group")
	}
	// An invalid value outside the time range is never decoded.
	if got, err := s.WindowedLeaderboard("m", "clan", 2, 10, 10, 0); err != nil || len(got) != 0 {
		t.Fatalf("read outside bounds: %v %v", got, err)
	}
}

func TestLeaderboardHeapPages(t *testing.T) {
	var entries []types.LeaderboardEntry
	for i := 0; i < 1000; i++ {
		entries = append(entries, types.LeaderboardEntry{EntityID: fmt.Sprintf("c:%04d", i), Value: float64(i%17 - 8)})
	}
	ordered := append([]types.LeaderboardEntry(nil), entries...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Value == ordered[j].Value {
			return ordered[i].EntityID < ordered[j].EntityID
		}
		return ordered[i].Value > ordered[j].Value
	})
	for _, limit := range []int{0, 1, 10, 499, 501, 1000, math.MaxInt} {
		for _, offset := range []int{0, 1, 75, 999, 1000, math.MaxInt} {
			got := leaderboardPage(append([]types.LeaderboardEntry(nil), entries...), limit, offset)
			start := min(offset, len(ordered))
			end := len(ordered)
			if limit > 0 && limit < end-start {
				end = start + limit
			}
			if !reflect.DeepEqual(got, ordered[start:end]) || cap(got) != len(got) {
				t.Fatalf("page %d/%d: %v != %v", limit, offset, got, ordered[start:end])
			}
		}
	}
}

func TestEventEntityValue(t *testing.T) {
	e := Event{123, "m", -2.5, map[string]string{"clan:id": "a:ß", "noise": "long tag", "empty": ""}}
	binary, _ := encodeEventValue(e)
	legacy, _ := json.Marshal(e)
	for _, data := range [][]byte{binary, legacy} {
		for _, tag := range []string{"clan:id", "noise", "empty", "missing"} {
			id, value, found, err := eventEntityValue(data, tag)
			want, exists := e.Tags[tag]
			if err != nil || found != exists || string(id) != want || value != e.Value {
				t.Fatalf("%s: %q %v %v %v", tag, id, value, found, err)
			}
		}
	}
	for i := 0; i < len(binary); i++ {
		if _, _, _, err := eventEntityValue(binary[:i], "clan:id"); err == nil {
			t.Fatalf("accepted truncated event at %d", i)
		}
	}
	allocs := testing.AllocsPerRun(100, func() {
		if _, _, _, err := eventEntityValue(binary, "clan:id"); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("binary decoding allocates: %v", allocs)
	}
}

func FuzzEventEntityValue(f *testing.F) {
	data, _ := encodeEventValue(Event{1, "m", 2, map[string]string{"clan": "a", "other": "b"}})
	f.Add(data, "clan")
	f.Add([]byte(`{"Value":2,"Tags":{"clan":"x"}}`), "clan")
	f.Fuzz(func(t *testing.T, data []byte, tag string) {
		e, fullErr := decodeEventValue(data, "")
		id, value, found, err := eventEntityValue(data, tag)
		if (fullErr == nil) != (err == nil) {
			t.Fatalf("decoder error mismatch: %v / %v", fullErr, err)
		}
		if err == nil {
			want, exists := e.Tags[tag]
			if string(id) != want || found != exists || math.Float64bits(value) != math.Float64bits(e.Value) {
				t.Fatal("decoder result mismatch")
			}
		}
	})
}

func seedWindowBenchmark(tb testing.TB, s *Storage, count, entities int) {
	tb.Helper()
	for start := 0; start < count; start += 1000 {
		events := make([]Event, 0, 1000)
		for i := start; i < min(start+1000, count); i++ {
			events = append(events, Event{Timestamp: int64(i+1) * int64(time.Minute), Metric: "craftattack:clans:joins", Value: float64(i%5 - 1), Tags: map[string]string{"clan": fmt.Sprintf("clan:%06d", i%entities), "player": fmt.Sprintf("player:%d", i%10000), "server": "survival"}})
		}
		if err := s.WriteEvents(events, false); err != nil {
			tb.Fatal(err)
		}
	}
	if err := s.db.Flush(); err != nil {
		tb.Fatal(err)
	}
}

func TestWindowedSteadyStateMemory(t *testing.T) {
	s := windowTestStore(t)
	seedWindowBenchmark(t, s, 10000, 100)
	var retained [5]uint64
	for cycle := range retained {
		for i := 0; i < 50; i++ {
			got, err := s.WindowedLeaderboard("craftattack:clans:joins", "clan", 1, math.MaxInt64, 10, i%10)
			if err != nil || len(got) != 10 {
				t.Fatalf("%v %v", got, err)
			}
		}
		runtime.GC()
		var stats runtime.MemStats
		runtime.ReadMemStats(&stats)
		retained[cycle] = stats.HeapAlloc
	}
	t.Logf("live Go heap after each 50-query cycle (bytes): %v", retained)
	// Allow background Pebble bookkeeping while catching retained per-query
	// event collections or iterators. RSS is deliberately not treated as live heap.
	for _, live := range retained[1:] {
		if live > retained[0]+4<<20 {
			t.Fatalf("repeated queries retained memory: %v", retained)
		}
	}
}

func BenchmarkWindowedLeaderboard(b *testing.B) {
	const count = 200000
	s := windowTestStore(b)
	seedWindowBenchmark(b, s, count, 1000)
	for _, window := range []struct {
		name string
		from int64
	}{
		{"last7d", int64(count-7*24*60) * int64(time.Minute)},
		{"full_history", 1},
	} {
		b.Run(window.name, func(b *testing.B) {
			for _, implementation := range []struct {
				name string
				fn   func(string, string, int64, int64, int, int) ([]types.LeaderboardEntry, error)
			}{{"legacy", s.legacyWindowedLeaderboard}, {"optimized", s.WindowedLeaderboard}} {
				b.Run(implementation.name, func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						got, err := implementation.fn("craftattack:clans:joins", "clan", window.from, math.MaxInt64, 10, 0)
						if err != nil || len(got) != 10 {
							b.Fatalf("%v %v", got, err)
						}
					}
				})
			}
		})
	}
}
