package aggregates

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2"
)

func BenchmarkLegacyLeaderboardLookup(b *testing.B) {
	db, err := pebble.Open(b.TempDir(), &pebble.Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	lb := New(db)
	const rows = 32768
	const metric = "craftattack:mob-kills"
	batch := db.NewBatch()
	for i := 0; i < rows; i++ {
		id := fmt.Sprintf("f3e5938d-9460-478c-a9a2-%012x", i)
		value, err := json.Marshal(LeaderboardEntry{EntityID: id, Value: 1})
		if err != nil {
			b.Fatal(err)
		}
		if err := batch.Set(lbKey(metric, id, 1), value, nil); err != nil {
			b.Fatal(err)
		}
		if err := batch.Set(lbEntityKey(metric, id), encodeFloat64(1), nil); err != nil {
			b.Fatal(err)
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		b.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		b.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		b.Fatal(err)
	}
	targets := map[string]map[string]struct{}{metric: {}}
	for i := 0; i < 128; i++ {
		targets[metric][fmt.Sprintf("fresh-%03d", i)] = struct{}{}
	}
	b.Run("missing-single", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var stats LookupStats
			if got, err := lb.GetWithLookupStats(metric, "fresh-000", &stats); err != nil || got != 0 || stats.LegacyEntries != rows {
				b.Fatalf("lookup: %v %v %+v", got, err, stats)
			}
		}
	})
	b.Run("missing-batch128", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var stats LookupStats
			reader := lb.NewLockedBatchLookup(targets, &stats)
			for id := range targets[metric] {
				if got, err := reader.Get(metric, id); err != nil || got != 0 {
					b.Fatalf("lookup: %v %v", got, err)
				}
			}
			if stats.LegacyScans != 1 || stats.LegacyEntries != rows {
				b.Fatalf("wrong scan: %+v", stats)
			}
		}
	})
	b.Run("indexed", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if got, err := lb.Get(metric, "f3e5938d-9460-478c-a9a2-000000000000"); err != nil || got != 1 {
				b.Fatalf("lookup: %v %v", got, err)
			}
		}
	})
}
