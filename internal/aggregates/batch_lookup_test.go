package aggregates

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

func TestLockedBatchLookupPreservesLegacyAndIndexedScores(t *testing.T) {
	db, err := pebble.Open("db", &pebble.Options{FS: vfs.NewMem()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	lb := New(db)
	put := func(metric, entity string, value float64) {
		t.Helper()
		data, err := json.Marshal(LeaderboardEntry{EntityID: entity, Value: value})
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Set(lbKey(metric, entity, value), data, pebble.Sync); err != nil {
			t.Fatal(err)
		}
	}
	put("m", "legacy", 7)
	put("m", "legacy", 10)
	put("m", "indexed", 99)
	if err := db.Set(lbEntityKey("m", "indexed"), encodeFloat64(12), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	named := BoardMetric("m", "clan")
	put(named, "legacy", -3)
	targets := map[string]map[string]struct{}{"m": {"legacy": {}, "indexed": {}, "missing": {}}, named: {"legacy": {}, "missing": {}}}
	var stats LookupStats
	err = lb.AtomicMutationMany([]string{"m:legacy", "m:indexed", "m:missing", named + ":legacy", named + ":missing"}, func(batch *pebble.Batch) error {
		reader := lb.NewLockedBatchLookup(targets, &stats)
		for _, metric := range []string{"m", named} {
			for entity := range targets[metric] {
				want, err := lb.Get(metric, entity)
				if err != nil {
					return err
				}
				got, err := reader.Get(metric, entity)
				if err != nil {
					return err
				}
				if got != want {
					t.Fatalf("legacy/indexed score changed: %v != %v", got, want)
				}
			}
		}
		if _, err := reader.Get("m", "not-locked"); err == nil {
			t.Fatal("lookup accepted an unlocked target")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.LegacyScans != 2 {
		t.Fatalf("want one scan per board, got %+v", stats)
	}
	// A later mutation gets a fresh lookup, never a stale missing-score cache.
	if err := lb.Set("m", "missing", 4); err != nil {
		t.Fatal(err)
	}
	reader := lb.NewLockedBatchLookup(targets, nil)
	got, err := reader.Get("m", "missing")
	if err != nil || got != 4 {
		t.Fatal(got, err)
	}
}

func TestLockedBatchLookupBoundsWorkForNewMobEntities(t *testing.T) {
	db, err := pebble.Open("db", &pebble.Options{FS: vfs.NewMem()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	lb := New(db)
	batch := db.NewBatch()
	const existing = 8192
	for i := 0; i < existing; i++ {
		entity := fmt.Sprintf("old-mob-%d", i)
		data, _ := json.Marshal(LeaderboardEntry{EntityID: entity, Value: 1})
		if err := batch.Set(lbKey("m", entity, 1), data, nil); err != nil {
			t.Fatal(err)
		}
		if err := batch.Set(lbEntityKey("m", entity), encodeFloat64(1), nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
	batch.Close()
	targets := map[string]map[string]struct{}{"m": {}}
	var before, after LookupStats
	for i := 0; i < 128; i++ {
		entity := fmt.Sprintf("new-mob-%d", i)
		targets["m"][entity] = struct{}{}
		if got, err := lb.GetWithLookupStats("m", entity, &before); err != nil || got != 0 {
			t.Fatal(got, err)
		}
	}
	reader := lb.NewLockedBatchLookup(targets, &after)
	for entity := range targets["m"] {
		if got, err := reader.Get("m", entity); err != nil || got != 0 {
			t.Fatal(got, err)
		}
	}
	if before.LegacyScans != 128 || before.LegacyEntries != 128*existing || after.LegacyScans != 1 || after.LegacyEntries != existing {
		t.Fatalf("unbounded repeated scans: before=%+v after=%+v", before, after)
	}
	t.Logf("MOB_ENTITY_SCAN_WORK_VERIFIED before=%d after=%d", before.LegacyEntries, after.LegacyEntries)
}
