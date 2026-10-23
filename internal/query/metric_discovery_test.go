package query

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/byPixelTV/flamedb/internal/aggregates"
	"github.com/byPixelTV/flamedb/internal/keyspace"
	"github.com/byPixelTV/flamedb/internal/storage"
	"github.com/cockroachdb/pebble/v2"
)

func TestMetricDiscoverySkipsPrefixesWithoutLosingNames(t *testing.T) {
	store, err := storage.Open(t.TempDir(), "none")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	exec := NewExecutor(store, aggregates.New(store.DB()))
	names := []string{"craftattack:blocks-broken", "craftattack:clans:joins", "playtime:sessions", "smp:kills", "a", "a.b", "a;", "aa", "idx.foo", "lb-entity.foo", "z"}
	events := []storage.Event{}
	for _, name := range names {
		for i := int64(1); i <= 50; i++ {
			events = append(events, storage.Event{Metric: name, Timestamp: i, Value: 1, Tags: map[string]string{"server": "one"}})
		}
	}
	if err := store.WriteEvents(events, true); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"board:only", "board:only\x1fnamed", "a\x1fnamed"} {
		if err := exec.lb.Set(name, "entity", 2); err != nil {
			t.Fatal(err)
		}
	}
	batch := store.DB().NewBatch()
	defer batch.Close()
	for _, prefix := range []string{"idx:", "lb:", "card:", "card-count:", "repl-outbox:", "repl-applied:"} {
		for i := 0; i < 100; i++ {
			if err := batch.Set([]byte(fmt.Sprintf("%sfake:%04d", prefix, i)), []byte("ignored"), nil); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
	want := append(append([]string{}, names...), "board:only")
	sort.Strings(want)
	got, err := exec.ListMetrics(nil)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("got %q, want %q, err=%v", got, want, err)
	}
	filtered, err := exec.ListMetrics(map[string]string{"server": "one"})
	sort.Strings(names)
	if err != nil || !reflect.DeepEqual(filtered, names) {
		t.Fatalf("filtered got %q, want %q, err=%v", filtered, names, err)
	}
}

func BenchmarkMetricDiscovery(b *testing.B) {
	store, err := storage.Open(b.TempDir(), "none")
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	exec := NewExecutor(store, aggregates.New(store.DB()))
	batch := store.DB().NewBatch()
	defer batch.Close()
	for metric := 0; metric < 10; metric++ {
		prefix := []byte(keyspace.Component(fmt.Sprintf("craftattack:metric-%02d", metric)) + ":")
		for event := 0; event < 10000; event++ {
			var stamp [8]byte
			binary.BigEndian.PutUint64(stamp[:], uint64(event))
			key := append(append([]byte{}, prefix...), stamp[:]...)
			if err := batch.Set(key, []byte("value"), nil); err != nil {
				b.Fatal(err)
			}
			if err := batch.Set(append([]byte("idx:"), key...), []byte("index"), nil); err != nil {
				b.Fatal(err)
			}
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		b.Fatal(err)
	}
	b.Run("prefix-seek", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			got, err := exec.listAllMetrics()
			if err != nil || len(got) != 10 {
				b.Fatalf("names=%d err=%v", len(got), err)
			}
		}
	})
	b.Run("full-scan", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			iter, err := store.DB().NewIter(&pebble.IterOptions{})
			if err != nil {
				b.Fatal(err)
			}
			count := 0
			for iter.First(); iter.Valid(); iter.Next() {
				count++
			}
			err = iter.Error()
			iter.Close()
			if err != nil || count != 200000 {
				b.Fatalf("keys=%d err=%v", count, err)
			}
		}
	})
}
