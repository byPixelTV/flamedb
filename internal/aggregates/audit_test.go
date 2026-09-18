package aggregates

import (
	"github.com/cockroachdb/pebble/v2"
	"sync"
	"testing"
)

func TestSignedLeaderboard(t *testing.T) {
	db, err := pebble.Open(t.TempDir(), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	l := New(db)
	for id, v := range map[string]float64{"a": -2, "b": 0, "c": 3, "d": -1} {
		if err := l.Set("m", id, v); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := l.TopN("m", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := []float64{3, 0, -1, -2}
	if len(rows) != len(want) {
		t.Fatalf("%v", rows)
	}
	for i, v := range want {
		if rows[i].Value != v {
			t.Fatalf("%v", rows)
		}
	}
}
func TestConcurrentSetAndIncrement(t *testing.T) {
	db, err := pebble.Open(t.TempDir(), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	l := New(db)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := l.Set("m", "p", 10); err != nil {
				t.Error(err)
			}
			if err := l.Increment("m", "p", 1); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	rows, err := l.TopN("m", 100, 0)
	if err != nil || len(rows) != 1 {
		t.Fatalf("%v %v", rows, err)
	}
}
