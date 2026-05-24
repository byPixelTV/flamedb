package query

import (
	"testing"

	"github.com/byPixelTV/flamedb/internal/aggregates"
	"github.com/byPixelTV/flamedb/internal/storage"
)

func TestExecutorExecuteBatchWritesEvents(t *testing.T) {
	store, err := storage.Open(t.TempDir(), "none")
	if err != nil {
		t.Fatalf("storage.Open() error = %v", err)
	}
	defer store.Close()

	exec := NewExecutor(store, aggregates.New(store.DB()))
	result, err := exec.ExecuteBatch([]*Query{
		{
			Type:   QueryTypeWrite,
			Metric: "kills",
			Value:  1,
			Tags:   map[string]string{"player": "pixel"},
		},
		{
			Type:       QueryTypeWrite,
			Metric:     "kills",
			Value:      5,
			Tags:       map[string]string{"player": "pixel"},
			UpdateLB:   true,
			LBEntityID: "pixel",
		},
	})
	if err != nil {
		t.Fatalf("ExecuteBatch() error = %v", err)
	}
	if !result.OK || result.Accepted != 2 || result.Failed != 0 {
		t.Fatalf("ExecuteBatch() result = %+v, want 2 accepted", result)
	}

	events, err := store.ReadRange("kills", 0, 1<<62)
	if err != nil {
		t.Fatalf("ReadRange() error = %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("len(events) = %d, want 2", len(events))
	}
	value, err := aggregates.New(store.DB()).Get("kills", "pixel")
	if err != nil {
		t.Fatalf("Leaderboard.Get() error = %v", err)
	}
	if value != 5 {
		t.Fatalf("leaderboard value = %v, want 5", value)
	}
}
