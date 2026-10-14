package query

import (
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/byPixelTV/flamedb/internal/aggregates"
	"github.com/byPixelTV/flamedb/internal/storage"
	dto "github.com/prometheus/client_model/go"
)

func TestPebbleBatchSingleDurableWALCommit(t *testing.T) {
	store, err := storage.Open(t.TempDir(), "none")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	exec := NewExecutor(store, aggregates.New(store.DB()))
	count := func() uint64 {
		var metric dto.Metric
		if err := store.DB().Metrics().LogWriter.FsyncLatency.Write(&metric); err != nil {
			t.Fatal(err)
		}
		return metric.GetHistogram().GetSampleCount()
	}
	queries := make([]*Query, 128)
	for i := range queries {
		queries[i] = &Query{Type: QueryTypeWrite, Metric: "m", Value: 1, Timestamp: int64(i + 1), UpdateLB: true, LBEntityID: "p", LBEntities: map[string]string{"team": "a"}}
	}
	before := count()
	r, err := exec.ExecuteBatch(queries)
	if err != nil || !r.OK || r.Accepted != 128 {
		t.Fatal(r, err)
	}
	if got := count() - before; got != 1 {
		t.Fatalf("128 leaderboard events used %d durable WAL syncs, want 1", got)
	}
}

func TestPebbleBatchScoresHistoryAndReopen(t *testing.T) {
	path := t.TempDir()
	store, err := storage.Open(path, "none")
	if err != nil {
		t.Fatal(err)
	}
	exec := NewExecutor(store, aggregates.New(store.DB()))
	queries := []*Query{}
	values := []float64{1e16, 1, -1e16, 3, -3, -1}
	for i, value := range values {
		queries = append(queries, &Query{Type: QueryTypeWrite, Metric: "smp:kills", Timestamp: int64(i + 1), Value: value, UpdateLB: true, LBEntityID: "玩家", LBEntities: map[string]string{"clan": "a"}, Tags: map[string]string{"player": "玩家"}})
	}
	r, err := exec.ExecuteBatch(queries)
	if err != nil || !r.OK || r.Accepted != len(queries) {
		t.Fatalf("%+v %v", r, err)
	}
	check := func() {
		t.Helper()
		lb := aggregates.New(store.DB())
		for _, metric := range []string{"smp:kills", aggregates.BoardMetric("smp:kills", "clan")} {
			entries, err := lb.TopN(metric, 0, 0)
			if err != nil || len(entries) != 1 || entries[0].Value != -1 {
				t.Fatalf("stale intermediate scores: %s %+v %v", metric, entries, err)
			}
		}
		events, err := store.ReadRange("smp:kills", 0, 1<<62)
		if err != nil || len(events) != len(values) {
			t.Fatalf("history: %+v %v", events, err)
		}
		for i, event := range events {
			if event.Value != values[i] || event.Tags["player"] != "玩家" {
				t.Fatal("payload changed", event)
			}
		}
	}
	check()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = storage.Open(path, "none")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	check()
}

func TestPebbleBatchPartialOverflowAndOverwrite(t *testing.T) {
	store, err := storage.Open(t.TempDir(), "none")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	lb := aggregates.New(store.DB())
	exec := NewExecutor(store, lb)
	if err := lb.Set(aggregates.BoardMetric("m", "bad"), "p", math.MaxFloat64); err != nil {
		t.Fatal(err)
	}
	r, err := exec.ExecuteBatch([]*Query{
		{Type: QueryTypeWrite, Metric: "m", Timestamp: 1, Value: 2, Tags: map[string]string{"player": "old"}, UpdateLB: true, LBEntityID: "p"},
		{Type: QueryTypeWrite, Metric: "m", Timestamp: 2, Value: math.MaxFloat64, UpdateLB: true, LBEntityID: "p", LBEntities: map[string]string{"bad": "p"}},
		{Type: QueryTypeWrite, Metric: "m", Timestamp: 1, Value: 3, Tags: map[string]string{"player": "new"}, UpdateLB: true, LBEntityID: "p"},
		{Type: QueryTypeWrite, Metric: "m", Value: math.NaN()}, nil,
	})
	if err != nil || r.OK || r.Accepted != 2 || r.Failed != 3 {
		t.Fatalf("%+v %v", r, err)
	}
	value, err := lb.Get("m", "p")
	if err != nil || value != 5 {
		t.Fatal("partially applied failed item", value, err)
	}
	value, err = lb.Get(aggregates.BoardMetric("m", "bad"), "p")
	if err != nil || value != math.MaxFloat64 {
		t.Fatal(value, err)
	}
	for player, count := range map[string]int{"old": 0, "new": 1} {
		q := &Query{Type: QueryTypeGet, Metric: "m", Where: map[string]string{"player": player}}
		r, err := exec.Execute(q)
		if err != nil || len(r.Events) != count {
			t.Fatal("stale tag index", player, r, err)
		}
	}
	events, err := store.ReadRange("m", 0, 1<<62)
	if err != nil || len(events) != 1 || events[0].Value != 3 {
		t.Fatal("rejected item created history", events, err)
	}
}

func TestPebbleBatchReplicaReceiptsWithinBatchAndRestart(t *testing.T) {
	path := t.TempDir()
	store, err := storage.Open(path, "none")
	if err != nil {
		t.Fatal(err)
	}
	queries := []*Query{}
	for i := 0; i < 4; i++ {
		queries = append(queries, &Query{Type: QueryTypeWrite, Metric: "m", Timestamp: 42, Value: 5, UpdateLB: true, LBEntityID: "p", IsReplica: true, OperationID: "same-receipt"})
	}
	for run := 0; run < 2; run++ {
		lb := aggregates.New(store.DB())
		exec := NewExecutor(store, lb)
		r, err := exec.ExecuteBatch(queries)
		if err != nil || !r.OK || r.Accepted != 4 {
			t.Fatal(r, err)
		}
		value, err := lb.Get("m", "p")
		if err != nil || value != 5 {
			t.Fatal("replayed replica", value, err)
		}
		events, err := store.ReadRange("m", 0, 1<<62)
		if err != nil || len(events) != 1 {
			t.Fatal(events, err)
		}
		if run == 0 {
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = storage.Open(path, "none")
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	store.Close()
}

func TestPebbleBatchConcurrentSharedScores(t *testing.T) {
	store, err := storage.Open(t.TempDir(), "none")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	lb := aggregates.New(store.DB())
	exec := NewExecutor(store, lb)
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for n := 0; n < 16; n++ {
				qs := []*Query{}
				for i := 0; i < 16; i++ {
					qs = append(qs, &Query{Type: QueryTypeWrite, Metric: "m", Value: 1, UpdateLB: true, LBEntityID: "p", LBEntities: map[string]string{"team": "shared"}, Tags: map[string]string{"worker": fmt.Sprint(worker)}})
				}
				r, err := exec.ExecuteBatch(qs)
				if err != nil || !r.OK || r.Accepted != 16 {
					t.Errorf("%+v %v", r, err)
					return
				}
				if _, err := exec.Execute(&Query{Type: QueryTypeWrite, Metric: "m", Value: 1, UpdateLB: true, LBEntityID: "p", LBEntities: map[string]string{"team": "shared"}}); err != nil {
					t.Error(err)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	for _, metric := range []string{"m", aggregates.BoardMetric("m", "team")} {
		entries, err := lb.TopN(metric, 0, 0)
		if err != nil || len(entries) != 1 || entries[0].Value != 8*16*17 {
			t.Fatal("lost concurrent updates", entries, err)
		}
	}
	events, err := store.ReadRange("m", 0, 1<<62)
	if err != nil || len(events) != 8*16*17 {
		t.Fatal("lost history", len(events), err)
	}
}

func TestPebbleBatchConcurrentSetCannotSplitBatch(t *testing.T) {
	store, err := storage.Open(t.TempDir(), "none")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	lb := aggregates.New(store.DB())
	exec := NewExecutor(store, lb)
	for attempt := 0; attempt < 16; attempt++ {
		metric := fmt.Sprintf("set-race:%d", attempt)
		queries := make([]*Query, 128)
		for i := range queries {
			queries[i] = &Query{Type: QueryTypeWrite, Metric: metric, Value: 1, UpdateLB: true, LBEntityID: "p"}
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			r, err := exec.ExecuteBatch(queries)
			if err != nil || !r.OK || r.Accepted != 128 {
				t.Error(r, err)
			}
		}()
		go func() {
			defer wg.Done()
			<-start
			_, err := exec.Execute(&Query{Type: QueryTypeSet, Metric: metric, Value: 1000, UpdateLB: true, LBEntityID: "p"})
			if err != nil {
				t.Error(err)
			}
		}()
		close(start)
		wg.Wait()
		entries, err := lb.TopN(metric, 0, 0)
		if err != nil || len(entries) != 1 || (entries[0].Value != 1000 && entries[0].Value != 1128) {
			t.Fatal("SET split a batch or left stale keys", entries, err)
		}
		events, err := store.ReadRange(metric, 0, 1<<62)
		if err != nil || len(events) != 128 {
			t.Fatal("SET changed raw history", len(events), err)
		}
	}
}
