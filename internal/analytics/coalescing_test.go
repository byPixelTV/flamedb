package analytics

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/byPixelTV/flamedb/internal/query"
)

func TestPostgresCoalescedSmallBatches(t *testing.T) {
	e, _ := integration(t)
	ctx := context.Background()
	metrics := []string{"coal_a_" + token(), "coal_b_" + token(), "coal_c_" + token(), "coal_d_" + token()}
	for _, metric := range metrics {
		run(t, e, &query.Query{Type: query.QueryTypeSet, Metric: metric, Value: math.MaxFloat64, LBEntities: map[string]string{"overflow": "x"}})
	}
	start := make(chan struct{})
	var finished sync.WaitGroup
	for worker := 0; worker < 32; worker++ {
		finished.Add(1)
		go func(worker int) {
			defer finished.Done()
			<-start
			items := make([]*query.Query, 16)
			for i := range items {
				items[i] = &query.Query{Type: query.QueryTypeWrite, Metric: metrics[i%4], Timestamp: int64(worker*16 + i + 1), Value: 1, UpdateLB: true, LBEntityID: "x"}
			}
			// Rejection is local index 5 for every caller, even after merging.
			items[5].Value = math.MaxFloat64
			items[5].LBEntities = map[string]string{"overflow": "x"}
			result, err := e.ExecuteBatch(items)
			if err != nil || result.Accepted != 15 || result.Failed != 1 || len(result.Errors) != 1 || result.Errors[0].Index != 5 {
				t.Errorf("caller %d result: %+v %v", worker, result, err)
			}
		}(worker)
	}
	close(start)
	finished.Wait()
	if e.ingest.merged.Load() < 2 {
		t.Fatal("no concurrent requests were actually coalesced")
	}
	for i, metric := range metrics {
		want := 128
		if i == 1 {
			want = 96
		}
		if count := run(t, e, &query.Query{Type: query.QueryTypeGet, Metric: metric, Aggregate: query.AggCount}).Aggregate.Count; count != want {
			t.Fatalf("event count %d != %d", count, want)
		}
		if score := run(t, e, &query.Query{Type: query.QueryTypeScore, Metric: metric, EntityID: "x"}).Ranking.Score; score == nil || *score != float64(want) {
			t.Fatalf("score %v != %d", score, want)
		}
	}
	// A queued cancelled request is removed before dispatch and cannot poison
	// the live request's deadline or get attributed another client's errors.
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	dead := &ingestRequest{ctx: cancelled, items: []*query.Query{{Metric: metrics[0], Value: 99, UpdateLB: true, LBEntityID: "x"}}, result: make(chan ingestResult, 1)}
	live := &ingestRequest{ctx: ctx, items: []*query.Query{{Type: query.QueryTypeWrite, Metric: metrics[0], Timestamp: 10000, Value: 2, UpdateLB: true, LBEntityID: "x"}}, result: make(chan ingestResult, 1)}
	e.ingest.execute([]*ingestRequest{dead, live}, 2)
	if result := <-dead.result; result.err != context.Canceled {
		t.Fatalf("cancelled admission: %+v", result)
	}
	if result := <-live.result; result.err != nil || len(result.rejected) != 0 {
		t.Fatalf("live admission: %+v", result)
	}
	if score := run(t, e, &query.Query{Type: query.QueryTypeScore, Metric: metrics[0], EntityID: "x"}).Ranking.Score; score == nil || *score != 130 {
		t.Fatalf("cancellation changed score: %v", score)
	}
	// Close cancels an in-flight SQL lock wait, drains queued callers and does
	// not return before its dispatcher and metric workers have stopped.
	blocker, err := e.db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(ctx)
	if err = lockMetric(ctx, blocker, metrics[0]); err != nil {
		t.Fatal(err)
	}
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		_, _ = e.ingest.submit(ctx, []*query.Query{{Type: query.QueryTypeWrite, Metric: metrics[0], Timestamp: 20000, Value: 1, UpdateLB: true, LBEntityID: "x"}})
	}()
	time.Sleep(20 * time.Millisecond)
	independent := make(chan error, 1)
	go func() {
		_, err := e.ingest.submit(ctx, []*query.Query{{Type: query.QueryTypeWrite, Metric: metrics[3], Timestamp: 20000, Value: 1, UpdateLB: true, LBEntityID: "x"}})
		independent <- err
	}()
	select {
	case err := <-independent:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked wave serialized independent caller")
	}
	cancelCtx, cancelActive := context.WithCancel(ctx)
	cancelResult := make(chan error, 1)
	go func() {
		_, err := e.ingest.submit(cancelCtx, []*query.Query{{Type: query.QueryTypeWrite, Metric: metrics[0], Timestamp: 30000, Value: 1, UpdateLB: true, LBEntityID: "x"}})
		cancelResult <- err
	}()
	until := time.Now().Add(time.Second)
	for e.ingest.active.Load() != 2 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if e.ingest.active.Load() != 2 {
		cancelActive()
		t.Fatal("cancelled request never reached an active SQL wave")
	}
	cancelActive()
	select {
	case err := <-cancelResult:
		if err == nil {
			t.Fatal("cancelled lock wait was accepted")
		}
	case <-time.After(time.Second):
		t.Fatal("all-participant cancellation did not interrupt SQL lock wait")
	}
	closed := make(chan struct{})
	go func() { e.ingest.close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("coalescer shutdown hung")
	}
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("admitted caller did not settle on shutdown")
	}
	if _, err = e.ingest.submit(ctx, live.items); err != context.Canceled {
		t.Fatalf("closed dispatcher accepted request: %v", err)
	}
}
