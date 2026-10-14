package analytics

import (
	"context"
	"sync"
	"testing"

	"github.com/byPixelTV/flamedb/internal/query"
)

func TestPostgresSharedAutomaticTimestampAllocator(t *testing.T) {
	e, options := integration(t)
	peer, err := Open(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	const fixed int64 = 1_000_000_000
	e.now = func() int64 { return fixed }
	peer.now = func() int64 { return fixed }
	m := "auto_clock_" + token()
	for _, timestamp := range []int64{fixed, fixed + 3, fixed + 1_000_000} {
		run(t, e, &query.Query{Type: query.QueryTypeWrite, Metric: m, Timestamp: timestamp, Value: 7})
	}
	// Both independent API processes observe exactly the same physical clock.
	// Existing explicit keys and a same-batch explicit key must stay reserved.
	batches := make([][]*query.Query, 2)
	for node := range batches {
		for i := 0; i < 50; i++ {
			batches[node] = append(batches[node], &query.Query{Type: query.QueryTypeWrite, Metric: m, Value: 1, UpdateLB: true, LBEntityID: "x"})
		}
		batches[node] = append(batches[node], &query.Query{Type: query.QueryTypeWrite, Metric: m, Timestamp: fixed + 3, Value: float64(9 + node)})
		batches[node] = append(batches[node], &query.Query{Type: query.QueryTypeWrite, Metric: m, Timestamp: fixed + 6, Value: float64(11 + node)})
	}
	start := make(chan struct{})
	failures := make(chan error, 2)
	var wg sync.WaitGroup
	for node, engine := range []*Engine{e, peer} {
		wg.Add(1)
		go func(node int, engine *Engine) {
			defer wg.Done()
			<-start
			result, err := engine.ExecuteBatch(batches[node])
			if err != nil {
				failures <- err
				return
			}
			if result.Accepted != 52 || result.Failed != 0 {
				t.Errorf("auto batch %d: %v", node, result)
			}
		}(node, engine)
	}
	close(start)
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	seen := map[int64]bool{}
	for _, batch := range batches {
		for _, q := range batch[:50] {
			if q.Timestamp <= fixed || q.Timestamp >= fixed+1_000_000 || q.Timestamp == fixed+3 || q.Timestamp == fixed+6 || seen[q.Timestamp] {
				t.Fatalf("colliding or future-shifted auto timestamp %d", q.Timestamp)
			}
			seen[q.Timestamp] = true
		}
	}
	events := run(t, e, &query.Query{Type: query.QueryTypeGet, Metric: m}).Events
	if len(events) != 104 {
		t.Fatalf("acknowledged auto event lost: %d, want 104", len(events))
	}
	score := run(t, e, &query.Query{Type: query.QueryTypeScore, Metric: m, EntityID: "x"}).Ranking.Score
	if score == nil || *score != 100 {
		t.Fatalf("auto score %v", score)
	}
	// The durable allocator survives a fresh API process with a reset clock.
	restarted, err := Open(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	restarted.now = func() int64 { return fixed }
	q := &query.Query{Type: query.QueryTypeWrite, Metric: m, Value: 1, UpdateLB: true, LBEntityID: "x", IsReplica: true, OperationID: token()}
	run(t, restarted, q)
	if seen[q.Timestamp] || q.Timestamp <= fixed || q.Timestamp >= fixed+1_000_000 {
		t.Fatalf("restarted allocator reused timestamp %d", q.Timestamp)
	}
	var lastBefore, lastAfter int64
	if err = e.db.QueryRow(context.Background(), `SELECT last_ns FROM flame_metric_clock WHERE metric=$1`, EncodeString(m)).Scan(&lastBefore); err != nil {
		t.Fatal(err)
	}
	run(t, peer, &query.Query{Type: query.QueryTypeWrite, Metric: m, Value: 1, UpdateLB: true, LBEntityID: "x", IsReplica: true, OperationID: q.OperationID})
	if err = e.db.QueryRow(context.Background(), `SELECT last_ns FROM flame_metric_clock WHERE metric=$1`, EncodeString(m)).Scan(&lastAfter); err != nil {
		t.Fatal(err)
	}
	if lastAfter != lastBefore {
		t.Fatal("duplicate replica advanced durable clock")
	}
	if len(run(t, e, &query.Query{Type: query.QueryTypeGet, Metric: m}).Events) != 105 {
		t.Fatal("restart or replica replay lost/duplicated auto events")
	}
	// Imported explicit timestamps can fill several allocator pages. Paging
	// must find the first gap without buffering all imported history.
	denseMetric := "auto_dense_" + token()
	dense := make([]*query.Query, 1200)
	for i := range dense {
		dense[i] = &query.Query{Type: query.QueryTypeWrite, Metric: denseMetric, Timestamp: fixed + int64(i), Value: 1}
	}
	seed, err := restarted.ExecuteBatch(dense)
	if err != nil || seed.Accepted != 1200 {
		t.Fatalf("dense timestamp seed: %v %v", seed, err)
	}
	auto := &query.Query{Type: query.QueryTypeWrite, Metric: denseMetric, Value: 1}
	run(t, restarted, auto)
	if auto.Timestamp != fixed+1200 {
		t.Fatalf("occupied timestamp paging chose %d", auto.Timestamp)
	}
	count := run(t, restarted, &query.Query{Type: query.QueryTypeGet, Metric: denseMetric, Aggregate: query.AggCount}).Aggregate.Count
	if count != 1201 {
		t.Fatalf("dense timestamp overwrite: %d", count)
	}
}
