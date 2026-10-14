package analytics

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/byPixelTV/flamedb/internal/query"
)

func TestPostgresIndependentMetricBatches(t *testing.T) {
	e, options := integration(t)
	ctx := context.Background()
	t.Run("blocked-metric-does-not-block-independent-metric", func(t *testing.T) {
		prefix := "independent_" + token()
		slow, fast := prefix+"_a", prefix+"_b"
		// A coherent warm board lets the cache publication itself demonstrate
		// progress while another metric's SQL transaction is still blocked.
		run(t, e, &query.Query{Type: query.QueryTypeSet, Metric: fast, Value: 0, UpdateLB: true, LBEntityID: "x"})
		run(t, e, &query.Query{Type: query.QueryTypeScore, Metric: fast, EntityID: "x"})
		blocker, err := e.db.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer blocker.Rollback(ctx)
		if err = lockMetric(ctx, blocker, slow); err != nil {
			t.Fatal(err)
		}
		deadline, cancel := context.WithTimeout(ctx, 4*time.Second)
		defer cancel()
		type outcome struct {
			batch *query.BatchResult
			err   error
		}
		done := make(chan outcome, 1)
		items := make([]*query.Query, 128)
		for i := range items {
			metric := slow
			if i%2 == 1 {
				metric = fast
			}
			items[i] = &query.Query{Type: query.QueryTypeWrite, Metric: metric, Timestamp: int64(i/2 + 1), Value: 1, UpdateLB: true, LBEntityID: "x"}
		}
		go func() {
			batch, err := e.ExecuteBatchContext(deadline, items)
			done <- outcome{batch, err}
		}()
		observed := false
		cacheObserved := e.cache == nil
		until := time.Now().Add(time.Second)
		for time.Now().Before(until) {
			var score float64
			if err = e.db.QueryRow(ctx, `SELECT value FROM flame_scores WHERE metric=$1 AND board='' AND entity='x'`, fast).Scan(&score); err == nil && score == 64 {
				observed = true
			}
			if e.cache != nil {
				version, _ := e.cache.HGet(ctx, metricKey(fast)+":meta", "version").Result()
				cacheObserved = version == "2"
			}
			if observed && cacheObserved {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		select {
		case result := <-done:
			t.Fatalf("batch returned before blocked metric completed: %+v %v", result.batch, result.err)
		default:
		}
		if err = blocker.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		result := <-done
		if !observed || !cacheObserved || result.err != nil || result.batch.Accepted != len(items) || result.batch.Failed != 0 {
			t.Fatalf("independent progress: observed=%t cache=%t batch=%+v err=%v", observed, cacheObserved, result.batch, result.err)
		}
	})
	t.Run("ordered-partial-rejection-indices", func(t *testing.T) {
		a, b := "ordered_a_"+token(), "ordered_b_"+token()
		for _, metric := range []string{a, b} {
			run(t, e, &query.Query{Type: query.QueryTypeSet, Metric: metric, Value: math.MaxFloat64, LBEntities: map[string]string{"overflow": "x"}})
		}
		items := []*query.Query{
			{Type: query.QueryTypeWrite, Metric: a, Timestamp: 1, Value: 1, UpdateLB: true, LBEntityID: "x"},
			{Type: query.QueryTypeWrite, Metric: b, Timestamp: 1, Value: 2, UpdateLB: true, LBEntityID: "x"},
			{Type: query.QueryTypeWrite, Metric: a, Timestamp: 2, Value: math.MaxFloat64, UpdateLB: true, LBEntityID: "x", LBEntities: map[string]string{"overflow": "x"}},
			{Type: query.QueryTypeWrite, Metric: b, Timestamp: 2, Value: math.MaxFloat64, UpdateLB: true, LBEntityID: "x", LBEntities: map[string]string{"overflow": "x"}},
			{Type: query.QueryTypeWrite, Metric: a, Timestamp: 3, Value: 3, UpdateLB: true, LBEntityID: "x"},
		}
		for i := 0; i < 64; i++ {
			metric := a
			if i%2 == 1 {
				metric = b
			}
			items = append(items, &query.Query{Type: query.QueryTypeWrite, Metric: metric, Timestamp: int64(i/2 + 4), Value: 0, UpdateLB: true, LBEntityID: "x"})
		}
		result, err := e.ExecuteBatch(items)
		if err != nil || result.Accepted != len(items)-2 || result.Failed != 2 || len(result.Errors) != 2 || result.Errors[0].Index != 2 || result.Errors[1].Index != 3 {
			t.Fatalf("global rejection positions: %+v %v", result, err)
		}
		for metric, want := range map[string]float64{a: 4, b: 2} {
			if score := run(t, e, &query.Query{Type: query.QueryTypeScore, Metric: metric, EntityID: "x"}).Ranking.Score; score == nil || *score != want {
				t.Fatalf("ordered partial score: %v", score)
			}
			if count := run(t, e, &query.Query{Type: query.QueryTypeGet, Metric: metric, From: 2, To: 3, Aggregate: query.AggCount}).Aggregate.Count; count != 0 {
				t.Fatal("rejected increment persisted an event")
			}
		}
	})
	t.Run("single-connection-and-cancellation", func(t *testing.T) {
		options.MaxConnections = 1
		one, err := Open(ctx, options)
		if err != nil {
			t.Fatal(err)
		}
		defer one.Close()
		items := make([]*query.Query, 64)
		metrics := []string{"one_a_" + token(), "one_b_" + token()}
		for i := range items {
			items[i] = &query.Query{Type: query.QueryTypeWrite, Metric: metrics[i%2], Timestamp: int64(i/2 + 1), Value: 1, UpdateLB: true, LBEntityID: "x"}
		}
		result, err := one.ExecuteBatch(items)
		if err != nil || result.Accepted != len(items) || result.Failed != 0 {
			t.Fatalf("single-connection batch: %+v %v", result, err)
		}
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		result, err = one.ExecuteBatchContext(cancelled, items)
		if err != nil || result.Accepted != 0 || result.Failed != len(items) || len(result.Errors) != len(items) {
			t.Fatalf("cancelled jobs disappeared: %+v %v", result, err)
		}
		for _, metric := range metrics {
			if score := run(t, one, &query.Query{Type: query.QueryTypeScore, Metric: metric, EntityID: "x"}).Ranking.Score; score == nil || *score != 32 {
				t.Fatal("cancelled jobs changed acknowledged scores")
			}
		}
	})
}
