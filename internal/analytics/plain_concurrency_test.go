package analytics

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/byPixelTV/flamedb/internal/query"
)

func TestPostgresParallelRawBatches(t *testing.T) {
	e, options := integration(t)
	metric := "parallel_raw_" + token()
	// A live ranking snapshot holds a shared metric lock. Raw explicit COPY
	// must progress without invalidating scores; allocation/mutation must wait.
	snapshot, err := e.db.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Rollback(context.Background())
	if _, err = snapshot.Exec(context.Background(), `SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))`, EncodeString(metric)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	batch, err := e.ExecuteBatchContext(ctx, []*query.Query{{Type: query.QueryTypeWrite, Metric: metric, Timestamp: 1, Value: 1}})
	if err != nil || batch.Accepted != 1 || batch.Failed != 0 {
		t.Fatalf("raw batch blocked ranking snapshot: %v %v", batch, err)
	}
	blocked, done := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err = e.ExecuteContext(blocked, &query.Query{Type: query.QueryTypeWrite, Metric: metric, Value: 999})
	done()
	if err == nil {
		t.Fatal("automatic allocation bypassed snapshot protection")
	}
	if err = snapshot.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	base := "raw_overwrite_" + token()
	var wg sync.WaitGroup
	errors := make(chan error, 12)
	for worker := 0; worker < 12; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			items := make([]*query.Query, 192)
			for i := range items {
				items[i] = &query.Query{Type: query.QueryTypeWrite, Metric: fmt.Sprintf("%s_%02d", base, i%8), Timestamp: int64(i/8 + 1), Value: float64(worker), Tags: map[string]string{"worker": strconv.Itoa(worker), "common": "same"}}
			}
			result, err := e.ExecuteBatch(items)
			if err != nil || result.Accepted != len(items) || result.Failed != 0 {
				errors <- fmt.Errorf("worker%d: %v %v", worker, result, err)
			}
		}(worker)
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		m := fmt.Sprintf("%s_%02d", base, i)
		events := run(t, e, &query.Query{Type: query.QueryTypeGet, Metric: m, Limit: 100}).Events
		if len(events) != 24 {
			t.Fatalf("lost/torn overwrite for%s: %d", m, len(events))
		}
		for _, event := range events {
			worker, err := strconv.Atoi(event.Tags["worker"])
			if err != nil || event.Value != float64(worker) || event.Tags["common"] != "same" {
				t.Fatalf("torn event:%v", event)
			}
		}
		stats := run(t, e, &query.Query{Type: query.QueryTypeStats, Metric: m, TagKeys: []string{"worker", "common"}}).Stats
		if stats.TagStats[0].Cardinality != 12 || stats.TagStats[1].Cardinality != 1 {
			t.Fatalf("historical tags lost:%v", stats)
		}
		if count := run(t, e, &query.Query{Type: query.QueryTypeLeaderboardCount, Metric: m}).Ranking.Count; count != 0 {
			t.Fatal("raw writes created scores")
		}
	}
	t.Run("connection_reuse", func(t *testing.T) {
		options.MaxConnections = 1
		reused, err := Open(context.Background(), options)
		if err != nil {
			t.Fatal(err)
		}
		defer reused.Close()
		m := "reuse_" + token()
		for i := 1; i <= 4; i++ {
			result, err := reused.ExecuteBatch([]*query.Query{{Type: query.QueryTypeWrite, Metric: m, Timestamp: int64(i), Value: 3, UpdateLB: true, LBEntityID: "x"}})
			if err != nil || result.Accepted != 1 || result.Failed != 0 {
				t.Fatalf("reuse%d:%v %v", i, result, err)
			}
		}
		if count := run(t, reused, &query.Query{Type: query.QueryTypeGet, Metric: m, Aggregate: query.AggCount}).Aggregate.Count; count != 4 {
			t.Fatalf("stale staging events:%d", count)
		}
		if score := run(t, reused, &query.Query{Type: query.QueryTypeScore, Metric: m, EntityID: "x"}).Ranking.Score; score == nil || *score != 12 {
			t.Fatalf("stale staging scores:%v", score)
		}
	})
}
