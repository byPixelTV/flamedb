package analytics

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"testing"

	"github.com/byPixelTV/flamedb/internal/query"
)

func largeID(t *testing.T, bytes int) string {
	t.Helper()
	data := make([]byte, bytes/2)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(data)
}

func TestPostgresLargeProtocolIdentities(t *testing.T) {
	e, options := integration(t)
	ctx := context.Background()
	entity, board, tag := largeID(t, 4096), largeID(t, 4096), largeID(t, 4096)
	first, second := largeID(t, 65536), largeID(t, 65536)
	metrics := []string{largeID(t, 4096), "large_fields_" + token()}
	for _, metric := range metrics {
		for _, item := range []struct {
			value    float64
			tagValue string
		}{{3, first}, {5, second}} {
			run(t, e, &query.Query{Type: query.QueryTypeWrite, Metric: metric, Timestamp: 100, Value: item.value, UpdateLB: true, LBEntityID: entity, LBEntities: map[string]string{board: "n"}, Tags: map[string]string{tag: item.tagValue}})
		}
		batch := make([]*query.Query, 4)
		for i := range batch {
			batch[i] = &query.Query{Type: query.QueryTypeWrite, Metric: metric, Timestamp: int64(101 + i), Value: 1, UpdateLB: true, LBEntityID: entity, LBEntities: map[string]string{board: "n"}, Tags: map[string]string{tag: first}}
		}
		got, err := e.ExecuteBatch(batch)
		if err != nil || got.Accepted != 4 || got.Failed != 0 {
			t.Fatalf("large COPY: %+v %v", got, err)
		}
		all := run(t, e, &query.Query{Type: query.QueryTypeGet, Metric: metric, Aggregate: query.AggSum}).Aggregate
		if all.Count != 5 || all.Value != 9 {
			t.Fatalf("large timestamp overwrite: %+v", all)
		}
		if got := run(t, e, &query.Query{Type: query.QueryTypeGet, Metric: metric, Where: map[string]string{tag: second}}).Events; len(got) != 1 || got[0].Tags[tag] != second {
			t.Fatal("large tag filtering lost exact text")
		}
		if got := run(t, e, &query.Query{Type: query.QueryTypeStats, Metric: metric, TagKeys: []string{tag}}).Stats.TagStats[0].Cardinality; got != 2 {
			t.Fatalf("large tag deduplication: %d", got)
		}
		for _, target := range []struct{ board, entity string }{{"", entity}, {board, "n"}} {
			r := run(t, e, &query.Query{Type: query.QueryTypeRank, Metric: metric, Board: target.board, EntityID: target.entity}).Ranking
			if r.Count != 1 || r.Score == nil || *r.Score != 12 || r.Rank == nil || *r.Rank != 1 {
				t.Fatalf("large exact ranking: %+v", r)
			}
		}
		run(t, e, &query.Query{Type: query.QueryTypeSet, Metric: metric, Value: -2, UpdateLB: true, LBEntityID: entity})
		if err = e.Rebuild(ctx); err != nil {
			t.Fatal(err)
		}
		if score := run(t, e, &query.Query{Type: query.QueryTypeScore, Metric: metric, EntityID: entity}).Ranking.Score; score == nil || *score != -2 {
			t.Fatal("large rebuild lost absolute score")
		}
		deleted := run(t, e, &query.Query{Type: query.QueryTypeDelete, Metric: metric, UpdateLB: true, LBEntityID: entity, Where: map[string]string{tag: second}}).Delete
		if deleted.Events != 1 || deleted.LeaderboardEntries != 1 {
			t.Fatalf("large combined delete: %+v", deleted)
		}
		for i := 0; i < 2; i++ {
			run(t, e, &query.Query{Type: query.QueryTypeWrite, Metric: metric, Value: 1})
		}
		if count := run(t, e, &query.Query{Type: query.QueryTypeGet, Metric: metric, Aggregate: query.AggCount}).Aggregate.Count; count != 6 {
			t.Fatalf("large automatic clock: %d", count)
		}
	}
	peer, err := Open(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	var wg sync.WaitGroup
	failures := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			engine := e
			if i%2 == 1 {
				engine = peer
			}
			_, err := engine.Execute(&query.Query{Type: query.QueryTypeWrite, Metric: metrics[0], Timestamp: 777, Value: 1, UpdateLB: true, LBEntityID: entity, Tags: map[string]string{tag: first}})
			if err != nil {
				failures <- err
			}
		}(i)
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	if count := run(t, e, &query.Query{Type: query.QueryTypeGet, Metric: metrics[0], Aggregate: query.AggCount}).Aggregate.Count; count != 7 {
		t.Fatalf("concurrent large event duplicated: %d", count)
	}
	if score := run(t, e, &query.Query{Type: query.QueryTypeScore, Metric: metrics[0], EntityID: entity}).Ranking.Score; score == nil || *score != 16 {
		t.Fatal("concurrent large score lost increments")
	}

	t.Run("digest-collisions", func(t *testing.T) {
		collision, _ := integration(t)
		// The indexes are intentionally NON-unique. Force every digest into
		// the same bucket before inserting data; full equality must decide IDs.
		if _, err := collision.db.Exec(ctx, `CREATE OR REPLACE FUNCTION flame_identity_hash(input text) RETURNS text LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS 'SELECT repeat(''0'',32)'`); err != nil {
			t.Fatal(err)
		}
		a, b := largeID(t, 4096), largeID(t, 4096)
		x, y := largeID(t, 4096), largeID(t, 4096)
		for _, q := range []*query.Query{
			{Type: query.QueryTypeWrite, Metric: a, Timestamp: 1, Value: 3, UpdateLB: true, LBEntityID: x, Tags: map[string]string{tag: first}},
			{Type: query.QueryTypeWrite, Metric: a, Timestamp: 2, Value: 4, UpdateLB: true, LBEntityID: y, Tags: map[string]string{tag: second}},
			{Type: query.QueryTypeWrite, Metric: b, Timestamp: 1, Value: 5, UpdateLB: true, LBEntityID: x, Tags: map[string]string{tag: first}},
			{Type: query.QueryTypeWrite, Metric: a, Timestamp: 1, Value: 2, UpdateLB: true, LBEntityID: x, Tags: map[string]string{tag: first}},
		} {
			run(t, collision, q)
		}
		if names, err := collision.ListMetrics(nil); err != nil || len(names) != 2 {
			t.Fatalf("digest collision merged metrics: %d %v", len(names), err)
		}
		for _, metric := range []string{a, b} {
			if score := run(t, collision, &query.Query{Type: query.QueryTypeScore, Metric: metric, EntityID: x}).Ranking.Score; score == nil || *score != 5 {
				t.Fatal("digest collision merged entity scores")
			}
		}
		if count := run(t, collision, &query.Query{Type: query.QueryTypeGet, Metric: a, Aggregate: query.AggCount}).Aggregate.Count; count != 2 {
			t.Fatal("digest collision overwrote different timestamps")
		}
		if count := run(t, collision, &query.Query{Type: query.QueryTypeStats, Metric: a, TagKeys: []string{tag}}).Stats.TagStats[0].Cardinality; count != 2 {
			t.Fatal("digest collision merged tag values")
		}
	})
}
