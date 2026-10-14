package analytics

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"

	"github.com/byPixelTV/flamedb/internal/query"
)

func TestPostgresNativeBoardBatch(t *testing.T) {
	e, options := integration(t)
	m := "copy_boards_" + token()
	run(t, e, &query.Query{Type: query.QueryTypeSet, Metric: m, Value: math.MaxFloat64, LBEntities: map[string]string{"overflow": "x"}})
	items := []*query.Query{
		{Type: query.QueryTypeWrite, Metric: m, Timestamp: 1, Value: 2, UpdateLB: true, LBEntityID: "x", LBEntities: map[string]string{"named": "x"}, Tags: map[string]string{"v": "old"}},
		{Type: query.QueryTypeWrite, Metric: m, Timestamp: 2, Value: math.MaxFloat64, UpdateLB: true, LBEntityID: "x", LBEntities: map[string]string{"overflow": "x"}},
		{Type: query.QueryTypeWrite, Metric: m, Timestamp: 1, Value: 3, UpdateLB: true, LBEntityID: "x", LBEntities: map[string]string{"named": "x"}, Tags: map[string]string{"v": "new"}},
	}
	batch, err := e.ExecuteBatch(items)
	if err != nil || batch.Accepted != 2 || batch.Failed != 1 || len(batch.Errors) != 1 || batch.Errors[0].Index != 1 {
		t.Fatalf("atomic COPY item rejection: %v %v", batch, err)
	}
	for _, board := range []string{"", "named"} {
		score := run(t, e, &query.Query{Type: query.QueryTypeScore, Metric: m, Board: board, EntityID: "x"}).Ranking.Score
		if score == nil || *score != 5 {
			t.Fatalf("staged board %q: %v", board, score)
		}
	}
	events := run(t, e, &query.Query{Type: query.QueryTypeGet, Metric: m}).Events
	if len(events) != 1 || events[0].Value != 3 || events[0].Tags["v"] != "new" {
		t.Fatalf("COPY timestamp overwrite or atomic rejection: %v", events)
	}
	peer, err := Open(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	// Each whole COPY batch, SET and DELETE must serialize through the shared
	// metric lock. These are the only possible scores for their six orderings.
	for round := 0; round < 6; round++ {
		metric := "copy_race_" + token()
		writes := make([]*query.Query, 50)
		for i := range writes {
			writes[i] = &query.Query{Type: query.QueryTypeWrite, Metric: metric, Timestamp: int64(i + 1), Value: 1, UpdateLB: true, LBEntityID: "x", LBEntities: map[string]string{"stable": "x"}}
		}
		start := make(chan struct{})
		failures := make(chan error, 3)
		var wg sync.WaitGroup
		operations := []func() error{
			func() error {
				r, err := e.ExecuteBatch(writes)
				if err == nil && (r.Accepted != 50 || r.Failed != 0) {
					return fmt.Errorf("batch rejected: %v", r)
				}
				return err
			},
			func() error {
				_, err := peer.Execute(&query.Query{Type: query.QueryTypeSet, Metric: metric, Value: 100, UpdateLB: true, LBEntityID: "x"})
				return err
			},
			func() error {
				_, err := peer.Execute(&query.Query{Type: query.QueryTypeDelete, Metric: metric, UpdateLB: true, LBEntityID: "x"})
				return err
			},
		}
		for _, operation := range operations {
			wg.Add(1)
			go func(operation func() error) {
				defer wg.Done()
				<-start
				if err := operation(); err != nil {
					failures <- err
				}
			}(operation)
		}
		close(start)
		wg.Wait()
		close(failures)
		for err := range failures {
			t.Fatal(err)
		}
		score := run(t, e, &query.Query{Type: query.QueryTypeScore, Metric: metric, EntityID: "x"}).Ranking.Score
		if score != nil && *score != 50 && *score != 100 && *score != 150 {
			t.Fatalf("non-serial COPY/SET/DELETE score: %v", *score)
		}
		stable := run(t, e, &query.Query{Type: query.QueryTypeScore, Metric: metric, Board: "stable", EntityID: "x"}).Ranking.Score
		if stable == nil || *stable != 50 {
			t.Fatalf("lost named increments: %v", stable)
		}
		if len(run(t, e, &query.Query{Type: query.QueryTypeGet, Metric: metric}).Events) != 50 {
			t.Fatal("COPY events lost during board SET/DELETE")
		}
	}
}

func TestPostgresResultByteBudget(t *testing.T) {
	e, _ := integration(t)
	m := "budget_" + token()
	large := strings.Repeat("x", 4096)
	run(t, e, &query.Query{Type: query.QueryTypeWrite, Metric: m, Timestamp: 1, Value: 1, Tags: map[string]string{"entity": large}, UpdateLB: true, LBEntityID: large})
	e.maxBytes = 1024
	queries := []*query.Query{
		{Type: query.QueryTypeGet, Metric: m},
		{Type: query.QueryTypeLeaderboard, Metric: m},
		{Type: query.QueryTypeAround, Metric: m, EntityID: large, Radius: 1},
		{Type: query.QueryTypeLeaderboard, Metric: m, From: 1, To: 2, EntityTag: "entity"},
		{Type: query.QueryTypeGroupLeaderboard, Metric: m, Groups: []query.GroupDef{{Name: large, Members: []string{large}}}},
		{Type: query.QueryTypeStats, Metric: m, TagKeys: []string{large}},
		{Type: query.QueryTypeGet, Metrics: []string{large, large + "2"}, Aggregate: query.AggCount},
	}
	for _, q := range queries {
		if _, err := e.Execute(q); err == nil {
			t.Fatalf("%s returned oversized result", q.Type)
		}
	}
	cache := e.cache
	e.cache = nil
	defer func() { e.cache = cache }()
	if _, err := e.Execute(&query.Query{Type: query.QueryTypeLeaderboard, Metric: m}); err == nil {
		t.Fatal("SQL fallback returned oversized board")
	}
	if _, err := e.ExecuteContext(context.Background(), &query.Query{Type: query.QueryTypeGet, Metric: m, Aggregate: query.AggCount}); err != nil {
		t.Fatalf("small aggregate incorrectly rejected: %v", err)
	}
}
