package analytics

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/byPixelTV/flamedb/internal/query"
)

// Both sides of the native-index boundary must preserve complete identities.
// A skipped trigger on the large side creates distinct row IDs and duplicates;
// an incorrect conflict target loses historical tags or fails writes.
func TestPostgresIdentityIndexBoundary(t *testing.T) {
	e, _ := integration(t)
	for _, size := range []int{indexTextBudget, indexTextBudget + 1} {
		for _, field := range []string{"metric", "entity", "board", "tag", "value"} {
			t.Run(fmt.Sprintf("%s-%d", field, size), func(t *testing.T) {
				metric := "boundary_" + token()
				board, entity, tag, value := "", "x", "k", "v"
				switch field {
				case "metric":
					metric = metric + strings.Repeat("m", size-len(metric))
				case "entity":
					entity = strings.Repeat("e", size-len(metric))
				case "board":
					board = strings.Repeat("b", size-len(metric)-len(entity))
				case "tag":
					tag = strings.Repeat("k", size-len(metric)-len(value))
				case "value":
					value = strings.Repeat("v", size-len(metric)-len(tag))
				}
				for _, number := range []float64{3, 5} {
					q := &query.Query{Type: query.QueryTypeWrite, Metric: metric, Timestamp: 1, Value: number, LBEntities: map[string]string{board: entity}, Tags: map[string]string{tag: value}}
					if number == 3 {
						run(t, e, q)
					} else {
						result, err := e.ExecuteBatch([]*query.Query{q, {Type: query.QueryTypeWrite, Metric: metric, Timestamp: 2, Value: 0, Tags: map[string]string{tag: value}}})
						if err != nil || result.Accepted != 2 || result.Failed != 0 {
							t.Fatalf("boundary batch: %+v %v", result, err)
						}
					}
				}
				if score := run(t, e, &query.Query{Type: query.QueryTypeScore, Metric: metric, Board: board, EntityID: entity}).Ranking.Score; score == nil || *score != 8 {
					t.Fatalf("boundary score: %v", score)
				}
				if cardinality := run(t, e, &query.Query{Type: query.QueryTypeStats, Metric: metric, TagKeys: []string{tag}}).Stats.TagStats[0].Cardinality; cardinality != 1 {
					t.Fatalf("duplicate boundary tag: %d", cardinality)
				}
				for i := 0; i < 2; i++ {
					run(t, e, &query.Query{Type: query.QueryTypeWrite, Metric: metric, Value: 1})
				}
				if count := run(t, e, &query.Query{Type: query.QueryTypeGet, Metric: metric, Aggregate: query.AggCount}).Aggregate.Count; count != 4 {
					t.Fatalf("boundary event overwrite/clock: %d", count)
				}
				for table, want := range map[string]int{"flame_metrics": 1, "flame_scores": 1, "flame_tag_values": 1, "flame_metric_clock": 1} {
					var count int
					if err := e.db.QueryRow(context.Background(), `SELECT count(*) FROM `+table+` WHERE metric=$1`, EncodeString(metric)).Scan(&count); err != nil || count != want {
						t.Fatalf("noncanonical %s: %d, %v", table, count, err)
					}
				}
			})
		}
	}
	t.Run("mixed-and-encoded-classes", func(t *testing.T) {
		metric := "mixed_boundary_" + token()
		largeMetric := metric + strings.Repeat("m", 3000)
		// 1494 bytes encode to 2001 bytes, crossing the index boundary even
		// though the original string fits. Classification must use the latter.
		encodedEntity := strings.Repeat("\x00", 1494)
		if len(encodedEntity) >= indexTextBudget || len(EncodeString(encodedEntity)) <= indexTextBudget {
			t.Fatal("encoded boundary fixture is invalid")
		}
		largeBoard := strings.Repeat("b", 3000)
		items := []*query.Query{
			{Type: query.QueryTypeWrite, Metric: metric, Timestamp: 1, Value: 1, UpdateLB: true, LBEntityID: "x", Tags: map[string]string{"k": "short"}},
			{Type: query.QueryTypeWrite, Metric: metric, Timestamp: 2, Value: 1, UpdateLB: true, LBEntityID: encodedEntity, LBEntities: map[string]string{largeBoard: "x"}, Tags: map[string]string{"k": encodedEntity, "other": "short"}},
			{Type: query.QueryTypeWrite, Metric: largeMetric, Timestamp: 1, Value: 1, UpdateLB: true, LBEntityID: "x", Tags: map[string]string{"k": "short"}},
		}
		for round := 0; round < 2; round++ {
			result, err := e.ExecuteBatch(items)
			if err != nil || result.Accepted != 3 || result.Failed != 0 {
				t.Fatalf("mixed batch: %+v %v", result, err)
			}
		}
		for _, target := range []scoreKey{{metric, "", "x"}, {metric, "", encodedEntity}, {metric, largeBoard, "x"}, {largeMetric, "", "x"}} {
			if score := run(t, e, &query.Query{Type: query.QueryTypeScore, Metric: target.metric, Board: target.board, EntityID: target.entity}).Ranking.Score; score == nil || *score != 2 {
				t.Fatalf("mixed score: %v", score)
			}
		}
		if cardinality := run(t, e, &query.Query{Type: query.QueryTypeStats, Metric: metric, TagKeys: []string{"k"}}).Stats.TagStats[0].Cardinality; cardinality != 2 {
			t.Fatalf("mixed historical tag: %d", cardinality)
		}
		if count := run(t, e, &query.Query{Type: query.QueryTypeGet, Metric: metric, Aggregate: query.AggCount}).Aggregate.Count; count != 2 {
			t.Fatalf("mixed overwrite: %d", count)
		}
		if count := run(t, e, &query.Query{Type: query.QueryTypeGet, Metric: largeMetric, Aggregate: query.AggCount}).Aggregate.Count; count != 1 {
			t.Fatalf("mixed large overwrite: %d", count)
		}
	})
}
