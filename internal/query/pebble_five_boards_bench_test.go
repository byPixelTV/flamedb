package query

import (
	"fmt"
	"testing"

	"github.com/byPixelTV/flamedb/internal/aggregates"
	"github.com/byPixelTV/flamedb/internal/storage"
)

// Both repeated and distinct metrics are measured, with real synchronous
// commits and the user's compression. The input and iteration count are fixed
// across dev.45 and the candidate; all events and board entries are read back.
func BenchmarkPebbleFiveBoards128(b *testing.B) {
	for _, name := range []string{"repeated", "distinct"} {
		b.Run(name, func(b *testing.B) {
			store, err := storage.Open(b.TempDir(), "zstd")
			if err != nil {
				b.Fatal(err)
			}
			defer store.Close()
			lb := aggregates.New(store.DB())
			exec := NewExecutor(store, lb)
			type scoreKey struct{ metric, entity string }
			want := map[scoreKey]float64{}
			counts := map[string]int{}
			boards := []string{"", "team", "clan", "daily", "weekly"}
			queries := make([]*Query, 128)
			before := store.DB().Metrics().WAL.BytesWritten
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				for i := range queries {
					metric := fmt.Sprintf("m%d", i%5)
					if name == "distinct" {
						metric = fmt.Sprintf("m%d", (n*128+i)%250)
					}
					entity := fmt.Sprintf("player%d", n%1500)
					queries[i] = &Query{Type: QueryTypeWrite, Metric: metric, Value: 1, Timestamp: int64(n*128 + i + 1), UpdateLB: true, LBEntityID: entity, LBEntities: map[string]string{"team": entity, "clan": entity, "daily": entity, "weekly": entity}}
					want[scoreKey{metric, entity}]++
					counts[metric]++
				}
				result, err := exec.ExecuteBatch(queries)
				if err != nil || !result.OK || result.Accepted != 128 || result.Failed != 0 {
					b.Fatalf("batch %+v: %v", result, err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(store.DB().Metrics().WAL.BytesWritten-before)/float64(b.N), "WAL-bytes/batch")
			b.ReportMetric(float64(b.N*128)/b.Elapsed().Seconds(), "events/s")
			for metric, count := range counts {
				events, err := store.ReadRange(metric, 0, 1<<62)
				if err != nil || len(events) != count {
					b.Fatalf("history %s: %d != %d: %v", metric, len(events), count, err)
				}
				for _, event := range events {
					if event.Value != 1 {
						b.Fatal("changed raw event", event)
					}
				}
			}
			for key, value := range want {
				for _, board := range boards {
					got, err := lb.Get(aggregates.BoardMetric(key.metric, board), key.entity)
					if err != nil || got != value {
						b.Fatalf("score %+v board=%s: %v != %v: %v", key, board, got, value, err)
					}
				}
			}
			b.Logf("FIVE_BOARDS_READBACK_VERIFIED events=%d boards=5", b.N*128)
		})
	}
}
