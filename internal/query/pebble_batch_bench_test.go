package query

import (
	"fmt"
	"testing"

	"github.com/byPixelTV/flamedb/internal/aggregates"
	"github.com/byPixelTV/flamedb/internal/storage"
	dto "github.com/prometheus/client_model/go"
)

// Real synchronous WAL commits, not a mock or an admission-only measurement.
// Fixed iteration counts keep before/after input cardinality identical.
func BenchmarkPebbleLeaderboardBatch128(b *testing.B) {
	store, err := storage.Open(b.TempDir(), "none")
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	lb := aggregates.New(store.DB())
	exec := NewExecutor(store, lb)
	type key struct{ metric, entity string }
	wantScores := map[key]float64{}
	wantCounts := map[string]int{}
	queries := make([]*Query, 128)
	for i := range queries {
		queries[i] = &Query{Type: QueryTypeWrite, Value: 1, UpdateLB: true}
	}
	syncs := func() uint64 {
		var metric dto.Metric
		if err := store.DB().Metrics().LogWriter.FsyncLatency.Write(&metric); err != nil {
			b.Fatal(err)
		}
		return metric.GetHistogram().GetSampleCount()
	}
	beforeSyncs := syncs()
	beforeBytes := store.DB().Metrics().WAL.BytesWritten
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for i, q := range queries {
			seq := n*128 + i
			q.Metric = fmt.Sprintf("batch:metric:%d", seq%250)
			q.LBEntityID = fmt.Sprintf("player:%d", (seq*17+seq/250)%1500)
			q.Tags = map[string]string{"player": q.LBEntityID, "region": "eu"}
			q.Timestamp = int64(seq + 1)
			wantScores[key{q.Metric, q.LBEntityID}]++
			wantCounts[q.Metric]++
		}
		r, err := exec.ExecuteBatch(queries)
		if err != nil || !r.OK || r.Accepted != 128 || r.Failed != 0 {
			b.Fatalf("batch: %+v %v", r, err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N*128)/b.Elapsed().Seconds(), "events/s")
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "commands/s")
	b.ReportMetric(float64(syncs()-beforeSyncs)/float64(b.N), "fsyncs/batch")
	b.ReportMetric(float64(store.DB().Metrics().WAL.BytesWritten-beforeBytes)/float64(b.N), "WAL-bytes/batch")
	for metric, count := range wantCounts {
		events, err := store.ReadRange(metric, 0, 1<<62)
		if err != nil || len(events) != count {
			b.Fatalf("history %s: %d != %d: %v", metric, len(events), count, err)
		}
		for _, event := range events {
			if event.Value != 1 || event.Tags["player"] == "" || event.Tags["region"] != "eu" {
				b.Fatal("changed event", event)
			}
		}
	}
	for k, want := range wantScores {
		got, err := lb.Get(k.metric, k.entity)
		if err != nil || got != want {
			b.Fatalf("score %+v: %v != %v: %v", k, got, want, err)
		}
	}
	b.Logf("independent readback verified: events=%d metrics=%d entity-scores=%d", b.N*128, len(wantCounts), len(wantScores))
}
