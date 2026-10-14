package analytics

import (
	"context"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/byPixelTV/flamedb/internal/query"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type allocationTrace struct{ batches, statements, queries, unbounded atomic.Int64 }

func (a *allocationTrace) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	a.queries.Add(1)
	return ctx
}

func TestPostgresAutomaticBatchIndependentMetrics(t *testing.T) {
	e, _ := integration(t)
	ctx := context.Background()
	prefix := "raw_auto_independent_" + token()
	slow, fast := prefix+"a", prefix+"b"
	blocker, err := e.db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(ctx)
	if err = lockMetric(ctx, blocker, slow); err != nil {
		t.Fatal(err)
	}
	items := make([]*query.Query, 128)
	for i := range items {
		m := slow
		if i%2 == 1 {
			m = fast
		}
		items[i] = &query.Query{Type: query.QueryTypeWrite, Metric: m, Value: 1}
	}
	type outcome struct {
		result *query.BatchResult
		err    error
	}
	done := make(chan outcome, 1)
	deadline, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	go func() { r, err := e.ExecuteBatchContext(deadline, items); done <- outcome{r, err} }()
	observed := false
	until := time.Now().Add(time.Second)
	for time.Now().Before(until) {
		var count int
		if err = e.db.QueryRow(ctx, `SELECT count(*) FROM flame_events WHERE metric=$1`, fast).Scan(&count); err == nil && count == 64 {
			observed = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case r := <-done:
		t.Fatalf("batch returned while metric was blocked: %+v", r)
	default:
	}
	if err = blocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	r := <-done
	if !observed || r.err != nil || r.result.Accepted != 128 || r.result.Failed != 0 {
		t.Fatalf("automatic independent progress=%t result=%+v err=%v", observed, r.result, r.err)
	}
	for _, m := range []string{slow, fast} {
		if count := run(t, e, &query.Query{Type: query.QueryTypeGet, Metric: m, Aggregate: query.AggCount}).Aggregate.Count; count != 64 {
			t.Fatalf("confirmed automatic raw count %q: %d", m, count)
		}
	}
}
func (*allocationTrace) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}
func (a *allocationTrace) TraceBatchStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
	a.batches.Add(1)
	a.statements.Add(int64(data.Batch.Len()))
	for _, q := range data.Batch.QueuedQueries {
		if strings.Contains(q.SQL, "FROM flame_events") && !strings.Contains(q.SQL, "ts < t.end_ns") && !strings.Contains(q.SQL, "ts < $3") {
			a.unbounded.Add(1)
		}
	}
	return ctx
}
func (*allocationTrace) TraceBatchQuery(context.Context, *pgx.Conn, pgx.TraceBatchQueryData) {}
func (*allocationTrace) TraceBatchEnd(context.Context, *pgx.Conn, pgx.TraceBatchEndData)     {}

func TestPostgresBatchedAutomaticTimestamps(t *testing.T) {
	e, _ := integration(t)
	ctx := context.Background()
	const fixed int64 = 1_000_000_000
	e.now = func() int64 { return fixed }
	prefix := "batch_clock_" + token()
	names := []string{prefix + "a", prefix + "b", prefix + "c", prefix + "d", prefix + "\x00", prefix + strings.Repeat("x", 2100)}
	trace := &allocationTrace{}
	config := e.db.Config()
	config.ConnConfig.Tracer = trace
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	counts := map[string]int{}
	reserved := map[string]map[int64]bool{}
	for _, name := range names {
		if err = lockMetric(ctx, tx, name); err != nil {
			t.Fatal(err)
		}
		counts[name] = 8
		reserved[name] = map[int64]bool{fixed + 3: true, fixed + 6: true}
	}
	trace.batches.Store(0)
	trace.statements.Store(0)
	trace.queries.Store(0)
	allocated, err := e.allocateBatchTimestamps(ctx, tx, counts, reserved)
	if err != nil {
		t.Fatal(err)
	}
	if trace.batches.Load() != 3 || trace.statements.Load() != 6 || trace.queries.Load() != 0 || trace.unbounded.Load() != 0 {
		t.Fatalf("allocator transport phases: batches=%d statements=%d standalone queries=%d unbounded pages=%d", trace.batches.Load(), trace.statements.Load(), trace.queries.Load(), trace.unbounded.Load())
	}
	for _, name := range names {
		if len(allocated[name]) != 8 {
			t.Fatalf("missing metric allocation: %q", name)
		}
		for i, timestamp := range allocated[name] {
			if timestamp < fixed || timestamp == fixed+3 || timestamp == fixed+6 || (i > 0 && timestamp <= allocated[name][i-1]) {
				t.Fatalf("invalid allocation %q: %v", name, allocated[name])
			}
		}
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		var last int64
		if err = e.db.QueryRow(ctx, `SELECT last_ns FROM flame_metric_clock WHERE metric=$1`, EncodeString(name)).Scan(&last); err != nil || last != allocated[name][7] {
			t.Fatalf("durable clock %q: %d %v", name, last, err)
		}
	}

	// A densely occupied first page followed by a gap and another occupied
	// position must advance only across proven gaps, even with bounded windows.
	denseName := prefix + "dense"
	dense := make([]*query.Query, 1201)
	for i := range dense {
		ts := fixed + int64(i)
		if i == 1200 {
			ts++
		}
		dense[i] = &query.Query{Type: query.QueryTypeWrite, Metric: denseName, Timestamp: ts, Value: 1}
	}
	if result, err := e.ExecuteBatch(dense); err != nil || result.Accepted != 1201 {
		t.Fatalf("dense fixture: %+v %v", result, err)
	}
	auto := []*query.Query{{Type: query.QueryTypeWrite, Metric: denseName, Value: 1}, {Type: query.QueryTypeWrite, Metric: denseName, Value: 1}}
	if result, err := e.ExecuteBatch(auto); err != nil || result.Accepted != 2 {
		t.Fatalf("dense allocation: %+v %v", result, err)
	}
	if auto[0].Timestamp != fixed+1200 || auto[1].Timestamp != fixed+1202 {
		t.Fatalf("window skipped/overwrote occupied position: %d %d", auto[0].Timestamp, auto[1].Timestamp)
	}
	if count := run(t, e, &query.Query{Type: query.QueryTypeGet, Metric: denseName, Aggregate: query.AggCount}).Aggregate.Count; count != 1203 {
		t.Fatalf("dense count %d", count)
	}

	// Exhaustion of one metric must not advance any companion clock.
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	for _, name := range []string{prefix + "limit_a", prefix + "limit_z"} {
		if err = lockMetric(ctx, tx, name); err != nil {
			t.Fatal(err)
		}
	}
	e.now = func() int64 { return math.MaxInt64 - 3 }
	_, err = e.allocateBatchTimestamps(ctx, tx, map[string]int{prefix + "limit_a": 1, prefix + "limit_z": 2}, map[string]map[int64]bool{prefix + "limit_z": {math.MaxInt64 - 2: true, math.MaxInt64 - 1: true}})
	if err == nil || !strings.Contains(err.Error(), "space exhausted") {
		t.Fatalf("missing exhaustion: %v", err)
	}
	var clocks int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM flame_metric_clock WHERE metric=ANY($1::text[])`, []string{prefix + "limit_a", prefix + "limit_z"}).Scan(&clocks); err != nil || clocks != 0 {
		t.Fatalf("failed allocation advanced companion clock: %d %v", clocks, err)
	}
}
