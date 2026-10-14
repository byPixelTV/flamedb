package analytics

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/byPixelTV/flamedb/internal/query"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type snapshotTrace struct {
	allocationTrace
	ready chan struct{}
	once  sync.Once
	mask  int
	locks int
	pid   uint32
}

func (h *snapshotTrace) TraceBatchStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
	h.allocationTrace.TraceBatchStart(ctx, conn, data)
	mask := 0
	locks := 0
	for _, q := range data.Batch.QueuedQueries {
		if strings.Contains(q.SQL, "pg_advisory_xact_lock(") || strings.Contains(q.SQL, "pg_advisory_xact_lock_shared(") {
			mask |= 1
			locks++
		}
		if strings.Contains(q.SQL, "FROM flame_metrics ") {
			mask |= 2
		}
		if strings.Contains(q.SQL, "FROM flame_scores ") {
			mask |= 4
		}
		if strings.Contains(q.SQL, "FROM flame_metric_clock ") {
			mask |= 8
		}
	}
	if mask&1 != 0 {
		h.once.Do(func() { h.mask = mask; h.locks = locks; h.pid = conn.PgConn().PID(); close(h.ready) })
	}
	return ctx
}

func TestPostgresIngestSnapshotAfterLock(t *testing.T) {
	e, _ := integration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	m := "snapshot_after_lock_" + token()
	run(t, e, &query.Query{Type: query.QueryTypeSet, Metric: m, Value: 10, UpdateLB: true, LBEntityID: "x"})
	run(t, e, &query.Query{Type: query.QueryTypeWrite, Metric: m, Value: 1, Timestamp: 1_000_000_000})
	blocker, err := e.db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(ctx)
	if err = lockMetric(ctx, blocker, m); err != nil {
		t.Fatal(err)
	}
	if _, err = blocker.Exec(ctx, `UPDATE flame_scores SET value=100 WHERE metric=$1;`, m); err != nil {
		t.Fatal(err)
	}
	if _, err = blocker.Exec(ctx, `UPDATE flame_metrics SET version=2 WHERE metric=$1`, m); err != nil {
		t.Fatal(err)
	}
	if _, err = blocker.Exec(ctx, `INSERT INTO flame_metric_clock(metric,last_ns) VALUES($1,2000000000)`, m); err != nil {
		t.Fatal(err)
	}
	trace := &snapshotTrace{ready: make(chan struct{})}
	config := e.db.Config()
	config.ConnConfig.Tracer = trace
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	writer := &Engine{db: pool, cache: e.cache, timeout: e.timeout, max: e.max, maxBytes: e.maxBytes, batchSize: e.batchSize, now: func() int64 { return 1_000_000_000 }}
	q := &query.Query{Type: query.QueryTypeWrite, Metric: m, Value: 2, UpdateLB: true, LBEntityID: "x"}
	shared := &query.Query{Type: query.QueryTypeWrite, Metric: m + "_shared", Value: 3, Timestamp: 9}
	encoded := &query.Query{Type: query.QueryTypeWrite, Metric: "\xff" + m, Value: 5}
	done := make(chan error, 1)
	go func() {
		failed, err := writer.copyWrites(ctx, []*query.Query{encoded, shared, q})
		if err == nil && len(failed) > 0 {
			err = failed[0]
		}
		done <- err
	}()
	select {
	case <-trace.ready:
	case <-ctx.Done():
		t.Fatal("snapshot batch did not start")
	}
	// Raw m sorts before the other names, while encoding the invalid UTF-8
	// name would move it before m. Observe the actual server wait and locks,
	// rather than inferring acquisition order from the final SQL result order.
	for {
		var waiting bool
		if err = e.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event='advisory')`, trace.pid).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("write finished before the metric lock wait: %v", err)
		case <-ctx.Done():
			t.Fatal("writer did not reach the metric lock wait")
		case <-time.After(time.Millisecond):
		}
	}
	var granted int
	if err = e.db.QueryRow(ctx, `SELECT count(*) FROM pg_locks WHERE pid=$1 AND locktype='advisory' AND granted`, trace.pid).Scan(&granted); err != nil || granted != 0 {
		t.Fatalf("later metric was locked before raw-name order: granted=%d %v", granted, err)
	}
	select {
	case err := <-done:
		t.Fatalf("write finished under external lock: %v", err)
	default:
	}
	if err = blocker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if trace.mask != 15 {
		t.Fatalf("lock/version/score/clock reads did not share one batch: mask=%d", trace.mask)
	}
	if trace.locks != 1 {
		t.Fatalf("multi-metric locks used %d statements, want one", trace.locks)
	}
	if q.Timestamp != 2_000_000_001 {
		t.Fatalf("clock snapshot preceded lock grant: %d", q.Timestamp)
	}
	var score float64
	var version int64
	if err = e.db.QueryRow(ctx, `SELECT value FROM flame_scores WHERE metric=$1 AND board='' AND entity='x'`, m).Scan(&score); err != nil || score != 102 {
		t.Fatalf("score snapshot preceded lock grant: %g %v", score, err)
	}
	if err = e.db.QueryRow(ctx, `SELECT version FROM flame_metrics WHERE metric=$1`, m).Scan(&version); err != nil || version != 3 {
		t.Fatalf("version snapshot preceded lock grant: %d %v", version, err)
	}
	if shared.Timestamp != 9 || encoded.Timestamp <= 0 {
		t.Fatalf("mixed timestamp modes changed: shared=%d encoded=%d", shared.Timestamp, encoded.Timestamp)
	}
	for _, item := range []struct {
		q     *query.Query
		value float64
	}{{shared, 3}, {encoded, 5}} {
		var count int
		var sum float64
		if err = e.db.QueryRow(ctx, `SELECT count(*),sum(value) FROM flame_events WHERE metric=$1`, EncodeString(item.q.Metric)).Scan(&count, &sum); err != nil || count != 1 || sum != item.value {
			t.Fatalf("mixed raw event %q: count=%d sum=%g: %v", item.q.Metric, count, sum, err)
		}
	}
	// Hold the sorted mixed-mode lock set and inspect the real lock modes.
	encoded.Timestamp = 0
	_, locks, err := writer.beginIngest(ctx, []*query.Query{shared, encoded, q})
	if err != nil {
		t.Fatal(err)
	}
	defer locks.Rollback(ctx)
	for _, item := range []struct{ name, mode string }{{m, "ExclusiveLock"}, {shared.Metric, "ShareLock"}, {encoded.Metric, "ExclusiveLock"}} {
		var mode string
		err = locks.QueryRow(ctx, `SELECT mode FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory' AND granted AND classid=((hashtextextended($1,0)>>32)&4294967295)::oid AND objid=(hashtextextended($1,0)&4294967295)::oid AND objsubid=1`, EncodeString(item.name)).Scan(&mode)
		if err != nil || mode != item.mode {
			t.Fatalf("metric lock mode %q: %q want %q: %v", item.name, mode, item.mode, err)
		}
	}
}
