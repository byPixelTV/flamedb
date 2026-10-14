package analytics

import (
	"context"
	"math"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/byPixelTV/flamedb/internal/query"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type automaticCopyTrace struct {
	allocationTrace
	direct, staged, auxiliary atomic.Int64
	arrayEvents               atomic.Int64
}

func (a *automaticCopyTrace) TraceBatchStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
	a.allocationTrace.TraceBatchStart(ctx, conn, data)
	for _, q := range data.Batch.QueuedQueries {
		if strings.HasPrefix(q.SQL, "INSERT INTO flame_events(") && strings.Contains(q.SQL, "unnest(") {
			a.arrayEvents.Add(1)
		}
	}
	return ctx
}

func (a *automaticCopyTrace) TraceCopyFromStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceCopyFromStartData) context.Context {
	if len(data.TableName) == 1 {
		switch data.TableName[0] {
		case "flame_events":
			a.direct.Add(1)
		case "flame_ingest":
			a.staged.Add(1)
		default:
			a.auxiliary.Add(1)
		}
	}
	return ctx
}
func (*automaticCopyTrace) TraceCopyFromEnd(context.Context, *pgx.Conn, pgx.TraceCopyFromEndData) {}

func TestPostgresAutomaticDirectCopy(t *testing.T) {
	e, _ := integration(t)
	ctx := context.Background()
	trace := &automaticCopyTrace{}
	config := e.db.Config()
	config.ConnConfig.Tracer = trace
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	writer := &Engine{db: pool, cache: e.cache, timeout: e.timeout, max: e.max, maxBytes: e.maxBytes, batchSize: e.batchSize}
	prefix := "auto_copy_" + token()
	names := []string{prefix + "short", prefix + strings.Repeat("wide", 700)}
	longEntity := strings.Repeat("entity", 400)
	longTag := strings.Repeat("tag", 750)
	items := make([]*query.Query, 0, 40)
	for _, name := range names {
		for range 20 {
			items = append(items, &query.Query{Type: query.QueryTypeWrite, Metric: name, Value: 1, UpdateLB: true, LBEntityID: "x", LBEntities: map[string]string{"named": longEntity}, Tags: map[string]string{"region": "eu", longTag: "null\x00"}})
		}
	}
	if rejected, err := writer.copyWrites(ctx, items); err != nil || len(rejected) != 0 {
		t.Fatalf("automatic COPY failed: %v %v", rejected, err)
	}
	if trace.direct.Load() != 1 || trace.staged.Load() != 0 || trace.auxiliary.Load() != 0 {
		t.Fatalf("automatic COPY cost: direct=%d staged=%d auxiliary=%d", trace.direct.Load(), trace.staged.Load(), trace.auxiliary.Load())
	}
	for _, name := range names {
		r := run(t, e, &query.Query{Type: query.QueryTypeGet, Metric: name, Aggregate: query.AggSum}).Aggregate
		if r.Count != 20 || r.Value != 20 {
			t.Fatalf("raw automatic COPY %q: %+v", name, r)
		}
		for board, entity := range map[string]string{"": "x", "named": longEntity} {
			score := run(t, e, &query.Query{Type: query.QueryTypeScore, Metric: name, Board: board, EntityID: entity}).Ranking.Score
			if score == nil || *score != 20 {
				t.Fatalf("automatic score %q/%q: %v", name, board, score)
			}
		}
	}
	var tags int
	if err = e.db.QueryRow(ctx, `SELECT count(*) FROM flame_tag_values WHERE metric=ANY($1::text[])`, []string{EncodeString(names[0]), EncodeString(names[1])}).Scan(&tags); err != nil || tags != 4 {
		t.Fatalf("distinct tag preservation: %d %v", tags, err)
	}
	large := make([]*query.Query, 0, 128)
	for _, name := range names {
		for range 64 {
			large = append(large, &query.Query{Type: query.QueryTypeWrite, Metric: name, Value: 1, UpdateLB: true, LBEntityID: "x", LBEntities: map[string]string{"named": longEntity}, Tags: map[string]string{"region": "eu", longTag: "null\x00"}})
		}
	}
	if failed, err := writer.copyWrites(ctx, large); err != nil || len(failed) != 0 {
		t.Fatalf("array event insertion: %v %v", failed, err)
	}
	if trace.arrayEvents.Load() != 1 || trace.direct.Load() != 1 || trace.auxiliary.Load() != 0 {
		t.Fatalf("array event cost: inserts=%d copies=%d auxiliary=%d", trace.arrayEvents.Load(), trace.direct.Load(), trace.auxiliary.Load())
	}
	for _, name := range names {
		events := run(t, e, &query.Query{Type: query.QueryTypeGet, Metric: name}).Events
		if len(events) != 84 {
			t.Fatalf("array raw count %d", len(events))
		}
		for _, event := range events {
			if event.Tags["region"] != "eu" || event.Tags[longTag] != "null\x00" {
				t.Fatalf("JSONB array tags changed: %v", event.Tags)
			}
		}
		for board, entity := range map[string]string{"": "x", "named": longEntity} {
			score := run(t, e, &query.Query{Type: query.QueryTypeScore, Metric: name, Board: board, EntityID: entity}).Ranking.Score
			if score == nil || *score != 84 {
				t.Fatalf("array score %q/%q: %v", name, board, score)
			}
		}
	}
	// Mixed explicit timestamps still overwrite existing raw events without
	// modifying previously accumulated scores.
	explicit := items[0].Timestamp
	if r, err := writer.ExecuteBatch([]*query.Query{{Type: query.QueryTypeWrite, Metric: names[0], Timestamp: explicit, Value: 9}, {Type: query.QueryTypeWrite, Metric: names[0], Value: 1}}); err != nil || r.Accepted != 2 {
		t.Fatalf("mixed COPY: %+v %v", r, err)
	}
	if trace.staged.Load() != 1 {
		t.Fatal("mixed timestamps bypassed overwrite staging")
	}
	r := run(t, e, &query.Query{Type: query.QueryTypeGet, Metric: names[0], Aggregate: query.AggSum}).Aggregate
	if r.Count != 85 || r.Value != 93 {
		t.Fatalf("explicit overwrite lost: %+v", r)
	}
	// Reused Query pointers historically represent the same final event key;
	// retain that behavior through staging instead of colliding in direct COPY.
	aliased := &query.Query{Type: query.QueryTypeWrite, Metric: prefix + "alias", Value: 1}
	if rejected, err := writer.copyWrites(ctx, []*query.Query{aliased, aliased}); err != nil || len(rejected) != 0 {
		t.Fatalf("aliased COPY: %v %v", rejected, err)
	}
	if count := run(t, e, &query.Query{Type: query.QueryTypeGet, Metric: aliased.Metric, Aggregate: query.AggCount}).Aggregate.Count; count != 1 {
		t.Fatalf("aliased event semantics changed: %d", count)
	}
	if trace.staged.Load() != 2 {
		t.Fatal("aliased timestamps bypassed overwrite staging")
	}
	run(t, e, &query.Query{Type: query.QueryTypeSet, Metric: names[0], Value: math.MaxFloat64, UpdateLB: true, LBEntityID: "x"})
	bad := &query.Query{Type: query.QueryTypeWrite, Metric: names[0], Value: math.MaxFloat64, UpdateLB: true, LBEntityID: "x", LBEntities: map[string]string{"uncreated": "y"}}
	if rejected, err := writer.copyWrites(ctx, []*query.Query{bad, {Type: query.QueryTypeWrite, Metric: names[0], Value: 1}}); err != nil || len(rejected) != 1 || rejected[0] == nil || bad.Timestamp != 0 {
		t.Fatalf("atomic automatic rejection: %v %v ts=%d", rejected, err, bad.Timestamp)
	}
	if score := run(t, e, &query.Query{Type: query.QueryTypeScore, Metric: names[0], Board: "uncreated", EntityID: "y"}).Ranking.Score; score != nil {
		t.Fatal("overflow created a companion score")
	}
	if count := run(t, e, &query.Query{Type: query.QueryTypeGet, Metric: names[0], Aggregate: query.AggCount}).Aggregate.Count; count != 86 {
		t.Fatalf("rejected event leaked: %d", count)
	}
}
