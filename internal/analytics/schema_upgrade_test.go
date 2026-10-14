package analytics

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/byPixelTV/flamedb/internal/query"
	"github.com/jackc/pgx/v5"
)

func TestPostgresSchemaUpgradeAndBoundedRebuild(t *testing.T) {
	_, base := integration(t)
	ctx := context.Background()
	t.Run("schema-error-releases-pool", func(t *testing.T) {
		engine, options := integration(t)
		if _, err := engine.db.Exec(ctx, `UPDATE flame_schema_version SET version=3`); err != nil {
			t.Fatal(err)
		}
		deadline, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		opened, err := Open(deadline, options)
		if opened != nil {
			opened.Close()
		}
		if err == nil || !strings.Contains(err.Error(), "unsupported newer FlameDB schema version") {
			t.Fatalf("schema failure hidden or blocked: %v", err)
		}
	})
	for _, timescale := range []bool{false, true} {
		if timescale && !base.Timescale {
			continue
		}
		t.Run(fmt.Sprintf("upgrade-timescale-%t", timescale), func(t *testing.T) {
			options := base
			options.DSN = isolatedDSN(t, base.DSN)
			options.Timescale = timescale
			connection, err := pgx.Connect(ctx, options.DSN)
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close(ctx)
			// The previous schema had text primary keys and no row IDs/version.
			if _, err = connection.Exec(ctx, schema); err != nil {
				t.Fatal(err)
			}
			if timescale {
				if _, err = connection.Exec(ctx, `SELECT create_hypertable('flame_events','ts',chunk_time_interval=>86400000000000::bigint)`); err != nil {
					t.Fatal(err)
				}
			}
			if _, err = connection.Exec(ctx, `INSERT INTO flame_metrics(metric,version) VALUES('before_upgrade',2); INSERT INTO flame_events(metric,ts,value,tags) VALUES('before_upgrade',123,3,'{"old":"yes"}'); INSERT INTO flame_scores(metric,board,entity,value) VALUES('before_upgrade','','x',99); INSERT INTO flame_tag_values(metric,tag,value) VALUES('before_upgrade','old','yes'); INSERT INTO flame_metric_clock(metric,last_ns) VALUES('before_upgrade',1000)`); err != nil {
				t.Fatal(err)
			}
			engine, err := Open(ctx, options)
			if err != nil {
				t.Fatal(err)
			}
			defer engine.Close()
			if count := run(t, engine, &query.Query{Type: query.QueryTypeGet, Metric: "before_upgrade", Aggregate: query.AggCount}).Aggregate.Count; count != 1 {
				t.Fatal("upgrade changed events")
			}
			if score := run(t, engine, &query.Query{Type: query.QueryTypeScore, Metric: "before_upgrade", EntityID: "x"}).Ranking.Score; score == nil || *score != 99 {
				t.Fatal("upgrade changed absolute score")
			}
			if count := run(t, engine, &query.Query{Type: query.QueryTypeStats, Metric: "before_upgrade", TagKeys: []string{"old"}}).Stats.TagStats[0].Cardinality; count != 1 {
				t.Fatal("upgrade changed historical metadata")
			}
			run(t, engine, &query.Query{Type: query.QueryTypeWrite, Metric: largeID(t, 4096), Timestamp: 123, Value: 1, UpdateLB: true, LBEntityID: largeID(t, 4096)})
			// Opening again must not rewrite the data or allocate orphan sequences.
			var sequencesBefore, sequencesAfter int
			if err = connection.QueryRow(ctx, `SELECT count(*) FROM pg_class WHERE relnamespace=current_schema()::regnamespace AND relkind='S'`).Scan(&sequencesBefore); err != nil {
				t.Fatal(err)
			}
			peer, err := Open(ctx, options)
			if err != nil {
				t.Fatal(err)
			}
			peer.Close()
			if err = connection.QueryRow(ctx, `SELECT count(*) FROM pg_class WHERE relnamespace=current_schema()::regnamespace AND relkind='S'`).Scan(&sequencesAfter); err != nil || sequencesBefore != sequencesAfter {
				t.Fatalf("migration not idempotent: %d %d %v", sequencesBefore, sequencesAfter, err)
			}
		})
	}
	t.Run("maintenance-over-public-limit-single-connection", func(t *testing.T) {
		options := base
		options.DSN = isolatedDSN(t, base.DSN)
		options.MaxConnections = 1
		options.MaxQueryEvents = 1
		engine, err := Open(ctx, options)
		if err != nil {
			t.Fatal(err)
		}
		defer engine.Close()
		prefix := "bounded_rebuild_" + token()
		items := make([]*query.Query, 260)
		for i := range items {
			items[i] = &query.Query{Type: query.QueryTypeWrite, Metric: fmt.Sprintf("%s_%d", prefix, i), Timestamp: 1, Value: 3, UpdateLB: true, LBEntityID: "x"}
		}
		result, err := engine.ExecuteBatch(items)
		if err != nil || result.Accepted != 260 || result.Failed != 0 {
			t.Fatalf("seed: %+v %v", result, err)
		}
		if _, err = engine.ListMetrics(nil); err == nil {
			t.Fatal("public listing limit was bypassed")
		}
		deadline, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if err = engine.Rebuild(deadline); err != nil {
			t.Fatalf("bounded maintenance failed: %v", err)
		}
		for _, i := range []int{0, 255, 256, 259} {
			metric := fmt.Sprintf("%s_%d", prefix, i)
			member, err := engine.cache.HGet(ctx, boardKey(metric, "")+":h", "x").Result()
			entry, decodeErr := decodeMember(member)
			if err != nil || decodeErr != nil || entry.Value != 3 {
				t.Fatalf("maintenance omitted page: %d %+v %v %v", i, entry, err, decodeErr)
			}
		}
	})
}
