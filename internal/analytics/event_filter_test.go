package analytics

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/byPixelTV/flamedb/internal/query"
)

func TestIndexedHistoricalAggregates(t *testing.T) {
	e, options := integration(t)
	ctx := context.Background()
	metric := "filter_" + token()
	check := func(kind query.AggType, where map[string]string, from, to int64, want float64, count int) {
		t.Helper()
		got, err := e.aggregate(ctx, &query.Query{Aggregate: kind, Where: where, From: from, To: to}, metric)
		if err != nil || got.Count != count || math.Float64bits(got.Value) != math.Float64bits(want) {
			t.Fatalf("%s %v [%d,%d): %+v %v expected %v/%d", kind, where, from, to, got, err, want, count)
		}
	}
	values := []float64{1e16, 1, -1e16, 3}
	for i, value := range values {
		run(t, e, &query.Query{Type: query.QueryTypeWrite, Metric: metric, Timestamp: int64(i + 1), Value: value, Tags: map[string]string{"player": "x", "region": "eu"}})
	}
	run(t, e, &query.Query{Type: query.QueryTypeWrite, Metric: metric, Timestamp: 5, Value: 99, Tags: map[string]string{"player": "y", "region": "eu"}})
	for _, where := range []map[string]string{{"player": "x"}, {"player": "x", "region": "eu"}} {
		check(query.AggSum, where, 0, 0, 3, 4)
		check(query.AggAvg, where, 0, 0, .75, 4)
		check(query.AggCount, where, 0, 0, 4, 4)
		check(query.AggSum, where, 2, 4, -1e16, 2)
	}
	check(query.AggSum, map[string]string{"player": "missing"}, 0, 0, 0, 0)
	check(query.AggSum, map[string]string{"player": "x", "region": "missing"}, 0, 0, 0, 0)
	// A same-position overwrite must replace its candidate tokens, not merely add.
	run(t, e, &query.Query{Type: query.QueryTypeWrite, Metric: metric, Timestamp: 4, Value: 7, Tags: map[string]string{"player": "y", "region": "us"}})
	check(query.AggSum, map[string]string{"player": "x", "region": "eu"}, 0, 0, 0, 3)
	check(query.AggSum, map[string]string{"player": "y", "region": "us"}, 0, 0, 7, 1)
	run(t, e, &query.Query{Type: query.QueryTypeDelete, Metric: metric, Where: map[string]string{"player": "y", "region": "us"}})
	check(query.AggSum, map[string]string{"player": "y", "region": "us"}, 0, 0, 0, 0)
	// Simulate upgrading an existing raw-history table with no candidate column.
	if _, err := e.db.Exec(ctx, `ALTER TABLE flame_events DROP COLUMN tag_tokens`); err != nil {
		t.Fatal(err)
	}
	upgraded, err := Open(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(upgraded.Close)
	e = upgraded
	check(query.AggSum, map[string]string{"player": "x", "region": "eu"}, 0, 0, 0, 3)
	// Unicode, embedded NUL, oversized identities and empty tag values stay exact.
	original := metric
	metric = strings.Repeat("m", 2100) + "\x00玩家"
	key, value := strings.Repeat("key", 750)+"\x00", "用户\x00"
	run(t, e, &query.Query{Type: query.QueryTypeWrite, Metric: metric, Timestamp: 10, Value: 13, Tags: map[string]string{key: value, "empty": ""}})
	check(query.AggSum, map[string]string{key: value, "empty": ""}, 0, 0, 13, 1)
	metric = original
	// Force candidate-token collisions in this test's isolated schema. Residual
	// equality checks must reject wrong metrics and wrong second tag values.
	var candidate string
	if err = e.db.QueryRow(ctx, `SELECT md5(jsonb_build_array($1::text,'player','x')::text)`, EncodeString(metric)).Scan(&candidate); err != nil {
		t.Fatal(err)
	}
	if _, err = e.db.Exec(ctx, fmt.Sprintf(`CREATE OR REPLACE FUNCTION flame_event_tag_tokens(metric text,tags jsonb) RETURNS text[] LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$ SELECT ARRAY['%s']::text[] $$`, candidate)); err != nil {
		t.Fatal(err)
	}
	run(t, e, &query.Query{Type: query.QueryTypeWrite, Metric: metric + "other", Timestamp: 20, Value: 101, Tags: map[string]string{"player": "x", "region": "eu"}})
	run(t, e, &query.Query{Type: query.QueryTypeWrite, Metric: metric, Timestamp: 21, Value: 102, Tags: map[string]string{"player": "x", "region": "us"}})
	check(query.AggSum, map[string]string{"player": "x", "region": "eu"}, 0, 0, 0, 3)
}

func TestIndexedAggregateGenericPlan(t *testing.T) {
	e, _ := integration(t)
	ctx := context.Background()
	metric := "filter_plan_" + token()
	if _, err := e.db.Exec(ctx, `INSERT INTO flame_events(metric,ts,value,tags) SELECT $1::text,1704067200000000000+i,1,jsonb_build_object('player','player'||(i%1500),'region','eu') FROM generate_series(0,149999) g(i);`, metric); err != nil {
		t.Fatal(err)
	}
	if _, err := e.db.Exec(ctx, `INSERT INTO flame_tag_values(metric,tag,value) SELECT $1::text,'player','player'||i FROM generate_series(0,1499) g(i) UNION ALL SELECT $1::text,'region','eu'`, metric); err != nil {
		t.Fatal(err)
	}
	conn, err := e.db.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if _, err = conn.Exec(ctx, `SET plan_cache_mode=force_generic_plan`); err != nil {
		t.Fatal(err)
	}
	defer conn.Exec(ctx, `RESET plan_cache_mode`)
	for _, where := range []map[string]string{{"player": "player0"}, {"player": "player0", "region": "eu"}} {
		source, args := e.aggregateCandidates(ctx, metric, where, 0, math.MaxInt64)
		var raw []byte
		if err = conn.QueryRow(ctx, `EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON) SELECT count(*),COALESCE(sum(value ORDER BY ts),0) FROM `+source, args...).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var plan []map[string]any
		if err = json.Unmarshal(raw, &plan); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(raw), "flame_events_tag_candidates") {
			t.Fatalf("generic plan bypassed candidate index: %s", raw)
		}
		if strings.Contains(string(raw), "flame_events_ts_idx") {
			t.Fatalf("unbounded generic query scans timestamp index: %s", raw)
		}
		got, err := e.aggregate(ctx, &query.Query{Aggregate: query.AggSum, Where: where}, metric)
		if err != nil || got.Count != 100 || got.Value != 100 {
			t.Fatalf("generic filtered result: %+v %v", got, err)
		}
		t.Logf("filter=%v execution_ms=%v", where, plan[0]["Execution Time"])
	}
}

func TestIndexedAggregateReusesPlanWithoutSessionLeak(t *testing.T) {
	e, options := integration(t)
	ctx := context.Background()
	options.MaxConnections = 1
	reader, err := Open(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reader.Close)
	metric := "filter_reuse_" + token()
	run(t, e, &query.Query{Type: query.QueryTypeWrite, Metric: metric, Timestamp: 1, Value: 7, Tags: map[string]string{"player": "x"}})
	q := &query.Query{Aggregate: query.AggSum, Where: map[string]string{"player": "x"}}
	for range 12 {
		got, err := reader.aggregate(ctx, q, metric)
		if err != nil || got.Value != 7 || got.Count != 1 {
			t.Fatalf("read: %+v %v", got, err)
		}
	}
	source, _ := reader.aggregateCandidates(ctx, metric, q.Where, 0, math.MaxInt64)
	statement := `SELECT count(*),COALESCE(sum(value ORDER BY ts),0) FROM ` + source
	var generic, custom int64
	if err = reader.db.QueryRow(ctx, `SELECT generic_plans,custom_plans FROM pg_prepared_statements WHERE statement=$1`, statement).Scan(&generic, &custom); err != nil {
		t.Fatal(err)
	}
	if generic < 12 || custom != 0 {
		t.Fatalf("read still replans each value: generic=%d custom=%d", generic, custom)
	}
	assertMode := func() {
		t.Helper()
		var mode string
		if err := reader.db.QueryRow(ctx, `SHOW plan_cache_mode`).Scan(&mode); err != nil || mode != "auto" {
			t.Fatalf("session mode leaked: %s %v", mode, err)
		}
	}
	assertMode()
	for i := int64(2); i < 4; i++ {
		run(t, e, &query.Query{Type: query.QueryTypeWrite, Metric: metric, Timestamp: i, Value: math.MaxFloat64, Tags: map[string]string{"player": "overflow"}})
	}
	q.Where = map[string]string{"player": "overflow"}
	if _, err = reader.aggregate(ctx, q, metric); err == nil {
		t.Fatal("overflow unexpectedly succeeded")
	}
	assertMode()
	q.Aggregate = query.AggCount
	if got, err := reader.aggregate(ctx, q, metric); err != nil || got.Value != 2 || got.Count != 2 {
		t.Fatalf("COUNT inherited SUM failure: %+v %v", got, err)
	}
	assertMode()
}
