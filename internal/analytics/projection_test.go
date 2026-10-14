package analytics

import (
	"context"
	"strconv"
	"testing"

	"github.com/byPixelTV/flamedb/internal/query"
)

func TestPostgresRawHistoryLeavesScoreProjectionValid(t *testing.T) {
	e, _ := integration(t)
	if e.cache == nil {
		t.Skip("FLAMEDB_TEST_REDIS absent")
	}
	m := "score_generation_" + token()
	rawOnly := "empty_generation_" + token()
	run(t, e, &query.Query{Type: query.QueryTypeSet, Metric: m, Value: 10, UpdateLB: true, LBEntityID: "x"})
	run(t, e, &query.Query{Type: query.QueryTypeRank, Metric: m, EntityID: "x"})
	run(t, e, &query.Query{Type: query.QueryTypeRank, Metric: rawOnly, EntityID: "x"})
	versions := map[string]string{}
	for _, metric := range []string{m, rawOnly} {
		value, err := e.cache.HGet(context.Background(), metricKey(metric)+":meta", "version").Result()
		if err != nil {
			t.Fatal(err)
		}
		versions[metric] = value
	}
	items := make([]*query.Query, 0, 1300)
	for i := 0; i < 1200; i++ {
		items = append(items, &query.Query{Type: query.QueryTypeWrite, Metric: m, Timestamp: int64(i + 1), Value: 1, Tags: map[string]string{"player": "window"}})
	}
	for i := 0; i < 100; i++ {
		items = append(items, &query.Query{Type: query.QueryTypeWrite, Metric: rawOnly, Timestamp: int64(i + 1), Value: 1, Tags: map[string]string{"player": "window"}})
	}
	batch, err := e.ExecuteBatch(items)
	if err != nil || batch.Accepted != 1300 || batch.Failed != 0 {
		t.Fatalf("raw batch: %v %v", batch, err)
	}
	run(t, e, &query.Query{Type: query.QueryTypeWrite, Metric: m, Timestamp: 1201, Value: 5, Tags: map[string]string{"player": "window"}})
	run(t, e, &query.Query{Type: query.QueryTypeDelete, Metric: m, From: 1, To: 601})
	run(t, e, &query.Query{Type: query.QueryTypeDelete, Metric: rawOnly, From: 1, To: 51})
	for _, metric := range []string{m, rawOnly} {
		var version int64
		if err = e.db.QueryRow(context.Background(), `SELECT version FROM flame_metrics WHERE metric=$1`, EncodeString(metric)).Scan(&version); err != nil {
			t.Fatal(err)
		}
		cachedVersion, err := e.cache.HGet(context.Background(), metricKey(metric)+":meta", "version").Result()
		if err != nil || cachedVersion != versions[metric] || cachedVersion != strconv.FormatInt(version, 10) {
			t.Fatalf("raw history invalidated scores for %s: DB%d cache%s, before%s (%v)", metric, version, cachedVersion, versions[metric], err)
		}
	}
	score := run(t, e, &query.Query{Type: query.QueryTypeScore, Metric: m, EntityID: "x"}).Ranking.Score
	if score == nil || *score != 10 {
		t.Fatalf("raw history changed absolute score: %v", score)
	}
	if count := run(t, e, &query.Query{Type: query.QueryTypeLeaderboardCount, Metric: m}).Ranking.Count; count != 1 {
		t.Fatalf("absolute count changed: %d", count)
	}
	if count := run(t, e, &query.Query{Type: query.QueryTypeLeaderboardCount, Metric: rawOnly}).Ranking.Count; count != 0 {
		t.Fatalf("raw-only board gained entities: %d", count)
	}
	if count := run(t, e, &query.Query{Type: query.QueryTypeLeaderboardCount, Metric: m, Board: "empty"}).Ranking.Count; count != 0 {
		t.Fatalf("empty board gained entities: %d", count)
	}
	for metric, want := range map[string]float64{m: 605, rawOnly: 50} {
		window := run(t, e, &query.Query{Type: query.QueryTypeLeaderboard, Metric: metric, From: 1, To: 1300, EntityTag: "player"}).Leaderboard
		if len(window) != 1 || window[0].Value != want {
			t.Fatalf("window failed to see raw history changes: %v", window)
		}
	}
	if count := run(t, e, &query.Query{Type: query.QueryTypeGet, Metric: m, Aggregate: query.AggCount}).Aggregate.Count; count != 601 {
		t.Fatalf("raw delete not reflected in history: %d", count)
	}
	run(t, e, &query.Query{Type: query.QueryTypeWrite, Metric: m, Timestamp: 1301, Value: 2, UpdateLB: true, LBEntityID: "x"})
	// Read directly: a rebuild during SCORE must not hide a missed update.
	member, err := e.cache.HGet(context.Background(), boardKey(m, "")+":h", "x").Result()
	if err != nil {
		t.Fatal(err)
	}
	entry, err := decodeMember(member)
	if err != nil || entry.Value != 12 {
		t.Fatalf("score write left stale projection: %v %v", entry, err)
	}
	version, err := e.cache.HGet(context.Background(), metricKey(m)+":meta", "version").Result()
	if err != nil || version != "2" {
		t.Fatalf("score generation did not advance: %s %v", version, err)
	}
}
