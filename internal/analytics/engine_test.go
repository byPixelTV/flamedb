package analytics

import (
	"context"
	"encoding/json"
	"math"
	"net/url"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/byPixelTV/flamedb/internal/query"
	"github.com/byPixelTV/flamedb/internal/types"
	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
)

func TestExactRankingEncoding(t *testing.T) {
	values := []float64{-math.MaxFloat64, -10, -1, -math.SmallestNonzeroFloat64, 0, math.SmallestNonzeroFloat64, 1, math.Nextafter(1, 2), float64(1 << 53), float64(1<<53) + 2, math.MaxFloat64}
	entries := []types.LeaderboardEntry{}
	members := []string{}
	for _, value := range values {
		for _, id := range []string{"a", "aa", "a\x00", "b", "玩家"} {
			entry := types.LeaderboardEntry{EntityID: id, Value: value}
			entries = append(entries, entry)
			member := encodeMember(value, id)
			members = append(members, member)
			decoded, err := decodeMember(member)
			if err != nil || decoded != entry {
				t.Fatalf("round trip %v => %v %v", entry, decoded, err)
			}
		}
	}
	sortEntries(entries, true)
	sort.Strings(members)
	for i, member := range members {
		entry, err := decodeMember(member)
		if err != nil || entry != entries[i] {
			t.Fatalf("order %d %v != %v: %v", i, entry, entries[i], err)
		}
	}
}
func integration(t *testing.T) (*Engine, Options) {
	t.Helper()
	dsn := os.Getenv("FLAMEDB_TEST_POSTGRES")
	if dsn == "" {
		if os.Getenv("FLAMEDB_INTEGRATION") == "1" {
			t.Fatal("FLAMEDB_TEST_POSTGRES is required in integration mode")
		}
		t.Skip("FLAMEDB_TEST_POSTGRES is absent")
	}
	options := Options{DSN: dsn, MaxConnections: 16, RequestTimeout: 10 * time.Second, MaxQueryEvents: 1000, Timescale: os.Getenv("FLAMEDB_TEST_TIMESCALE") == "1"}
	options.DSN = isolatedDSN(t, dsn)
	options.RedisMode = os.Getenv("FLAMEDB_TEST_REDIS_MODE")
	options.RedisOptions.MasterName = os.Getenv("FLAMEDB_TEST_REDIS_MASTER")
	if addr := os.Getenv("FLAMEDB_TEST_REDIS"); addr != "" {
		if strings.Contains(addr, "://") {
			r, err := redis.ParseURL(addr)
			if err != nil {
				t.Fatal(err)
			}
			options.RedisOptions = redis.UniversalOptions{Addrs: []string{r.Addr}, Password: r.Password, DB: r.DB}
		} else {
			options.RedisOptions.Addrs = strings.Split(addr, ",")
		}
	} else if os.Getenv("FLAMEDB_INTEGRATION") == "1" {
		t.Fatal("FLAMEDB_TEST_REDIS is required in integration mode")
	}
	e, err := Open(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	return e, options
}
func isolatedDSN(t *testing.T, dsn string) string {
	t.Helper()
	// Each case owns its SQL namespace. Repeated release runs and unrelated
	// fixtures must not change global METRICS/maintenance observations.
	admin, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	namespace := "flame_test_" + token()
	if _, err = admin.Exec(context.Background(), `CREATE SCHEMA `+pgx.Identifier{namespace}.Sanitize()); err != nil {
		admin.Close(context.Background())
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(context.Background(), `DROP SCHEMA `+pgx.Identifier{namespace}.Sanitize()+` CASCADE`); err != nil {
			t.Error(err)
		}
		admin.Close(context.Background())
	})
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			t.Fatal(err)
		}
		parameters := u.Query()
		parameters.Set("search_path", namespace+",public")
		u.RawQuery = parameters.Encode()
		return u.String()
	}
	return dsn + " search_path=" + namespace + ",public"
}
func run(t *testing.T, e *Engine, q *query.Query) *query.Result {
	t.Helper()
	r, err := e.Execute(q)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func TestPostgresSemantics(t *testing.T) {
	e, _ := integration(t)
	metric := "integration_" + token()
	run(t, e, &query.Query{Type: query.QueryTypeWrite, Metric: metric, Timestamp: 100, Value: 3, Tags: map[string]string{"player": "a", "zone": "old"}, UpdateLB: true, LBEntityID: "a"})
	run(t, e, &query.Query{Type: query.QueryTypeWrite, Metric: metric, Timestamp: 100, Value: 7, Tags: map[string]string{"player": "a", "zone": "new"}, UpdateLB: true, LBEntityID: "a"})
	run(t, e, &query.Query{Type: query.QueryTypeSet, Metric: metric, Value: -2, UpdateLB: true, LBEntityID: "b"})
	events := run(t, e, &query.Query{Type: query.QueryTypeGet, Metric: metric}).Events
	if len(events) != 1 || events[0].Value != 7 {
		t.Fatalf("timestamp overwrite/SET event: %v", events)
	}
	stale := run(t, e, &query.Query{Type: query.QueryTypeGet, Metric: metric, Where: map[string]string{"zone": "old"}})
	if len(stale.Events) != 0 {
		t.Fatal("stale tag match")
	}
	stats := run(t, e, &query.Query{Type: query.QueryTypeStats, Metric: metric, TagKeys: []string{"zone"}})
	if stats.Stats.TagStats[0].Cardinality != 2 {
		t.Fatalf("history cardinality %v", stats)
	}
	alltime := run(t, e, &query.Query{Type: query.QueryTypeLeaderboard, Metric: metric})
	if len(alltime.Leaderboard) != 2 || alltime.Leaderboard[0].Value != 10 {
		t.Fatalf("all time %v", alltime)
	}
	window := run(t, e, &query.Query{Type: query.QueryTypeLeaderboard, Metric: metric, From: 100, To: 101, EntityTag: "player"})
	if len(window.Leaderboard) != 1 || window.Leaderboard[0].Value != 7 {
		t.Fatalf("window %v", window)
	}
	dry := run(t, e, &query.Query{Type: query.QueryTypeDelete, Metric: metric, From: 100, To: 101, DryRun: true})
	if dry.Delete.Events != 1 {
		t.Fatalf("dry run %v", dry)
	}
	deleted := run(t, e, &query.Query{Type: query.QueryTypeDelete, Metric: metric, From: 100, To: 101})
	if deleted.Delete.Events != 1 {
		t.Fatalf("delete count %v", deleted)
	}
	if len(run(t, e, &query.Query{Type: query.QueryTypeGet, Metric: metric}).Events) != 0 {
		t.Fatal("raw not deleted")
	}
	if run(t, e, &query.Query{Type: query.QueryTypeLeaderboard, Metric: metric}).Leaderboard[0].Value != 10 {
		t.Fatal("raw delete altered alltime")
	}
	run(t, e, &query.Query{Type: query.QueryTypeDelete, Metric: metric, UpdateLB: true, LBEntityID: "a"})
	run(t, e, &query.Query{Type: query.QueryTypeDelete, Metric: metric, UpdateLB: true, LBEntityID: "b"})
	metrics, err := e.ListMetrics(nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range metrics {
		if m == metric {
			t.Fatal("deleted metric listed")
		}
	}
}
func TestPostgresBatchReplicaAndRejection(t *testing.T) {
	e, options := integration(t)
	m := "batch_" + token()
	items := []*query.Query{{Type: query.QueryTypeWrite, Metric: m, Timestamp: 10, Value: 1, Tags: map[string]string{"k": "first"}}, {Type: query.QueryTypeWrite, Metric: m, Timestamp: 10, Value: 2, Tags: map[string]string{"k": "last"}}, {Type: query.QueryTypeWrite, Metric: m, Value: math.NaN()}, {Type: query.QueryTypeWrite, Metric: m, Timestamp: 11, Value: 4, Quorum: true}, {Type: query.QueryTypeWrite, Metric: m, Timestamp: 12, Value: 5, UpdateLB: true, LBEntityID: "x", IsReplica: true, OperationID: token()}}
	result, err := e.ExecuteBatch(items)
	if err != nil || result.Accepted != 3 || result.Failed != 2 {
		t.Fatalf("partial batch %v %v", result, err)
	}
	run(t, e, items[4])
	score := run(t, e, &query.Query{Type: query.QueryTypeScore, Metric: m, EntityID: "x"})
	if score.Ranking.Score == nil || *score.Ranking.Score != 5 {
		t.Fatalf("replica dedupe %v", score)
	}
	// Receipts survive the API process and are shared by independent nodes.
	peer, err := Open(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	run(t, peer, items[4])
	score = run(t, peer, &query.Query{Type: query.QueryTypeScore, Metric: m, EntityID: "x"})
	if score.Ranking.Score == nil || *score.Ranking.Score != 5 {
		t.Fatalf("replica dedupe after reopen %v", score)
	}
	events := run(t, e, &query.Query{Type: query.QueryTypeGet, Metric: m}).Events
	if len(events) != 2 || events[0].Value != 2 {
		t.Fatalf("COPY overwrite %v", events)
	}
}
func TestPostgresConcurrentAndCacheRecovery(t *testing.T) {
	e, options := integration(t)
	peer, err := Open(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	m := "concurrent_" + token()
	var wg sync.WaitGroup
	failures := make(chan error, 100)
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			engine := e
			if i%2 == 1 {
				engine = peer
			}
			_, err := engine.Execute(&query.Query{Type: query.QueryTypeWrite, Metric: m, Timestamp: int64(100 + i), Value: 1, UpdateLB: true, LBEntityID: "same"})
			if err != nil {
				failures <- err
			}
		}(i)
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	rank := run(t, e, &query.Query{Type: query.QueryTypeRank, Metric: m, EntityID: "same"})
	if rank.Ranking.Score == nil || *rank.Ranking.Score != 40 {
		t.Fatalf("lost increment %v", rank)
	}
	for i, v := range []float64{math.Nextafter(float64(1<<53), math.Inf(1)), float64(1 << 53), -1, -1, 0} {
		run(t, e, &query.Query{Type: query.QueryTypeSet, Metric: m, UpdateLB: true, LBEntityID: []string{"high", "lower", "a", "b", "zero"}[i], Value: v})
	}
	baseline := run(t, e, &query.Query{Type: query.QueryTypeLeaderboard, Metric: m}).Leaderboard
	if e.cache != nil {
		if err = e.cache.Del(context.Background(), "flamedb:epoch").Err(); err != nil {
			t.Fatal(err)
		}
		if err = e.cache.Del(context.Background(), boardKey(m, "")+":z").Err(); err != nil {
			t.Fatal(err)
		}
		recovered := run(t, peer, &query.Query{Type: query.QueryTypeLeaderboard, Metric: m}).Leaderboard
		if !reflect.DeepEqual(baseline, recovered) {
			t.Fatalf("cache loss %v != %v", baseline, recovered)
		}
		if err = e.Rebuild(context.Background()); err != nil {
			t.Fatal(err)
		}
		rebuilt := run(t, e, &query.Query{Type: query.QueryTypeLeaderboard, Metric: m}).Leaderboard
		if !reflect.DeepEqual(baseline, rebuilt) {
			t.Fatalf("rebuild %v", rebuilt)
		}
	}
	around := run(t, e, &query.Query{Type: query.QueryTypeAround, Metric: m, EntityID: "same", Radius: 1})
	if around.Ranking.Rank == nil || *around.Ranking.Rank != 3 || len(around.Ranking.Around) != 3 {
		t.Fatalf("around %v", around)
	}
	missing := run(t, e, &query.Query{Type: query.QueryTypeScore, Metric: m, EntityID: "missing"})
	if missing.Ranking.Score != nil || missing.Ranking.Rank != nil {
		t.Fatal("missing should be null")
	}
	missingAround := run(t, e, &query.Query{Type: query.QueryTypeAround, Metric: m, EntityID: "missing", Radius: 1})
	encoded, err := json.Marshal(missingAround)
	if err != nil || !strings.Contains(string(encoded), `"around":[]`) {
		t.Fatalf("missing around JSON %s %v", encoded, err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = e.ExecuteContext(cancelled, &query.Query{Type: query.QueryTypeWrite, Metric: m, Value: 1}); err == nil {
		t.Fatal("cancelled write accepted")
	}
}
