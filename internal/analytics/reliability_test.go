package analytics

import (
	"context"
	"math"
	"net"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/byPixelTV/flamedb/internal/query"
	"github.com/byPixelTV/flamedb/internal/types"
	"github.com/redis/go-redis/v9"
)

func TestPostgresLargeNativeAnalytics(t *testing.T) {
	e, _ := integration(t)
	m := "native_" + token()
	items := make([]*query.Query, 1200)
	for i := range items {
		value := 1.0
		if i%2 == 0 {
			value = 3
		}
		items[i] = &query.Query{Type: query.QueryTypeWrite, Metric: m, Timestamp: int64(i + 1), Value: value, Tags: map[string]string{"entity": "same"}}
	}
	result, err := e.ExecuteBatch(items)
	if err != nil || result.Accepted != 1200 {
		t.Fatalf("seed %v %v", result, err)
	}
	for agg, want := range map[query.AggType]float64{query.AggCount: 1200, query.AggSum: 2400, query.AggAvg: 2} {
		r := run(t, e, &query.Query{Type: query.QueryTypeGet, Metric: m, Aggregate: agg})
		if r.Aggregate.Count != 1200 || r.Aggregate.Value != want {
			t.Fatalf("native %s %v", agg, r.Aggregate)
		}
	}
	for agg, want := range map[query.AggType]float64{query.AggCount: 1200, query.AggSum: 2400, query.AggAvg: 2} {
		r := run(t, e, &query.Query{Type: query.QueryTypeGet, Metric: m, Aggregate: agg, GroupBySpec: "1s"})
		if len(r.Series) != 1 || r.Series[0].Count != 1200 || r.Series[0].Value != want {
			t.Fatalf("streamed series %v", r.Series)
		}
	}
	window := run(t, e, &query.Query{Type: query.QueryTypeLeaderboard, Metric: m, From: 1, To: 1201, EntityTag: "entity"})
	if len(window.Leaderboard) != 1 || window.Leaderboard[0].Value != 2400 {
		t.Fatalf("large window %v", window)
	}
	// Floating summation remains timestamp ordered, rather than a parallel sum.
	n := "ordered_" + token()
	ordered := []*query.Query{{Type: query.QueryTypeWrite, Metric: n, Timestamp: 3, Value: 1}, {Type: query.QueryTypeWrite, Metric: n, Timestamp: 1, Value: math.Pow(2, 54)}, {Type: query.QueryTypeWrite, Metric: n, Timestamp: 2, Value: -math.Pow(2, 54)}}
	if _, err = e.ExecuteBatch(ordered); err != nil {
		t.Fatal(err)
	}
	sum := run(t, e, &query.Query{Type: query.QueryTypeGet, Metric: n, Aggregate: query.AggSum})
	if sum.Aggregate.Value != 1 {
		t.Fatalf("summation reordered %v", sum)
	}
}

func TestRedisDelayedProjectionCannotOverwriteNewerState(t *testing.T) {
	e, _ := integration(t)
	if e.cache == nil {
		t.Skip("FLAMEDB_TEST_REDIS absent")
	}
	m := "stale_projection_" + token()
	run(t, e, &query.Query{Type: query.QueryTypeSet, Metric: m, Value: 1, UpdateLB: true, LBEntityID: "x"})
	run(t, e, &query.Query{Type: query.QueryTypeLeaderboard, Metric: m}) // Publish v1.
	run(t, e, &query.Query{Type: query.QueryTypeSet, Metric: m, Value: 2, UpdateLB: true, LBEntityID: "x"})
	run(t, e, &query.Query{Type: query.QueryTypeSet, Metric: m, Value: 3, UpdateLB: true, LBEntityID: "x"})
	old, stale := 1.0, 2.0
	e.project(context.Background(), m, 1, []scoreChange{{board: "", entity: "x", old: &old, new: &stale}})
	// Read Redis directly so an automatic SQL rebuild cannot hide the defect.
	member, err := e.cache.HGet(context.Background(), boardKey(m, "")+":h", "x").Result()
	if err != nil {
		t.Fatal(err)
	}
	entry, err := decodeMember(member)
	if err != nil || entry.Value != 3 {
		t.Fatalf("delayed projection overwrote newer value: %v %v", entry, err)
	}
	version, err := e.cache.HGet(context.Background(), metricKey(m)+":meta", "version").Result()
	if err != nil || version != strconv.FormatInt(3, 10) {
		t.Fatalf("projection regressed version: %s %v", version, err)
	}
	// Rebuilding after the final entity deletion must explicitly remove former
	// board keys as well as publish a complete empty metric snapshot.
	run(t, e, &query.Query{Type: query.QueryTypeDelete, Metric: m, UpdateLB: true, LBEntityID: "x"})
	tx, err := e.db.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if err = lockMetric(context.Background(), tx, m); err != nil {
		t.Fatal(err)
	}
	epoch, err := e.epoch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = e.rebuildMetric(context.Background(), tx, m, 4, epoch); err != nil {
		t.Fatal(err)
	}
	exists, err := e.cache.Exists(context.Background(), boardKey(m, "")+":z", boardKey(m, "")+":h").Result()
	if err != nil || exists != 0 {
		t.Fatalf("empty rebuild left obsolete boards: %d %v", exists, err)
	}
}

func TestRedisSocketDeadline(t *testing.T) {
	_, options := integration(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	connections := make(chan net.Conn, 16)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			select {
			case connections <- connection:
			case <-stop:
				connection.Close()
				return
			}
		}
	}()
	defer func() {
		close(stop)
		listener.Close()
		<-done
		for {
			select {
			case connection := <-connections:
				connection.Close()
			default:
				return
			}
		}
	}()
	options.RedisMode = ""
	options.RedisOptions = redis.UniversalOptions{Addrs: []string{listener.Addr().String()}, MaxRetries: -1}
	options.RequestTimeout = 150 * time.Millisecond
	engine, err := Open(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	m := "deadline_" + token()
	started := time.Now()
	_, err = engine.Execute(&query.Query{Type: query.QueryTypeWrite, Metric: m, Timestamp: 1, Value: 7, UpdateLB: true, LBEntityID: "x"})
	if err != nil {
		t.Fatalf("committed write failed because optional Redis stalled: %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("Redis ignored request deadline: %s", elapsed)
	}
	engine.cache.Close()
	engine.cache = nil
	score := run(t, engine, &query.Query{Type: query.QueryTypeScore, Metric: m, EntityID: "x"}).Ranking.Score
	if score == nil || *score != 7 {
		t.Fatal("acknowledged write was not durable")
	}
}
func TestPostgresLosslessIDsAndAtomicOverflow(t *testing.T) {
	e, _ := integration(t)
	m := "bytes_" + token() + "\x00\xff"
	key := "entity\x00\xff"
	ids := []string{"normal", "a\x00", "\xff", stringPrefix + "literal", "玩家"}
	want := []types.LeaderboardEntry{}
	for i, id := range ids {
		value := float64(i%2 - 1)
		run(t, e, &query.Query{Type: query.QueryTypeWrite, Metric: m, Timestamp: int64(i + 1), Value: value, Tags: map[string]string{key: id}, UpdateLB: true, LBEntityID: id, LBEntities: map[string]string{"named\x00": id}})
		want = append(want, types.LeaderboardEntry{EntityID: id, Value: value})
	}
	sortEntries(want, true)
	board := run(t, e, &query.Query{Type: query.QueryTypeLeaderboard, Metric: m}).Leaderboard
	if !reflect.DeepEqual(want, board) {
		t.Fatalf("byte ordering %v != %v", want, board)
	}
	for _, id := range ids {
		events := run(t, e, &query.Query{Type: query.QueryTypeGet, Metric: m, Where: map[string]string{key: id}}).Events
		if len(events) != 1 || events[0].Tags[key] != id {
			t.Fatalf("lossless tag %q %v", id, events)
		}
		rank := run(t, e, &query.Query{Type: query.QueryTypeRank, Metric: m, Board: "named\x00", EntityID: id})
		if rank.Ranking.Rank == nil {
			t.Fatalf("lost named entity %q", id)
		}
	}
	native := run(t, e, &query.Query{Type: query.QueryTypeLeaderboard, Metric: m, From: 1, To: 9, EntityTag: key}).Leaderboard
	windowWant := append([]types.LeaderboardEntry(nil), want...)
	sortEntries(windowWant, false)
	if !reflect.DeepEqual(native, windowWant) {
		t.Fatalf("lossless window %v != %v", native, windowWant)
	}
	saved := e.cache
	e.cache = nil
	sql := run(t, e, &query.Query{Type: query.QueryTypeLeaderboard, Metric: m}).Leaderboard
	e.cache = saved
	if !reflect.DeepEqual(sql, want) {
		t.Fatalf("SQL byte order %v", sql)
	}
	overflow := "overflow_" + token()
	run(t, e, &query.Query{Type: query.QueryTypeSet, Metric: overflow, Value: math.MaxFloat64, UpdateLB: true, LBEntityID: "a"})
	_, err := e.Execute(&query.Query{Type: query.QueryTypeWrite, Metric: overflow, Timestamp: 1, Value: math.MaxFloat64, UpdateLB: true, LBEntityID: "a"})
	if err == nil {
		t.Fatal("overflow acknowledged")
	}
	if len(run(t, e, &query.Query{Type: query.QueryTypeGet, Metric: overflow}).Events) != 0 {
		t.Fatal("failed atomic write left event")
	}
}
func TestRedisOutageFallbackAndIncompleteRebuild(t *testing.T) {
	e, _ := integration(t)
	if e.cache == nil {
		t.Skip("FLAMEDB_TEST_REDIS absent")
	}
	m := "outage_" + token()
	run(t, e, &query.Query{Type: query.QueryTypeSet, Metric: m, Value: 11, UpdateLB: true, LBEntityID: "x"})
	before := run(t, e, &query.Query{Type: query.QueryTypeLeaderboard, Metric: m}).Leaderboard
	saved := e.cache
	unavailable := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 10 * time.Millisecond, ReadTimeout: 10 * time.Millisecond, WriteTimeout: 10 * time.Millisecond, MaxRetries: -1})
	e.cache = unavailable
	t.Cleanup(func() { e.cache = saved; unavailable.Close() })
	run(t, e, &query.Query{Type: query.QueryTypeWrite, Metric: m, Timestamp: 10, Value: 2, UpdateLB: true, LBEntityID: "x"})
	score := run(t, e, &query.Query{Type: query.QueryTypeRank, Metric: m, EntityID: "x"})
	if score.Ranking.Score == nil || *score.Ranking.Score != 13 {
		t.Fatalf("outage DB fallback %v", score)
	}
	e.cache = saved
	after := run(t, e, &query.Query{Type: query.QueryTypeLeaderboard, Metric: m}).Leaderboard
	if len(after) != 1 || after[0].Value != 13 {
		t.Fatalf("recovery %v", after)
	}
	epoch, err := e.epoch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	key := boardKey(m, "")
	temp := key + ":tmp:" + token()
	published, err := publishProjection.Run(context.Background(), e.cache, []string{metricKey(m) + ":meta", temp + ":z", temp + ":h", key + ":z", key + ":h"}, epoch, "99", 1, key, 1).Int64()
	if err != nil || published != 0 {
		t.Fatalf("missing staging accepted %d %v", published, err)
	}
	preserved := run(t, e, &query.Query{Type: query.QueryTypeLeaderboard, Metric: m}).Leaderboard
	if !reflect.DeepEqual(preserved, after) || reflect.DeepEqual(preserved, before) {
		t.Fatalf("partial rebuild altered projection %v", preserved)
	}
	// Losing just the entity hash is detected independently of the epoch key.
	if err = e.cache.Del(context.Background(), key+":h").Err(); err != nil {
		t.Fatal(err)
	}
	repaired := run(t, e, &query.Query{Type: query.QueryTypeLeaderboard, Metric: m}).Leaderboard
	if !reflect.DeepEqual(repaired, after) {
		t.Fatalf("hash loss %v", repaired)
	}
}
