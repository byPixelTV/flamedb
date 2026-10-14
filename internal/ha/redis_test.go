package ha

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/byPixelTV/flamedb/internal/analytics"
	"github.com/byPixelTV/flamedb/internal/query"
	"github.com/redis/go-redis/v9"
)

// The external fixture fences only its labelled master after this test signals
// readiness. The same clients must discover the replacement and restore cache.
func TestRedisMasterLossAndRecovery(t *testing.T) {
	if os.Getenv("FLAMEDB_REDIS_HA_INTEGRATION") != "1" {
		t.Skip("dedicated Redis HA fixture required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Second)
	defer cancel()
	mode := required(t, "FLAMEDB_TEST_REDIS_MODE")
	addresses := strings.Split(required(t, "FLAMEDB_TEST_REDIS"), ",")
	ro := redis.UniversalOptions{Addrs: addresses, MasterName: os.Getenv("FLAMEDB_TEST_REDIS_MASTER"), ContextTimeoutEnabled: true, DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second}
	var client redis.UniversalClient
	if mode == "cluster" {
		client = redis.NewClusterClient(ro.Cluster())
	} else {
		client = redis.NewUniversalClient(&ro)
	}
	defer client.Close()
	e, err := analytics.Open(ctx, analytics.Options{DSN: required(t, "FLAMEDB_TEST_POSTGRES"), Timescale: true, RedisMode: mode, RedisOptions: ro, RequestTimeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	metric := fmt.Sprintf("hafail%d", time.Now().UnixNano())
	metricHash := sha256.Sum256([]byte(metric))
	pattern := fmt.Sprintf("flamedb:{%x}*", metricHash)
	mutate := func(ts int64, value float64) error {
		_, err := e.ExecuteContext(ctx, &query.Query{Type: query.QueryTypeWrite, Metric: metric, Timestamp: ts, Value: value, UpdateLB: true, LBEntityID: "entity"})
		return err
	}
	if err = mutate(1, 3); err != nil {
		t.Fatal(err)
	}
	initial, err := e.ExecuteContext(ctx, &query.Query{Type: query.QueryTypeScore, Metric: metric, EntityID: "entity"})
	if err != nil || initial.Ranking.Score == nil || *initial.Ranking.Score != 3 {
		t.Fatalf("initial ranking: %v %v", initial, err)
	}
	// Obtain the actual cache key, hence the master holding this projection.
	var key, master string
	var ownerMu sync.Mutex
	if cluster, ok := client.(*redis.ClusterClient); ok {
		err = cluster.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
			keys, err := node.Keys(ctx, pattern).Result()
			if err != nil {
				return err
			}
			if len(keys) != 0 {
				ownerMu.Lock()
				key, master = keys[0], node.Options().Addr
				ownerMu.Unlock()
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	} else {
		sentinel := redis.NewSentinelClient(&redis.Options{Addr: addresses[0]})
		parts, err := sentinel.GetMasterAddrByName(ctx, ro.MasterName).Result()
		sentinel.Close()
		if err != nil || len(parts) != 2 {
			t.Fatalf("master: %v %v", parts, err)
		}
		master = parts[0] + ":" + parts[1]
		keys, err := client.Keys(ctx, pattern).Result()
		if err != nil || len(keys) == 0 {
			t.Fatalf("cache projection absent: %v %v", keys, err)
		}
		key = keys[0]
	}
	if key == "" || master == "" {
		t.Fatal("no real Redis projection found")
	}
	if n, err := client.Exists(ctx, key).Result(); err != nil || n != 1 {
		t.Fatalf("initial cache probe: %d %v", n, err)
	}
	if err = os.WriteFile(required(t, "FLAMEDB_HA_SIGNAL"), []byte(master), 0600); err != nil {
		t.Fatal(err)
	}
	// External fixture writes this only after abrupt master fencing completes.
	for {
		if _, err := os.Stat(required(t, "FLAMEDB_HA_FENCED")); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	// DB reads/writes remain correct during Redis discovery and promotion.
	if err = mutate(2, 2); err != nil {
		t.Fatal(err)
	}
	result, err := e.ExecuteContext(ctx, &query.Query{Type: query.QueryTypeScore, Metric: metric, EntityID: "entity"})
	if err != nil || result.Ranking.Score == nil || *result.Ranking.Score != 5 {
		t.Fatalf("fallback: %v %v", result, err)
	}
	for {
		probe, done := context.WithTimeout(ctx, time.Second)
		_, err = client.Exists(probe, key).Result()
		done()
		if err == nil {
			break
		}
		if cluster, ok := client.(*redis.ClusterClient); ok {
			cluster.ReloadState(ctx)
		}
		select {
		case <-ctx.Done():
			t.Fatal("Redis did not recover", ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
	// The engine and the independent verifier have distinct slot caches. Demand
	// reads must recover the engine's own projection before a full rebuild starts.
	for {
		result, err = e.ExecuteContext(ctx, &query.Query{Type: query.QueryTypeScore, Metric: metric, EntityID: "entity"})
		if err != nil || result.Ranking.Score == nil || *result.Ranking.Score != 5 {
			t.Fatalf("recovery read: %v %v", result, err)
		}
		probe, done := context.WithTimeout(ctx, time.Second)
		version, cacheErr := client.HGet(probe, fmt.Sprintf("flamedb:{%x}:meta", metricHash), "version").Result()
		done()
		if cacheErr == nil && version == "2" {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("engine cache did not recover", ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
	// Explicit full rebuild verifies publication on the newly discovered master.
	if err = e.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}
	if n, err := client.Exists(ctx, key).Result(); err != nil || n != 1 {
		t.Fatalf("cache not restored: %d %v", n, err)
	}
	if err = mutate(3, 4); err != nil {
		t.Fatal(err)
	}
	result, err = e.ExecuteContext(ctx, &query.Query{Type: query.QueryTypeScore, Metric: metric, EntityID: "entity"})
	if err != nil || result.Ranking.Score == nil || *result.Ranking.Score != 9 {
		t.Fatalf("reconnected projection: %v %v", result, err)
	}
	if version, err := client.HGet(ctx, fmt.Sprintf("flamedb:{%x}:meta", metricHash), "version").Result(); err != nil || version != "3" {
		t.Fatalf("engine did not project through its recovered client: version=%s %v", version, err)
	}
	t.Logf("%s abrupt master loss, PostgreSQL fallback, live client rediscovery and cache rebuild verified", mode)
}
