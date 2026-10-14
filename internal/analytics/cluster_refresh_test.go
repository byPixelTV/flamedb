package analytics

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/byPixelTV/flamedb/internal/query"
	"github.com/redis/go-redis/v9"
)

// The real ClusterClient caches an old master whose sockets stop responding.
// Discovery subsequently advertises a different master. Short request contexts
// must trigger independent discovery, rather than trapping the old route until
// the client's periodic topology expiry. Actual node promotion is covered by HA.
func testClusterTopologyRecovery(t *testing.T) {
	seed, options := integration(t)
	for _, operation := range []string{"ranking", "projection", "batch", "rebuild"} {
		t.Run(operation, func(t *testing.T) {
			metric := "cluster_refresh_" + token()
			run(t, seed, &query.Query{Type: query.QueryTypeSet, Metric: metric, Value: 12, UpdateLB: true, LBEntityID: "x"})
			var promoted atomic.Bool
			// ClusterSlots callbacks have no origin address; go-redis rewrites
			// literal loopback IPs against that origin. Hostnames preserve these
			// two local fixture addresses verbatim in the cached routing map.
			oldMaster := strings.Replace(stalledRedisSwitch(t, "read", &promoted), "127.0.0.1", "localhost", 1)
			newMaster := strings.Replace(stalledRedis(t, "connect"), "127.0.0.1", "localhost", 1)
			var discoveries atomic.Int64
			cluster := newClusterCache(&redis.ClusterOptions{
				Addrs: []string{oldMaster}, Protocol: 2, DisableIdentity: true,
				ContextTimeoutEnabled: true, MaxRetries: -1,
				ReadOnly: true, RouteByLatency: true, RouteRandomly: true,
				ClusterSlots: func(ctx context.Context) ([]redis.ClusterSlot, error) {
					if err := ctx.Err(); err != nil {
						return nil, err
					}
					discoveries.Add(1)
					address := oldMaster
					if promoted.Load() {
						address = newMaster
					}
					return []redis.ClusterSlot{{Start: 0, End: 16383, Nodes: []redis.ClusterNode{{ID: "master", Addr: address}}}}, nil
				},
			})
			if !cluster.Options().DisableRoutingPolicies || cluster.Options().ReadOnly || cluster.Options().RouteByLatency || cluster.Options().RouteRandomly {
				t.Fatal("core cluster routing must bypass command metadata and use masters")
			}
			broken := options
			broken.RedisOptions.Addrs = nil
			broken.RequestTimeout = 600 * time.Millisecond
			engine, err := Open(context.Background(), broken)
			if err != nil {
				cluster.Close()
				t.Fatal(err)
			}
			engine.cache = cluster
			t.Cleanup(engine.Close)
			master, err := cluster.MasterForKey(context.Background(), "flamedb:epoch")
			if err != nil || master.Options().Addr != oldMaster || discoveries.Load() != 1 {
				t.Fatalf("stale topology positive control: %v %v", master, err)
			}
			// The command cache remains cold. No Redis command was sent while
			// building this slot-map positive control.
			promoted.Store(true)
			// Negative control: go-redis's own short-deadline command leaves the
			// primed slot map unchanged. Recovery below must come from Engine's
			// error handling, rather than a periodic or automatic client reload.
			faultCtx, faultCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			_, faultErr := cluster.Get(faultCtx, "flamedb:epoch").Result()
			faultCancel()
			if faultErr == nil || !errors.Is(faultCtx.Err(), context.DeadlineExceeded) {
				t.Fatalf("stale route did not exceed its short deadline: %v", faultErr)
			}
			master, err = cluster.MasterForKey(context.Background(), "flamedb:epoch")
			if err != nil || master.Options().Addr != oldMaster || discoveries.Load() != 1 {
				t.Fatalf("negative control independently refreshed topology: %v %d %v", master, discoveries.Load(), err)
			}
			switch operation {
			case "ranking":
				result, err := engine.Execute(&query.Query{Type: query.QueryTypeRank, Metric: metric, EntityID: "x"})
				if err != nil || result.Ranking.Score == nil || *result.Ranking.Score != 12 {
					t.Fatalf("short-deadline authoritative fallback: %v %v", result, err)
				}
			case "projection":
				if _, err = engine.Execute(&query.Query{Type: query.QueryTypeWrite, Metric: metric, Timestamp: 1, Value: 1, UpdateLB: true, LBEntityID: "x"}); err != nil {
					t.Fatal(err)
				}
			case "batch":
				result, err := engine.ExecuteBatch([]*query.Query{{Type: query.QueryTypeWrite, Metric: metric, Timestamp: 1, Value: 1, UpdateLB: true, LBEntityID: "x"}})
				if err != nil || result.Accepted != 1 {
					t.Fatalf("committed batch: %v %v", result, err)
				}
			case "rebuild":
				ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
				err = engine.Rebuild(ctx)
				cancel()
				if err == nil || !errors.Is(ctx.Err(), context.DeadlineExceeded) {
					t.Fatalf("explicit rebuild did not hit stale master deadline: %v", err)
				}
			}
			deadline := time.Now().Add(time.Second)
			for {
				master, err = cluster.MasterForKey(context.Background(), "flamedb:epoch")
				if err == nil && master.Options().Addr == newMaster && discoveries.Load() >= 2 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("expired request left cluster permanently stale: route=%v discoveries=%d error=%v", master, discoveries.Load(), err)
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
}
