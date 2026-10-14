package analytics

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/byPixelTV/flamedb/internal/query"
	"github.com/redis/go-redis/v9"
)

type rankCommandLog struct {
	mu    sync.Mutex
	names []string
}

func (h *rankCommandLog) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) { return next(ctx, network, addr) }
}
func (h *rankCommandLog) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, command redis.Cmder) error {
		h.mu.Lock()
		h.names = append(h.names, command.Name())
		h.mu.Unlock()
		return next(ctx, command)
	}

}
func (h *rankCommandLog) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *rankCommandLog) take() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	names := h.names
	h.names = nil
	return names
}

func TestRedisRankingAtomicVersionAndRoundTrips(t *testing.T) {
	e, _ := integration(t)
	ctx := context.Background()
	m := "rank_roundtrips_" + token()
	run(t, e, &query.Query{Type: query.QueryTypeSet, Metric: m, Value: 11, UpdateLB: true, LBEntityID: "x"})
	run(t, e, &query.Query{Type: query.QueryTypeRank, Metric: m, EntityID: "x"})
	hook := &rankCommandLog{}
	e.cache.AddHook(hook)
	for _, kind := range []query.QueryType{query.QueryTypeRank, query.QueryTypeScore, query.QueryTypeAround, query.QueryTypeLeaderboardCount, query.QueryTypeLeaderboard} {
		run(t, e, &query.Query{Type: kind, Metric: m, EntityID: "x", Radius: 1, Limit: 2})
		names := hook.take()
		if len(names) != 2 || names[0] != "get" || names[1] != "evalsha" {
			t.Fatalf("warm %s performs redundant Redis operations: %v", kind, names)
		}
	}
	key, meta := boardKey(m, ""), metricKey(m)+":meta"
	for _, fault := range []string{"version", "epoch", "sorted-set", "hash"} {
		t.Run(fault, func(t *testing.T) {
			var err error
			switch fault {
			case "version":
				// A self-consistent but newer Redis generation must never be
				// trusted in place of PostgreSQL's authoritative version.
				_, err = e.cache.TxPipelined(ctx, func(p redis.Pipeliner) error {
					p.Del(ctx, key+":z")
					p.ZAdd(ctx, key+":z", redis.Z{Score: 0, Member: encodeMember(999, "x")})
					p.HSet(ctx, key+":h", "x", encodeMember(999, "x"))
					p.HSet(ctx, meta, "version", "999")
					return nil
				})
			case "epoch":
				err = e.cache.HSet(ctx, meta, "epoch", "expired-epoch").Err()
			case "sorted-set":
				err = e.cache.Del(ctx, key+":z").Err()
			case "hash":
				err = e.cache.Del(ctx, key+":h").Err()
			}
			if err != nil {
				t.Fatal(err)
			}
			epoch, err := e.epoch(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = e.cachedBoard(ctx, &query.Query{Type: query.QueryTypeRank, Metric: m, EntityID: "x"}, epoch, 1); !errors.Is(err, errStaleProjection) {
				t.Fatalf("invalid projection accepted: %v", err)
			}
			result := run(t, e, &query.Query{Type: query.QueryTypeRank, Metric: m, EntityID: "x"}).Ranking
			if result.Score == nil || *result.Score != 11 || result.Rank == nil || *result.Rank != 1 || result.Count != 1 {
				t.Fatalf("incorrect recovery: %+v", result)
			}
			member, err := e.cache.HGet(ctx, key+":h", "x").Result()
			entry, decodeErr := decodeMember(member)
			if err != nil || decodeErr != nil || entry.Value != 11 {
				t.Fatalf("rebuild was hidden by SQL fallback: %v %v %v", entry, err, decodeErr)
			}
		})
	}
	t.Run("version-after-lock", func(t *testing.T) {
		writer, err := e.db.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer writer.Rollback(ctx)
		if err = lockMetric(ctx, writer, m); err != nil {
			t.Fatal(err)
		}
		if _, err = writer.Exec(ctx, `UPDATE flame_metrics SET version=2 WHERE metric=$1`, EncodeString(m)); err != nil {
			t.Fatal(err)
		}
		reader, err := e.db.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Rollback(ctx)
		pid := reader.Conn().PgConn().PID()
		type outcome struct {
			version int64
			err     error
		}
		done := make(chan outcome, 1)
		go func() { v, err := lockProjectionVersion(ctx, reader, m, true); done <- outcome{v, err} }()
		waiting := false
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if err = writer.QueryRow(ctx, `SELECT coalesce(wait_event,'')='advisory' FROM pg_stat_activity WHERE pid=$1`, pid).Scan(&waiting); err != nil {
				t.Fatal(err)
			}
			if waiting {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		// Release even on a failed observation so the worker can always finish.
		if err = writer.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		got := <-done
		if !waiting || got.err != nil || got.version != 2 {
			t.Fatalf("version snapshot taken before lock: waiting=%t version=%d error=%v", waiting, got.version, got.err)
		}
	})
	t.Run("warm-snapshot-during-write", func(t *testing.T) {
		metric := "warm_snapshot_" + token()
		q := &query.Query{Type: query.QueryTypeRank, Metric: metric, EntityID: "x"}
		run(t, e, &query.Query{Type: query.QueryTypeSet, Metric: metric, Value: 11, UpdateLB: true, LBEntityID: "x"})
		run(t, e, q) // Prime the complete durable generation 1.
		writer, err := e.db.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer writer.Rollback(ctx)
		if err = lockMetric(ctx, writer, metric); err != nil {
			t.Fatal(err)
		}
		if _, err = writer.Exec(ctx, `UPDATE flame_scores SET value=22 WHERE metric=$1`, EncodeString(metric)); err != nil {
			t.Fatal(err)
		}
		if _, err = writer.Exec(ctx, `UPDATE flame_metrics SET version=2 WHERE metric=$1`, EncodeString(metric)); err != nil {
			t.Fatal(err)
		}
		// The uncommitted writer holds the exclusive metric lock. A warm read
		// must complete with the prior coherent snapshot rather than block.
		readCtx, cancel := context.WithTimeout(ctx, 600*time.Millisecond)
		before, err := e.ExecuteContext(readCtx, q)
		cancel()
		if err != nil || before.Ranking.Score == nil || *before.Ranking.Score != 11 {
			t.Fatalf("warm snapshot blocked or exposed uncommitted data: %+v %v", before, err)
		}
		if err = writer.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		// SQL is now at version 2 while Redis is still at version 1. A read
		// beginning after that durable commit must rebuild and return 22.
		after := run(t, e, q).Ranking
		if after.Score == nil || *after.Score != 22 {
			t.Fatalf("post-commit read returned stale cache: %+v", after)
		}
		epoch, err := e.epoch(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = e.cachedBoard(ctx, q, epoch, 1); !errors.Is(err, errStaleProjection) {
			t.Fatalf("Redis advanced after the initial SQL snapshot, but an old version was accepted: %v", err)
		}
	})
}
