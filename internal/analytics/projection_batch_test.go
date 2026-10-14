package analytics

import (
	"context"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/byPixelTV/flamedb/internal/query"
	"github.com/redis/go-redis/v9"
)

type publicationTrace struct {
	epochs, singles, pipelines, scripts atomic.Int64
	arguments                           map[string]bool
}

func (*publicationTrace) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		return next(ctx, network, address)
	}
}
func (h *publicationTrace) projection(cmd redis.Cmder) bool {
	args := cmd.Args()
	if len(args) < 2 || (cmd.Name() != "evalsha" && cmd.Name() != "eval") {
		return false
	}
	argument, _ := args[1].(string)
	return h.arguments[argument]
}
func (h *publicationTrace) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "get" && len(cmd.Args()) > 1 && cmd.Args()[1] == "flamedb:epoch" {
			h.epochs.Add(1)
		}
		if h.projection(cmd) {
			h.singles.Add(1)
			h.scripts.Add(1)
		}
		return next(ctx, cmd)
	}
}
func (h *publicationTrace) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, commands []redis.Cmder) error {
		seen := false
		for _, cmd := range commands {
			if h.projection(cmd) {
				h.scripts.Add(1)
				seen = true
			}
		}
		if seen {
			h.pipelines.Add(1)
		}
		return next(ctx, commands)
	}
}
func (h *publicationTrace) reset() {
	h.epochs.Store(0)
	h.singles.Store(0)
	h.pipelines.Store(0)
	h.scripts.Store(0)
}

func TestRedisBatchedProjectionRoundTrips(t *testing.T) {
	e, _ := integration(t)
	if e.cache == nil {
		t.Skip("Redis is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := "publication_batch_" + token()
	names := make([]string, 4)
	for i := range names {
		names[i] = prefix + strconv.Itoa(i)
		run(t, e, &query.Query{Type: query.QueryTypeSet, Metric: names[i], Value: 0, UpdateLB: true, LBEntityID: "x"})
		run(t, e, &query.Query{Type: query.QueryTypeScore, Metric: names[i], EntityID: "x"})
	}
	coldSource := applyProjectionLua + "\n-- cold " + token()
	trace := &publicationTrace{arguments: map[string]bool{applyProjection.Hash(): true, applyProjectionLua: true, redis.NewScript(coldSource).Hash(): true, coldSource: true}}
	e.cache.AddHook(trace)
	capture := func() []committedProjection {
		items := make([]*query.Query, len(names))
		for i, name := range names {
			items[i] = &query.Query{Type: query.QueryTypeWrite, Metric: name, Value: 1, UpdateLB: true, LBEntityID: "x"}
		}
		var publications []committedProjection
		failed, err := e.copyWritesWithPublisher(ctx, items, func(metric string, previous int64, changes []scoreChange) {
			publications = append(publications, committedProjection{metric, previous, changes})
		})
		if err != nil || len(failed) != 0 || len(publications) != 4 {
			t.Fatalf("durable publications: failed=%v count=%d err=%v", failed, len(publications), err)
		}
		return publications
	}
	verify := func(score float64, version string) {
		for _, name := range names {
			v, err := e.cache.HGet(ctx, metricKey(name)+":meta", "version").Result()
			if err != nil || v != version {
				t.Fatalf("publication version %q: %q %v", name, v, err)
			}
			result := run(t, e, &query.Query{Type: query.QueryTypeScore, Metric: name, EntityID: "x"}).Ranking.Score
			if result == nil || *result != score {
				t.Fatalf("publication score %q: %v", name, result)
			}
		}
	}
	first := capture()
	trace.reset()
	e.projectManyWithin(ctx, first)
	if trace.epochs.Load() != 1 || trace.singles.Load() != 0 || trace.pipelines.Load() != 1 || trace.scripts.Load() != 4 {
		t.Fatalf("warm publication cost: epochs=%d singles=%d pipelines=%d scripts=%d", trace.epochs.Load(), trace.singles.Load(), trace.pipelines.Load(), trace.scripts.Load())
	}
	verify(1, "2")
	second := capture()
	trace.reset()
	epoch, err := e.epoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	e.projectBatchWithSource(ctx, epoch, second, coldSource)
	if trace.epochs.Load() != 1 || trace.singles.Load() != 0 || trace.pipelines.Load() != 2 || trace.scripts.Load() != 8 {
		t.Fatalf("NOSCRIPT publication cost: epochs=%d singles=%d pipelines=%d scripts=%d", trace.epochs.Load(), trace.singles.Load(), trace.pipelines.Load(), trace.scripts.Load())
	}
	verify(2, "3")
	// An older complete pipeline cannot overwrite a newer committed projection.
	e.projectManyWithin(ctx, first)
	verify(2, "3")
	// A cached older epoch cannot cross a reset/rebuild boundary, even when
	// its previous SQL generation matches the replacement metadata.
	third := capture()
	for _, name := range names {
		if err := e.cache.HSet(ctx, metricKey(name)+":meta", "epoch", token()).Err(); err != nil {
			t.Fatal(err)
		}
	}
	e.projectBatchWithSource(ctx, epoch, third, applyProjectionLua)
	for _, name := range names {
		version, err := e.cache.HGet(ctx, metricKey(name)+":meta", "version").Result()
		if err != nil || version != "3" {
			t.Fatalf("old epoch crossed replacement metadata: version=%s err=%v", version, err)
		}
		member, err := e.cache.HGet(ctx, boardKey(name, "")+":h", "x").Result()
		if err != nil || member != encodeMember(2, "x") {
			t.Fatalf("old epoch mutated replacement score: %s %v", member, err)
		}
	}
}
