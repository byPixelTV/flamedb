package analytics

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/byPixelTV/flamedb/internal/query"
	"github.com/redis/go-redis/v9"
)

type projectionKeyLog struct{ count atomic.Int64 }

func (h *projectionKeyLog) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *projectionKeyLog) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *projectionKeyLog) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, command redis.Cmder) error {
		args := command.Args()
		if len(args) > 2 && ((command.Name() == "evalsha" && fmt.Sprint(args[1]) == applyProjection.Hash()) || (command.Name() == "eval" && fmt.Sprint(args[1]) == applyProjectionLua)) {
			if count, ok := args[2].(int); ok {
				h.count.Store(int64(count))
			}
		}
		return next(ctx, command)
	}
}

func TestRedisProjectionBoardCommandCost(t *testing.T) {
	e, _ := integration(t)
	ctx := context.Background()
	epoch, err := e.epoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	metric := "projection_cost_" + token()
	meta := metricKey(metric) + ":meta"
	boardNames := []string{"", "named:α", "negative"}
	if err = e.cache.HSet(ctx, meta, "epoch", epoch, "version", "0").Err(); err != nil {
		t.Fatal(err)
	}
	const perBoard = 200
	keys := []string{meta}
	args := []any{epoch, "0", "1", len(boardNames) * perBoard}
	for _, board := range boardNames {
		key := boardKey(metric, board)
		index := len(keys) + 1
		keys = append(keys, key+":z", key+":h")
		for i := 0; i < perBoard; i++ {
			entity := fmt.Sprintf("entity%03d", i)
			value := 1.0
			if board == "negative" {
				value = -1
			}
			args = append(args, index, entity, "", "1", encodeMember(value, entity))
		}
	}
	// Execute the actual source against Redis, forwarding every redis.call
	// unchanged while counting its real control flow. No server-global INFO
	// counters can be polluted by unrelated tests running in parallel.
	wrapped := `local counts={}
local function tracked(command,...)
 counts[command]=(counts[command] or 0)+1
 return redis.call(command,...)
end
local function apply()
` + strings.ReplaceAll(applyProjectionLua, "redis.call(", "tracked(") + `
end
local result=apply()
return {result,counts.ZCARD or 0,counts.HLEN or 0,counts.HSET or 0,counts.ZADD or 0}
`
	result, err := redis.NewScript(wrapped).Run(ctx, e.cache, keys, args...).Result()
	if err != nil {
		t.Fatal(err)
	}
	counts, ok := result.([]any)
	if !ok || len(counts) != 5 {
		t.Fatalf("invalid cost observation: %v", result)
	}
	want := []int64{1, 2 * int64(len(boardNames)), int64(len(boardNames)), int64(len(boardNames)*perBoard + len(boardNames) + 1), int64(len(boardNames) * perBoard)}
	for i, expected := range want {
		if counts[i] != expected {
			t.Fatalf("board command cost[%d]=%v, expected %d", i, counts[i], expected)
		}
	}
	// Use the untouched production script for existing entities, then consume
	// its actual ranking response. Both directions of legacy float tie order
	// and default/named boards must retain complete membership and values.
	args = []any{epoch, "1", "2", len(boardNames) * perBoard}
	for boardIndex, board := range boardNames {
		for i := 0; i < perBoard; i++ {
			entity := fmt.Sprintf("entity%03d", i)
			old, value := 1.0, 2.0
			if board == "negative" {
				old, value = -1, -2
			}
			args = append(args, 2+boardIndex*2, entity, encodeMember(old, entity), "1", encodeMember(value, entity))
		}
	}
	if applied, err := applyProjection.Run(ctx, e.cache, keys, args...).Int(); err != nil || applied != 1 {
		t.Fatalf("production update: %d %v", applied, err)
	}
	for _, board := range boardNames {
		response, err := e.cachedBoard(ctx, &query.Query{Type: query.QueryTypeLeaderboard, Metric: metric, Board: board, Limit: perBoard}, epoch, 2)
		if err != nil || len(response.Leaderboard) != perBoard {
			t.Fatalf("complete board %q: %+v %v", board, response, err)
		}
		for i, entry := range response.Leaderboard {
			position, value := i, 2.0
			if board == "negative" {
				position, value = perBoard-1-i, -2
			}
			if entry.EntityID != fmt.Sprintf("entity%03d", position) || entry.Value != value {
				t.Fatalf("board %q entry%d: %+v", board, i, entry)
			}
		}
	}
	log := &projectionKeyLog{}
	e.cache.AddHook(log)
	changes := make([]scoreChange, 0, len(boardNames)*perBoard)
	for _, board := range boardNames {
		for i := 0; i < perBoard; i++ {
			old, value := 2.0, 3.0
			if board == "negative" {
				old, value = -2, -3
			}
			changes = append(changes, scoreChange{board: board, entity: fmt.Sprintf("entity%03d", i), old: &old, new: &value})
		}
	}
	e.projectWithin(ctx, metric, 2, changes)
	if count := log.count.Load(); count != int64(1+2*len(boardNames)) {
		t.Fatalf("packed keys=%d, expected %d", count, 1+2*len(boardNames))
	}
	for _, board := range boardNames {
		response, err := e.cachedBoard(ctx, &query.Query{Type: query.QueryTypeLeaderboard, Metric: metric, Board: board, Limit: perBoard}, epoch, 3)
		if err != nil || len(response.Leaderboard) != perBoard {
			t.Fatalf("Go-packed board %q: %+v %v", board, response, err)
		}
		for i, entry := range response.Leaderboard {
			position, value := i, 3.0
			if board == "negative" {
				position, value = perBoard-1-i, -3
			}
			if entry.EntityID != fmt.Sprintf("entity%03d", position) || entry.Value != value {
				t.Fatalf("Go-packed board %q entry%d: %+v", board, i, entry)
			}
		}
	}
}
