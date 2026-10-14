// Compile against the frozen SDK module, never the changed working-tree SDK.
package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	f "github.com/byPixelTV/flamedb/pkg/go/flamedb"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func require(ok bool, message string) {
	if !ok {
		panic(message)
	}
}
func main() {
	addr, prefix := os.Args[1], os.Args[2]
	host, port, err := net.SplitHostPort(addr)
	must(err)
	n, err := strconv.Atoi(port)
	must(err)
	db, err := f.New(f.Config{Host: host, Port: n, APIKey: "compat-key", Timeout: 10 * time.Second})
	must(err)
	defer db.Close()
	ctx := context.Background()
	metric := prefix + ".go"
	tags := map[string]string{"suite": prefix, "player": "alice"}
	must(db.Write(ctx, metric, 5, f.WriteOpts{LeaderboardEntity: "alice", LeaderboardEntities: map[string]string{"reason": "fall"}, Tags: tags, TimestampNs: 1704067200000000001}))
	br, err := db.WriteBatch(ctx, []f.WriteBatchItem{{Metric: metric, Value: 3, Opts: f.WriteOpts{LeaderboardEntity: "bob", Tags: map[string]string{"suite": prefix, "player": "bob"}, TimestampNs: 1704070800000000002}}, {Metric: metric, Value: -2, Opts: f.WriteOpts{LeaderboardEntity: "alice", Tags: tags, TimestampNs: 1704153600000000003}}})
	must(err)
	require(br.OK && br.Accepted == 2 && br.Failed == 0, "native batch result")
	get, err := db.Get(ctx, []string{metric}, f.GetOpts{Where: map[string]string{"player": "alice"}, Order: "ASC", Limit: 10})
	must(err)
	require(len(get.Events) == 2 && get.Events[0].Timestamp == 1704067200000000001 && get.Events[1].Value == -2, "exact events, timestamps and tags")
	multi, err := db.Get(ctx, []string{metric, metric + ".absent"}, f.GetOpts{})
	must(err)
	require(len(multi.Metrics[metric]) == 3, "multi metric get")
	rows, err := db.Leaderboard(ctx, metric, f.LeaderboardOpts{Limit: 10})
	must(err)
	require(len(rows) == 2 && rows[0].EntityID == "alice" && rows[0].Score == 3 && rows[1].EntityID == "bob", "tie order and increments")
	named, err := db.Leaderboard(ctx, metric, f.LeaderboardOpts{Board: "reason"})
	must(err)
	require(len(named) == 1 && named[0].EntityID == "fall" && named[0].Score == 5, "named board")
	groups, err := db.GroupLeaderboard(ctx, metric, []f.GroupDef{{Name: "team", Members: []string{"alice", "bob"}}}, f.LeaderboardOpts{})
	must(err)
	require(len(groups) == 1 && groups[0].Score == 6, "group board")
	must(db.Set(ctx, metric, 0, "alice"))
	rows, err = db.Leaderboard(ctx, metric, f.LeaderboardOpts{Offset: 1})
	must(err)
	require(len(rows) == 1 && rows[0].Score == 0, "absolute zero set and pagination")
	stats, err := db.Stats(ctx, metric, []string{"player"})
	must(err)
	require(stats.Metric == metric && len(stats.TagStats) == 1, "stats envelope")
	keys, err := db.ListMetrics(ctx, map[string]string{"suite": prefix})
	must(err)
	require(len(keys) == 1 && keys[0] == metric, "metric filtering")
	preview, err := db.DeleteWithResult(ctx, metric, f.DeleteOpts{Where: map[string]string{"player": "alice"}, LeaderboardEntity: "alice", DryRun: true})
	must(err)
	require(preview.DryRun && preview.Events == 2 && preview.LeaderboardEntries == 1, "delete preview")
	must(db.Delete(ctx, metric, f.DeleteOpts{LeaderboardEntity: "alice"}))
	rows, err = db.Leaderboard(ctx, metric, f.LeaderboardOpts{})
	must(err)
	require(len(rows) == 1 && rows[0].EntityID == "bob", "entry deletion")
	report, err := db.DeleteByTags(ctx, map[string]string{"suite": prefix}, f.DeleteOpts{})
	must(err)
	require(report.Events == 3 && len(report.Metrics) == 1, "cross metric tag deletion report")
	get, err = db.Get(ctx, []string{metric}, f.GetOpts{})
	must(err)
	require(len(get.Events) == 0, "events deleted")
	rows, err = db.Leaderboard(ctx, metric, f.LeaderboardOpts{})
	must(err)
	require(len(rows) == 1 && rows[0].Score == 3, "event deletion preserves board")
	must(db.Delete(ctx, metric, f.DeleteOpts{LeaderboardEntity: "bob"}))
	fmt.Println("FROZEN_GO_SDK_OK")
}
