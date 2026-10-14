package main

import (
	"context"
	"fmt"
	f "github.com/byPixelTV/flamedb/pkg/go/flamedb"
	"net"
	"os"
	"strconv"
	"time"
)

func check(ok bool, err error) {
	if err != nil {
		panic(err)
	}
	if !ok {
		panic("ranking assertion failed")
	}
}
func main() {
	host, port, err := net.SplitHostPort(os.Args[1])
	check(true, err)
	n, err := strconv.Atoi(port)
	check(true, err)
	db, err := f.New(f.Config{Host: host, Port: n, APIKey: "compat-key", Timeout: 10 * time.Second})
	check(true, err)
	defer db.Close()
	ctx := context.Background()
	metric := os.Args[2] + ".go"
	for _, entity := range []string{"b", "a", "zero"} {
		value := 5.0
		if entity == "zero" {
			value = 0
		}
		check(true, db.Write(ctx, metric, value, f.WriteOpts{LeaderboardEntity: entity, LeaderboardEntities: map[string]string{"team": entity}, Tags: map[string]string{"player": entity}, TimestampNs: 1704067200000000001 + int64(entity[0])}))
	}
	rank, err := db.GetRank(ctx, metric, "b", f.RankingOpts{})
	check(rank != nil && *rank == 2, err)
	score, err := db.GetScore(ctx, metric, "zero", f.RankingOpts{})
	check(score != nil && *score == 0, err)
	rank, err = db.GetRank(ctx, metric, "missing", f.RankingOpts{})
	check(rank == nil, err)
	score, err = db.GetScore(ctx, metric, "missing", f.RankingOpts{})
	check(score == nil, err)
	rows, err := db.GetAroundEntity(ctx, metric, "b", 1, f.RankingOpts{})
	check(len(rows) == 3 && rows[0].EntityID == "a" && rows[0].Rank == 1 && rows[2].Rank == 3, err)
	count, err := db.GetLeaderboardCount(ctx, metric, f.RankingOpts{LeaderboardOpts: f.LeaderboardOpts{Board: "team", Limit: 1, Offset: 2}})
	check(count == 3, err)
	window := f.RankingOpts{From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), To: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), EntityTag: "player"}
	rank, err = db.GetRank(ctx, metric, "b", window)
	check(rank != nil && *rank == 2, err)
	count, err = db.GetLeaderboardCount(ctx, metric, window)
	check(count == 3, err)
	rows, err = db.GetAroundEntity(ctx, metric, "missing", 0, window)
	check(len(rows) == 0, err)
	fmt.Println("RANKING_GO_SDK_OK")
}
