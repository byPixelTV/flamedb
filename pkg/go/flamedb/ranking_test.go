package flamedb

import (
	"context"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRankingWireAndMissingEntities(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	c := fakeServer(t, func(line string) string {
		mu.Lock()
		seen = append(seen, line)
		mu.Unlock()
		if line == `SCORE kills "missing"` || line == `RANK kills "missing"` {
			return `{"ranking":{"rank":null,"score":null,"count":3}}`
		}
		return `{"ranking":{"rank":2,"score":0,"count":3,"around":[{"entity_id":"a","value":10,"rank":1},{"entity_id":"🌍 \"quoted\"","value":0,"rank":2},{"entity_id":"z","value":-4,"rank":3}]}}`
	})
	ctx := context.Background()
	opts := RankingOpts{LeaderboardOpts: LeaderboardOpts{Board: "reason", Limit: 7, Offset: 2}}
	rank, err := c.GetRank(ctx, "kills", "🌍 \"quoted\"", opts)
	if err != nil || rank == nil || *rank != 2 {
		t.Fatalf("rank=%v err=%v", rank, err)
	}
	score, err := c.GetScore(ctx, "kills", "zero", RankingOpts{})
	if err != nil || score == nil || *score != 0 {
		t.Fatalf("score=%v err=%v", score, err)
	}
	for _, command := range []string{"rank", "score"} {
		if command == "rank" {
			value, err := c.GetRank(ctx, "kills", "missing", RankingOpts{})
			if err != nil || value != nil {
				t.Fatalf("missing rank %v %v", value, err)
			}
		} else {
			value, err := c.GetScore(ctx, "kills", "missing", RankingOpts{})
			if err != nil || value != nil {
				t.Fatalf("missing score %v %v", value, err)
			}
		}
	}
	from := time.Date(2026, 10, 10, 1, 2, 3, 456, time.FixedZone("offset", 3600))
	window := RankingOpts{From: from, To: from.Add(time.Hour), EntityTag: "player:id"}
	around, err := c.GetAroundEntity(ctx, "kills", "zero", 1, window)
	if err != nil || len(around) != 3 || around[1].Score != 0 || around[2].Rank != 3 {
		t.Fatalf("around=%v err=%v", around, err)
	}
	count, err := c.GetLeaderboardCount(ctx, "kills", opts)
	if err != nil || count != 3 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	want := []string{`RANK kills "🌍 \"quoted\"" BOARD reason LIMIT 7 OFFSET 2`, `SCORE kills "zero"`, `RANK kills "missing"`, `SCORE kills "missing"`, `AROUND kills "zero" 1 FROM 2026-10-10T00:02:03.000000456Z TO 2026-10-10T01:02:03.000000456Z ENTITY player:id`, `LEADERBOARD_COUNT kills BOARD reason LIMIT 7 OFFSET 2`}
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(seen, want) {
		t.Fatalf("wire=%q want=%q", seen, want)
	}
}

func TestRankingValidation(t *testing.T) {
	for _, reply := range []string{`{}`, `{"ranking":null}`, `{"ranking":{"rank":null,"score":null}}`, `{"ranking":{"rank":null,"score":null,"count":null}}`, `{"ranking":{"rank":0,"score":0,"count":1}}`, `{"ranking":{"rank":2,"score":0,"count":1}}`, `{"ranking":{"rank":null,"score":0,"count":1}}`, `{"ranking":{"rank":null,"score":null,"count":-1}}`} {
		t.Run(reply, func(t *testing.T) {
			c := fakeServer(t, func(string) string { return reply })
			if _, err := c.GetRank(context.Background(), "m", "a", RankingOpts{}); err == nil {
				t.Fatal("invalid response accepted")
			}
		})
	}
	c := fakeServer(t, func(string) string { return `{"ranking":{"rank":null,"score":null,"count":0,"around":[]}}` })
	rows, err := c.GetAroundEntity(context.Background(), "m", "missing", 0, RankingOpts{})
	if err != nil || rows == nil || len(rows) != 0 {
		t.Fatalf("empty around=%v err=%v", rows, err)
	}
	for _, opts := range []RankingOpts{{From: time.Now()}, {LeaderboardOpts: LeaderboardOpts{Board: "bad:board"}}, {LeaderboardOpts: LeaderboardOpts{Limit: -1}}, {LeaderboardOpts: LeaderboardOpts{Board: "reason"}, From: time.Now(), EntityTag: "p"}} {
		if _, err := c.GetRank(context.Background(), "m", "a", opts); err == nil {
			t.Fatal("invalid options accepted")
		}
	}
	if _, err := c.GetAroundEntity(context.Background(), "m", "a", -1, RankingOpts{}); err == nil {
		t.Fatal("negative radius accepted")
	}
	if _, err := c.GetRank(context.Background(), "m 1", "a", RankingOpts{}); err == nil {
		t.Fatal("identifier injection accepted")
	}
	if _, err := c.GetScore(context.Background(), "m", "", RankingOpts{}); err == nil {
		t.Fatal("empty entity accepted")
	}
}

func TestRankingUsesReservedReadsWhileWriteIsBlocked(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	c := pooledServer(t, Config{ReadConnections: 1, WriteConnections: 1, Timeout: 2 * time.Second}, func(line string) string {
		if strings.HasPrefix(line, "WRITE ") {
			close(started)
			<-release
			return `{}`
		}
		return `{"ranking":{"rank":1,"score":0,"count":1,"around":[{"entity_id":"a","value":0,"rank":1}]}}`
	})
	go c.Write(context.Background(), "blocked", 1, WriteOpts{})
	waitCommand(t, started)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := c.GetRank(ctx, "m", "a", RankingOpts{}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetScore(ctx, "m", "a", RankingOpts{}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetAroundEntity(ctx, "m", "a", 0, RankingOpts{}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetLeaderboardCount(ctx, "m", RankingOpts{}); err != nil {
		t.Fatal(err)
	}
}

func TestRankingAroundRejectsIncompleteEntries(t *testing.T) {
	for _, entry := range []string{`{"entity_id":"a","rank":1}`, `{"entity_id":"a","value":null,"rank":1}`, `{"entity_id":"a","value":1}`, `null`} {
		t.Run(entry, func(t *testing.T) {
			c := fakeServer(t, func(string) string {
				return `{"ranking":{"rank":1,"score":1,"count":1,"around":[` + entry + `]}}`
			})
			if _, err := c.GetAroundEntity(context.Background(), "m", "a", 1, RankingOpts{}); err == nil {
				t.Fatal("incomplete around entry accepted")
			}
		})
	}
}
