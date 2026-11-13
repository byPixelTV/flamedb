package query

import (
	"reflect"
	"testing"

	"github.com/byPixelTV/flamedb/internal/aggregates"
	"github.com/byPixelTV/flamedb/internal/storage"
)

func TestRankingCommandsPreserveLeaderboardOptions(t *testing.T) {
	for _, input := range []string{`RANK website:visits "alice snow" BOARD daily LIMIT 2 OFFSET 1`, `SCORE website:visits "alice snow" BOARD daily`, `AROUND website:visits "alice snow" 4 FROM 2024-01-01 TO 2024-01-03 ENTITY player`, `LEADERBOARD_COUNT website:visits BOARD daily`} {
		q, err := Parse(input)
		if err != nil {
			t.Fatalf("%s: %v", input, err)
		}
		if q.Metric != "website:visits" {
			t.Fatal(q)
		}
		if q.Type != QueryTypeLeaderboardCount && q.EntityID != "alice snow" {
			t.Fatal(q)
		}
		if q.Type == QueryTypeAround && (q.Radius != 4 || q.EntityTag != "player" || q.From == 0 || q.To == 0) {
			t.Fatal(q)
		}
	}
	for _, input := range []string{`RANK`, `RANK x`, `SCORE x ""`, `AROUND x a -1`, `AROUND x a 100001`, `AROUND x a 2 BOARD b FROM 2024-01-01`, `LEADERBOARD_COUNT x LIMIT -1`} {
		if _, err := Parse(input); err == nil {
			t.Fatalf("accepted %q", input)
		}
	}
}

func TestPebbleRankingSignedTiesAndBoundedNeighbors(t *testing.T) {
	store, err := storage.Open(t.TempDir(), "none")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	e := NewExecutor(store, aggregates.New(store.DB()))
	run := func(line string) *Result {
		t.Helper()
		q, err := Parse(line)
		if err != nil {
			t.Fatal(err)
		}
		out, err := e.Execute(q)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	for _, line := range []string{`SET m 5 lb.daily="a"`, `SET m 5 lb.daily="b"`, `SET m 0 lb.daily="zero"`, `SET m -2 lb.daily="left"`, `SET m -2 lb.daily="right"`, `SET m -4 lb.daily="last"`, `SET m 9 lb="other-board"`} {
		run(line)
	}
	got := run(`AROUND m "right" 2 BOARD daily LIMIT 1 OFFSET 5`).Ranking
	want := []RankedEntry{{"b", 5, 2}, {"zero", 0, 3}, {"right", -2, 4}, {"left", -2, 5}, {"last", -4, 6}}
	if got.Count != 6 || got.Rank == nil || *got.Rank != 4 || got.Score == nil || *got.Score != -2 || !reflect.DeepEqual(got.Around, want) {
		t.Fatalf("wrong neighbors: %+v", got)
	}
	missing := run(`AROUND m "unknown" 2 BOARD daily`).Ranking
	if missing.Count != 6 || missing.Rank != nil || missing.Score != nil || missing.Around == nil || len(missing.Around) != 0 {
		t.Fatalf("wrong absent result: %+v", missing)
	}
	if got := run(`LEADERBOARD_COUNT m BOARD daily LIMIT 1 OFFSET 5`).Ranking; got.Count != 6 || got.Rank != nil || got.Score != nil {
		t.Fatalf("wrong count: %+v", got)
	}
}
