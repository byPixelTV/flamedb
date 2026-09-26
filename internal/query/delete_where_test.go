package query

import (
	"github.com/byPixelTV/flamedb/internal/aggregates"
	"github.com/byPixelTV/flamedb/internal/storage"
	"testing"
)

func TestDeleteWhereIsolationAndIndexes(t *testing.T) {
	store, err := storage.Open(t.TempDir(), "none")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	exec := NewExecutor(store, aggregates.New(store.DB()))
	run := func(line string) *Result {
		t.Helper()
		q, err := Parse(line)
		if err != nil {
			t.Fatal(line, err)
		}
		r, err := exec.Execute(q)
		if err != nil {
			t.Fatal(line, err)
		}
		return r
	}
	for _, line := range []string{
		`WRITE smp:kills 2 player:id="a" region="eu" lb="a" ts=100`,
		`WRITE smp:kills 3 player:id="a" region="us" lb="a" ts=200`,
		`WRITE smp:kills 7 player:id="b" region="eu" lb="b" ts=300`,
		`WRITE smp:kills 9 region="eu" ts=400`,
		`WRITE other 1 player:id="a" ts=100`,
	} {
		run(line)
	}

	preview := run(`DELETE smp:kills WHERE player:id="a" lb="a" DRY_RUN`).Delete
	if preview == nil || !preview.DryRun || preview.Events != 2 || preview.LeaderboardEntries != 1 {
		t.Fatal(preview)
	}
	if len(run(`GET smp:kills`).Events) != 4 || len(run(`LEADERBOARD smp:kills`).Leaderboard) != 2 {
		t.Fatal("preview mutated data")
	}
	if got := run(`DELETE smp:kills WHERE player:id="missing" DRY_RUN`).Delete; got.Events != 0 || got.LeaderboardEntries != 0 {
		t.Fatal(got)
	}
	run(`SET zero 0 lb="a"`)
	if got := run(`DELETE zero lb="a" DRY_RUN`).Delete; got.LeaderboardEntries != 1 {
		t.Fatal(got)
	}
	for _, line := range []string{`DELETE smp:kills WHERE`, `DELETE smp:kills WHERE player:id="a" AND`} {
		if _, err := Parse(line); err == nil {
			t.Fatalf("accepted malformed delete: %s", line)
		}
	}
	run(`DELETE smp:kills WHERE player:id=""`)
	if n := len(run(`GET smp:kills`).Events); n != 4 {
		t.Fatal(n)
	}
	run(`DELETE smp:kills WHERE player:id="a" AND region="eu" FROM 1970-01-01T00:00:00.000000100Z TO 1970-01-01T00:00:00.000000200Z`)
	if n := len(run(`GET smp:kills`).Events); n != 3 {
		t.Fatal(n)
	}
	if rows := run(`GET smp:kills WHERE player:id="a"`).Events; len(rows) != 1 || rows[0].Value != 3 {
		t.Fatal(rows)
	}
	// A second tag index must not expose deleted events either.
	if n := len(run(`GET smp:kills WHERE region="eu"`).Events); n != 2 {
		t.Fatal(n)
	}
	if rows := run(`LEADERBOARD smp:kills`).Leaderboard; len(rows) != 2 {
		t.Fatal(rows)
	}
	got := run(`DELETE smp:kills WHERE player:id="a" lb="a"`).Delete
	if got.DryRun || got.Events != 1 || got.LeaderboardEntries != 1 {
		t.Fatal(got)
	}
	got = run(`DELETE smp:kills WHERE player:id="a" lb="a"`).Delete
	if got.Events != 0 || got.LeaderboardEntries != 0 {
		t.Fatal(got)
	}
	if rows := run(`LEADERBOARD smp:kills`).Leaderboard; len(rows) != 1 || rows[0].EntityID != "b" || rows[0].Value != 7 {
		t.Fatal(rows)
	}
	if n := len(run(`GET smp:kills`).Events); n != 2 {
		t.Fatal(n)
	}
	if n := len(run(`GET other`).Events); n != 1 {
		t.Fatal(n)
	}
	if n := len(run(`METRICS WHERE player:id="a"`).MetricKeys); n != 1 {
		t.Fatal(n)
	}
	if rows := run(`LEADERBOARD smp:kills FROM 1970-01-01 TO now ENTITY player:id`).Leaderboard; len(rows) != 1 || rows[0].EntityID != "b" {
		t.Fatal(rows)
	}
}
