package query

import (
	"testing"

	"github.com/byPixelTV/flamedb/internal/aggregates"
	"github.com/byPixelTV/flamedb/internal/storage"
)

func TestMultipleLeaderboardsPerWrite(t *testing.T) {
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
			t.Fatal(err)
		}
		result, err := exec.Execute(q)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	run(`WRITE deaths 1 lb="alice" lb.reason="fall" player="alice" reason="fall"`)
	run(`WRITE deaths 1 lb="bob" lb.reason="fall" player="bob" reason="fall"`)
	run(`WRITE deaths 1 lb="alice" lb.reason="lava" player="alice" reason="lava"`)
	players := run("LEADERBOARD deaths LIMIT 10").Leaderboard
	reasons := run("LEADERBOARD deaths BOARD reason LIMIT 10").Leaderboard
	if len(players) != 2 || players[0].EntityID != "alice" || players[0].Value != 2 {
		t.Fatalf("players: %+v", players)
	}
	if len(reasons) != 2 || reasons[0].EntityID != "fall" || reasons[0].Value != 2 {
		t.Fatalf("reasons: %+v", reasons)
	}
	groups := run(`GROUP_LEADERBOARD deaths BOARD reason GROUP "environment:fall,lava"`).Leaderboard
	if len(groups) != 1 || groups[0].Value != 3 {
		t.Fatalf("reason group: %+v", groups)
	}
	if events := run("GET deaths LIMIT 10").Events; len(events) != 3 {
		t.Fatalf("events: %+v", events)
	}
	data, err := store.ExportMetricData("deaths")
	if err != nil {
		t.Fatal(err)
	}
	if len(data.Leaderboard) != 8 {
		t.Fatalf("exported leaderboard keys: %d", len(data.Leaderboard))
	}
	copyStore, err := storage.Open(t.TempDir(), "none")
	if err != nil {
		t.Fatal(err)
	}
	defer copyStore.Close()
	if err := copyStore.ImportRebalanceData(data); err != nil {
		t.Fatal(err)
	}
	copyExec := NewExecutor(copyStore, aggregates.New(copyStore.DB()))
	copyQuery, _ := Parse("LEADERBOARD deaths BOARD reason LIMIT 10")
	copied, err := copyExec.Execute(copyQuery)
	if err != nil || len(copied.Leaderboard) != 2 || copied.Leaderboard[0].Value != 2 {
		t.Fatalf("rebalance copy: %+v, %v", copied, err)
	}
	run(`DELETE deaths lb.reason="fall"`)
	if reasons := run("LEADERBOARD deaths BOARD reason LIMIT 10").Leaderboard; len(reasons) != 1 || reasons[0].EntityID != "lava" {
		t.Fatalf("after delete: %+v", reasons)
	}
	run(`SET deaths 5 lb.reason="fall"`)
	if reasons := run("LEADERBOARD deaths BOARD reason LIMIT 10").Leaderboard; len(reasons) != 2 || reasons[0].EntityID != "fall" || reasons[0].Value != 5 {
		t.Fatalf("after set: %+v", reasons)
	}
}

func TestInvalidNamedLeaderboard(t *testing.T) {
	for _, line := range []string{`WRITE deaths 1 lb.="fall"`, `WRITE deaths 1 lb.reason=""`, `LEADERBOARD deaths BOARD ""`} {
		if _, err := Parse(line); err == nil {
			t.Fatalf("accepted %q", line)
		}
	}
}
