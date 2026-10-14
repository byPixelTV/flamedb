package query

import "testing"

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
