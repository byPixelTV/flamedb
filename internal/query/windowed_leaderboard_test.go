package query

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/byPixelTV/flamedb/internal/aggregates"
	"github.com/byPixelTV/flamedb/internal/storage"
	"github.com/byPixelTV/flamedb/internal/types"
)

func TestClanLeaderboardLastSevenDays(t *testing.T) {
	s, err := storage.Open(t.TempDir(), "none")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	e := NewExecutor(s, aggregates.New(s.DB()))
	now := time.Now()
	for i, row := range []struct {
		clan  string
		days  int
		value int
	}{{"old", 8, 999}, {"alpha", 6, 2}, {"alpha", 1, 3}, {"beta", 2, 4}} {
		q, err := Parse(fmt.Sprintf(`WRITE craftattack:clans:joins %d clan="%s" ts=%d`, row.value, row.clan, now.Add(-time.Duration(row.days)*24*time.Hour).UnixNano()+int64(i)))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := e.Execute(q); err != nil {
			t.Fatal(err)
		}
	}
	q, err := Parse("LEADERBOARD craftattack:clans:joins FROM now-7d ENTITY clan")
	if err != nil {
		t.Fatal(err)
	}
	got, err := e.Execute(q)
	want := []types.LeaderboardEntry{{EntityID: "alpha", Value: 5}, {EntityID: "beta", Value: 4}}
	if err != nil || !reflect.DeepEqual(got.Leaderboard, want) {
		t.Fatalf("got %+v (%v), want %v", got, err, want)
	}
}
