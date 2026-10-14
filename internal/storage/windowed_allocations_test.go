//go:build !race

package storage

// Race instrumentation adds allocations absent from production binaries.
// The release gate runs this unchanged allocation threshold without -race,
// while all semantic/concurrency tests remain part of the race run.
import (
	"github.com/byPixelTV/flamedb/internal/types"
	"math"
	"reflect"
	"testing"
)

func TestWindowedAllocations(t *testing.T) {
	s := windowTestStore(t)
	seedWindowBenchmark(t, s, 10000, 100)
	measure := func(fn func(string, string, int64, int64, int, int) ([]types.LeaderboardEntry, error)) float64 {
		return testing.AllocsPerRun(3, func() {
			got, err := fn("craftattack:clans:joins", "clan", 1, math.MaxInt64, 10, 0)
			if err != nil || len(got) != 10 {
				t.Fatalf("%v %v", got, err)
			}
		})
	}
	old, current := measure(s.legacyWindowedLeaderboard), measure(s.WindowedLeaderboard)
	t.Logf("allocations/query: legacy=%.0f optimized=%.0f", old, current)
	if current >= old/10 {
		t.Fatalf("expected at least 90%% fewer allocations: %.0f / %.0f", current, old)
	}
	oldPage, _ := s.legacyWindowedLeaderboard("craftattack:clans:joins", "clan", 1, math.MaxInt64, 10, 0)
	newPage, _ := s.WindowedLeaderboard("craftattack:clans:joins", "clan", 1, math.MaxInt64, 10, 0)
	if !reflect.DeepEqual(oldPage, newPage) {
		t.Fatalf("different scores: %v / %v", oldPage, newPage)
	}
}
