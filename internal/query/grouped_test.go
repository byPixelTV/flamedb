package query

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/byPixelTV/flamedb/internal/aggregates"
	"github.com/byPixelTV/flamedb/internal/storage"
)

func TestGroupedParserValidation(t *testing.T) {
	for _, line := range []string{
		"GET x TAGS region LAST", "GET x GROUP BY 1m AVG TAGS server,world,region POINTS 1000",
	} {
		if _, err := Parse(line); err != nil {
			t.Fatalf("%s: %v", line, err)
		}
	}
	for _, line := range []string{
		"GET x LAST", "GET x POINTS 10", "GET x TAGS region", "GET x GROUP BY 1mo AVG TAGS region", "GET x GROUP BY 1s AVG TAGS a,a", "GET x TAGS a, LAST", "GET x,y TAGS region LAST", "GET x GROUP BY 1s TAGS a POINTS 0", "GET x GROUP BY 1s TAGS a POINTS 20001", "GET x TAGS a LAST AVG", "GET x TAGS a LAST GROUP BY 1s", "LEADERBOARD x TAGS region LAST",
	} {
		if _, err := Parse(line); err == nil {
			t.Fatalf("accepted %s", line)
		}
	}
}

func TestGroupedAggregationCoarsensAllHistoryWithoutLosingCounts(t *testing.T) {
	const records = 60001
	for _, agg := range []string{"AVG", "SUM", "COUNT"} {
		t.Run(agg, func(t *testing.T) {
			q, err := Parse("GET system:region:tps GROUP BY 1s " + agg + " TAGS region POINTS 12")
			if err != nil {
				t.Fatal(err)
			}
			result, err := AggregateGrouped(q, DefaultGroupedLimits(), func(visit func(storage.Event) error) error {
				for i := 0; i < records; i++ {
					if err := visit(storage.Event{Timestamp: int64(i) * int64(time.Second), Value: float64(i%7) + .25, Tags: map[string]string{"region": string(rune('a' + i%3))}}); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if result.Grouped.IntervalNS <= int64(time.Second) {
				t.Fatal("fixture did not coarsen buckets")
			}
			total, points := 0, 0
			for _, series := range result.Grouped.Series {
				for _, point := range series.Points {
					count := 0
					sum := 0.0
					for i := 0; i < records; i++ {
						if string(rune('a'+i%3)) == series.Tags["region"] && (int64(i)*int64(time.Second))/result.Grouped.IntervalNS*result.Grouped.IntervalNS == point.TS {
							count++
							sum += float64(i%7) + .25
						}
					}
					want := sum
					if agg == "AVG" {
						want /= float64(count)
					} else if agg == "COUNT" {
						want = float64(count)
					}
					if point.Count != count || math.Abs(point.Value-want) > 1e-9 {
						t.Fatalf("count=%d/%d value=%v/%v", point.Count, count, point.Value, want)
					}
					total += point.Count
					points++
				}
			}
			if total != records || points > 12 {
				t.Fatalf("records=%d points=%d", total, points)
			}
		})
	}
}

func TestGroupedLatestAndLimits(t *testing.T) {
	q, _ := Parse("GET x TAGS region LAST")
	scan := func(visit func(storage.Event) error) error {
		for i := 0; i < 60001; i++ {
			if err := visit(storage.Event{Timestamp: int64(i), Value: float64(i), Tags: map[string]string{"region": string(rune('a' + i%3)), "unused": "not returned"}}); err != nil {
				return err
			}
		}
		return nil
	}
	result, err := AggregateGrouped(q, DefaultGroupedLimits(), scan)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Grouped.Latest) != 3 {
		t.Fatal(result)
	}
	for _, last := range result.Grouped.Latest {
		if last.TS < 59998 || len(last.Tags) != 1 {
			t.Fatal(last)
		}
	}
	limits := DefaultGroupedLimits()
	limits.Latest = 2
	if result, err := AggregateGrouped(q, limits, scan); err == nil || result != nil {
		t.Fatal("group limit returned partial data")
	}
	sentinel := errors.New("storage failed")
	if _, err := AggregateGrouped(q, DefaultGroupedLimits(), func(func(storage.Event) error) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
}

func TestPebbleGroupedReadFiltersAndSnapshotWalkCancellation(t *testing.T) {
	store, err := storage.Open(t.TempDir(), "none")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	events := make([]storage.Event, 60001)
	for i := range events {
		events[i] = storage.Event{Metric: "system:region:tps", Timestamp: int64(i + 1), Value: float64(i), Tags: map[string]string{"server": "a", "region": string(rune('a' + i%3))}}
	}
	if err := store.WriteEvents(events, true); err != nil {
		t.Fatal(err)
	}
	exec := NewExecutor(store, aggregates.New(store.DB()))
	q, _ := Parse(`GET system:region:tps WHERE server="a" FROM 1970-01-01T00:00:00Z TO 1970-01-01T00:00:01Z GROUP BY 1s COUNT TAGS region`)
	result, err := exec.Execute(q)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, s := range result.Grouped.Series {
		for _, p := range s.Points {
			count += p.Count
		}
	}
	if count != len(events) {
		t.Fatalf("count=%d", count)
	}
	q, _ = Parse(`GET system:region:tps WHERE server="missing" GROUP BY 1s COUNT TAGS region`)
	result, err = exec.Execute(q)
	if err != nil || len(result.Grouped.Series) != 0 {
		t.Fatalf("filter: %v %v", result, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	seen := 0
	err = store.WalkEvents(ctx, "system:region:tps", 0, 1e9, nil, func(storage.Event) error {
		seen++
		if seen == 10 {
			cancel()
		}
		return nil
	})
	if !errors.Is(err, context.Canceled) || seen != 10 {
		t.Fatalf("cancel: %d %v", seen, err)
	}
	// The default and explicit limits remain bounded by compact output, not history.
	if _, err := Parse(strings.Repeat(" ", 1) + "GET x GROUP BY 1s AVG TAGS server POINTS 20"); err != nil {
		t.Fatal(err)
	}
}
