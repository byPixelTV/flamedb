package query

import (
	"fmt"
	"math"
	"reflect"
	"sort"
	"testing"

	"github.com/byPixelTV/flamedb/internal/aggregates"
	"github.com/byPixelTV/flamedb/internal/storage"
)

func TestNamespacedIdentifiersRemainIsolated(t *testing.T) {
	path := t.TempDir()
	store, err := storage.Open(path, "none")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if store != nil {
			store.Close()
		}
	}()
	exec := NewExecutor(store, aggregates.New(store.DB()))
	run := func(line string) *Result {
		t.Helper()
		q, err := Parse(line)
		if err != nil {
			t.Fatalf("%s: %v", line, err)
		}
		result, err := exec.Execute(q)
		if err != nil {
			t.Fatalf("%s: %v", line, err)
		}
		return result
	}
	names := []string{"smp", "smp:", "smp:kills", "smp::kills", "idx:custom", "smp%3A"}
	for i, name := range names {
		run(fmt.Sprintf(`WRITE %s %d player:id="p:1" lb="p:1" ts=123`, name, i+1))
	}
	// A colon in a tag key must not collide with the tag value separator.
	run(`WRITE smp: 20 player="id:p:1" ts=124`)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = nil
	store, err = storage.Open(path, "none")
	if err != nil {
		t.Fatal(err)
	}
	exec = NewExecutor(store, aggregates.New(store.DB()))
	metrics := exec.GetAllMetrics()
	sort.Strings(metrics)
	want := append([]string(nil), names...)
	sort.Strings(want)
	if !reflect.DeepEqual(metrics, want) {
		t.Fatalf("metrics: %q, want %q", metrics, want)
	}
	for i, name := range names {
		result := run(fmt.Sprintf(`GET %s WHERE player:id="p:1"`, name))
		if len(result.Events) != 1 || result.Events[0].Metric != name || result.Events[0].Value != float64(i+1) {
			t.Fatalf("%s: %+v", name, result)
		}
		for _, suffix := range []string{"", " FROM 1970-01-01T00:00:00.000000001Z TO 1970-01-01T00:00:00.000001Z ENTITY player:id"} {
			result = run("LEADERBOARD " + name + suffix)
			if len(result.Leaderboard) != 1 || result.Leaderboard[0].EntityID != "p:1" || result.Leaderboard[0].Value != float64(i+1) {
				t.Fatalf("%s: %+v", name, result)
			}
		}
		stats := run("STATS " + name + " TAGS player:id")
		if len(stats.Stats.TagStats) != 1 || stats.Stats.TagStats[0].Cardinality != 1 {
			t.Fatalf("stats %s: %+v", name, stats.Stats)
		}
	}
	// Rebalancing must preserve public names and rebuild namespaced tag indexes.
	data, err := store.ExportMetricData("smp:")
	if err != nil {
		t.Fatal(err)
	}
	target, err := storage.Open(t.TempDir(), "none")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	if err := target.ImportRebalanceData(data); err != nil {
		t.Fatal(err)
	}
	rows, err := target.ReadRangeWithTags("smp:", 0, math.MaxInt64, map[string]string{"player:id": "p:1"})
	if err != nil || len(rows) != 1 || rows[0].Value != 2 {
		t.Fatalf("import: %+v %v", rows, err)
	}
	score, err := aggregates.New(target.DB()).Get("smp:", "p:1")
	if err != nil || score != 2 {
		t.Fatalf("import score: %v %v", score, err)
	}
	run(`SET smp: 9 lb="p:1"`)
	if got := run("LEADERBOARD smp:").Leaderboard; len(got) != 1 || got[0].Value != 9 {
		t.Fatal(got)
	}
	run(`DELETE smp: lb="p:1"`)
	if got := run("LEADERBOARD smp:").Leaderboard; len(got) != 0 {
		t.Fatal(got)
	}
	run("DELETE smp:")
	for _, name := range names {
		rows, err := store.ReadRange(name, 0, math.MaxInt64)
		count := 1
		if name == "smp:" {
			count = 0
		}
		if err != nil || len(rows) != count {
			t.Fatalf("delete isolation %s: %+v %v", name, rows, err)
		}
	}
}
