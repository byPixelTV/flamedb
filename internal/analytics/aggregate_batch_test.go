package analytics

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"testing"

	"github.com/byPixelTV/flamedb/internal/query"
)

func TestAggregateBatchExactFreshnessAndIndependentErrors(t *testing.T) {
	e, _ := integration(t)
	ctx := context.Background()
	metric := "aggregate_wave_" + token()
	base := int64(1704067200000000000)
	writes := []*query.Query{}
	for player := range 32 {
		for i, value := range []float64{1e16, 1, -1e16, float64(3 + player)} {
			writes = append(writes, &query.Query{Type: query.QueryTypeWrite, Metric: metric, Timestamp: base + int64(i*1000+player), Value: value, Tags: map[string]string{"player": string(rune('A' + player)), "region": "eu"}})
		}
	}
	if result, err := e.ExecuteBatch(writes); err != nil || result.Accepted != len(writes) {
		t.Fatalf("seed: %+v %v", result, err)
	}
	makeRead := func(player string, kind query.AggType, where map[string]string, from, to int64) *aggregateRead {
		t.Helper()
		encoded := EncodeTags(where)
		raw, _ := json.Marshal(encoded)
		return &aggregateRead{ctx: ctx, metric: metric, encoded: EncodeString(metric), key: "player", value: player, filters: string(raw), q: query.Query{Aggregate: kind, Where: where, From: from, To: to}, result: make(chan aggregateReply, 1)}
	}
	wave := []*aggregateRead{}
	wants := []query.AggregateResult{}
	for player := range 32 {
		id := string(rune('A' + player))
		kind := []query.AggType{query.AggSum, query.AggAvg, query.AggCount}[player%3]
		where := map[string]string{"player": id, "region": "eu"}
		from, to := int64(0), int64(0)
		if player%4 == 0 {
			from = base + 2000
		}
		if player%5 == 0 {
			to = base + 3000
		}
		if player%7 == 0 {
			where["region"] = "missing"
		}
		count, sum := 0, float64(0)
		for i, value := range []float64{1e16, 1, -1e16, float64(3 + player)} {
			ts := base + int64(i*1000+player)
			if where["region"] == "eu" && ts >= from && (to == 0 || ts < to) {
				count++
				sum += value
			}
		}
		if kind == query.AggCount {
			sum = float64(count)
		} else if kind == query.AggAvg && count > 0 {
			sum /= float64(count)
		}
		if sum == 0 {
			sum = 0
		}
		wave = append(wave, makeRead(id, kind, where, from, to))
		wants = append(wants, query.AggregateResult{Type: string(kind), Value: sum, Count: count})
	}
	e.reads.group(wave)
	for i, item := range wave {
		got := <-item.result
		if got.err != nil || got.value == nil || got.value.Count != wants[i].Count || got.value.Type != wants[i].Type || math.Float64bits(got.value.Value) != math.Float64bits(wants[i].Value) {
			t.Fatalf("request %d: %+v %v expected %+v", i, got.value, got.err, wants[i])
		}
	}
	// Every new statement must see changes acknowledged before its requests.
	run(t, e, &query.Query{Type: query.QueryTypeWrite, Metric: metric, Timestamp: base + 5000, Value: 7, Tags: map[string]string{"player": "A", "region": "eu"}})
	checkFresh := func(want float64, count int) {
		t.Helper()
		items := []*aggregateRead{makeRead("A", query.AggSum, map[string]string{"player": "A"}, 0, 0), makeRead("A", query.AggCount, map[string]string{"player": "A", "region": "eu"}, 0, 0)}
		e.reads.group(items)
		for i, item := range items {
			got := <-item.result
			value := want
			if i == 1 {
				value = float64(count)
			}
			if got.err != nil || got.value.Value != value || got.value.Count != count {
				t.Fatalf("fresh read: %+v %v expected %v/%d", got.value, got.err, value, count)
			}
		}
	}
	checkFresh(10, 5)
	run(t, e, &query.Query{Type: query.QueryTypeDelete, Metric: metric, Where: map[string]string{"player": "A"}, From: base + 3000})
	checkFresh(0, 3)
	for i := range 2 {
		run(t, e, &query.Query{Type: query.QueryTypeWrite, Metric: metric, Timestamp: base + 6000 + int64(i), Value: math.MaxFloat64, Tags: map[string]string{"player": "overflow"}})
	}
	items := []*aggregateRead{makeRead("overflow", query.AggSum, map[string]string{"player": "overflow"}, 0, 0), makeRead("overflow", query.AggCount, map[string]string{"player": "overflow"}, 0, 0), makeRead("B", query.AggSum, map[string]string{"player": "B"}, 0, 0)}
	e.reads.group(items)
	if got := <-items[0].result; got.err == nil {
		t.Fatal("overflow did not fail its own read")
	}
	for i, want := range []float64{2, 4} {
		got := <-items[i+1].result
		if got.err != nil || got.value.Value != want {
			t.Fatalf("unrelated read poisoned by overflow: %+v %v", got.value, got.err)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	items = []*aggregateRead{makeRead("B", query.AggSum, map[string]string{"player": "B"}, 0, 0), makeRead("C", query.AggCount, map[string]string{"player": "C"}, 0, 0), makeRead("D", query.AggSum, map[string]string{"player": "D"}, 0, 0)}
	items[0].ctx = canceled
	e.reads.execute(items)
	if got := <-items[0].result; !errors.Is(got.err, context.Canceled) {
		t.Fatalf("canceled read: %v", got.err)
	}
	for i, want := range []float64{4, 6} {
		got := <-items[i+1].result
		if got.err != nil || got.value.Value != want {
			t.Fatalf("unrelated read canceled: %+v %v", got.value, got.err)
		}
	}
	// Token candidates deliberately use jsonb_each_text, which gives the same
	// token to a numeric tag and its string spelling. Matching must still obey
	// the original JSON containment predicate, including in a shared scan.
	if _, err := e.db.Exec(ctx, `INSERT INTO flame_events(metric,ts,value,tags) VALUES($1,$2,99,'{"player":123}'::jsonb),($1,$3,7,'{"player":"123"}'::jsonb)`, EncodeString(metric), base+8000, base+8001); err != nil {
		t.Fatal(err)
	}
	items = []*aggregateRead{makeRead("123", query.AggSum, map[string]string{"player": "123"}, 0, 0), makeRead("123", query.AggCount, map[string]string{"player": "123"}, 0, 0), makeRead("B", query.AggSum, map[string]string{"player": "B", "region": "missing"}, 0, 0)}
	e.reads.group(items)
	for i, want := range []float64{7, 1, 0} {
		got := <-items[i].result
		if got.err != nil || got.value.Value != want {
			t.Fatalf("JSON tag type or residual filter mismatch: %+v %v", got.value, got.err)
		}
	}
}
