package storage

import (
	"fmt"
	"math"
	"testing"
)

func TestLatestAfterRestartAndHistoricalWrite(t *testing.T) {
	path := t.TempDir()
	s, err := Open(path, "none")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.WriteEvent(Event{Metric: "m", Timestamp: 200, Value: 2}, true); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(path, "none")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.WriteEvent(Event{Metric: "m", Timestamp: 100, Value: 1}, true); err != nil {
		t.Fatal(err)
	}
	rows, err := s.ReadRangeDesc("m", 0, math.MaxInt64, 1, 0)
	if err != nil || len(rows) != 1 || rows[0].Timestamp != 200 {
		t.Fatalf("%v %v", rows, err)
	}
	rows, err = s.ReadRangeDesc("m", 0, 200, 1, 0)
	if err != nil || len(rows) != 1 || rows[0].Timestamp != 100 {
		t.Fatalf("exclusive TO: %v %v", rows, err)
	}
}
func TestRebalanceRebuildsTagIndex(t *testing.T) {
	source, err := Open(t.TempDir(), "none")
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	target, err := Open(t.TempDir(), "none")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	if err = source.WriteEvent(Event{Metric: "m", Timestamp: 10, Tags: map[string]string{"player": "p"}}, true); err != nil {
		t.Fatal(err)
	}
	data, err := source.ExportMetricData("m")
	if err != nil {
		t.Fatal(err)
	}
	if err = target.ImportRebalanceData(data); err != nil {
		t.Fatal(err)
	}
	got, err := target.ReadRangeWithTags("m", 0, 20, map[string]string{"player": "p"})
	if err != nil || len(got) != 1 {
		t.Fatalf("%v %v", got, err)
	}
}
func TestOverwrittenTagNotReturned(t *testing.T) {
	s, err := Open(t.TempDir(), "none")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, v := range []string{"a", "b"} {
		if err = s.WriteEvent(Event{Metric: "m", Timestamp: 10, Tags: map[string]string{"tag": v}}, true); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.ReadRangeWithTags("m", 0, 20, map[string]string{"tag": "a"})
	if err != nil || len(got) != 0 {
		t.Fatalf("%v %v", got, err)
	}
}

func TestAscendingPaginationAndIndexChoice(t *testing.T) {
	s, err := Open(t.TempDir(), "none")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var events []Event
	for i := 1; i <= 10; i++ {
		events = append(events, Event{Metric: "m", Timestamp: int64(i), Value: float64(i), Tags: map[string]string{"region": "eu", "player": fmt.Sprint(i)}})
	}
	if err = s.WriteEvents(events, true); err != nil {
		t.Fatal(err)
	}
	if err = s.cardUpdater.Flush(true); err != nil {
		t.Fatal(err)
	}
	rows, err := s.ReadPage("m", 0, 20, map[string]string{"region": "eu"}, 2, 3, false)
	if err != nil || len(rows) != 2 || rows[0].Timestamp != 4 || rows[1].Timestamp != 5 {
		t.Fatalf("%v %v", rows, err)
	}
	key, _ := s.BestIndexTag("m", map[string]string{"region": "eu", "player": "4"})
	if key != "player" {
		t.Fatal(key)
	}
}
