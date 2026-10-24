package plugin

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
)

func TestReplyFramingAndSize(t *testing.T) {
	payload := `{"events":[{"timestamp":1,"tags":{"large":"` + strings.Repeat("x", 200000) + `"}}]}` + "\n"
	r, err := readReply(bufio.NewReaderSize(strings.NewReader(payload), 32))
	if err != nil || len(r.Events) != 1 || len(r.Events[0].Tags["large"]) != 200000 {
		t.Fatalf("chunked response: %v", err)
	}
	_, err = readReply(bufio.NewReader(strings.NewReader(strings.Repeat(" ", maxResponseBytes) + "x\n")))
	if !errors.Is(err, errResponseTooLarge) {
		t.Fatalf("oversize frame: %v", err)
	}
	_, err = readReply(bufio.NewReader(strings.NewReader(`{"events":[]}`)))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("accepted unterminated frame: %v", err)
	}
	_, err = readReply(bufio.NewReader(strings.NewReader("{bad json}\n")))
	if err == nil {
		t.Fatal("accepted malformed JSON")
	}
	_, err = readReply(bufio.NewReader(strings.NewReader("{\"error\":\"bufio.Scanner: token too long\"}\n")))
	if !errors.Is(err, errResponseTooLarge) {
		t.Fatalf("upstream frame limit was not recognized: %v", err)
	}
}

var windowCommand = regexp.MustCompile(` FROM (\S+) TO (\S+) LIMIT (\d+) ORDER ASC$`)

func fixtureHandler(events []event, calls *atomic.Int32) func(string) string {
	return func(command string) string {
		calls.Add(1)
		match := windowCommand.FindStringSubmatch(command)
		if match == nil {
			return `{"error":"test expected a bounded time-window read"}`
		}
		from, err := time.Parse(time.RFC3339Nano, match[1])
		if err != nil {
			return `{"error":"invalid FROM"}`
		}
		to, err := time.Parse(time.RFC3339Nano, match[2])
		if err != nil {
			return `{"error":"invalid TO"}`
		}
		limit, _ := strconv.Atoi(match[3])
		page := []event{}
		for _, e := range events {
			if e.Timestamp >= from.UnixNano() && e.Timestamp < to.UnixNano() {
				page = append(page, e)
				if len(page) == limit {
					break
				}
			}
		}
		raw, _ := json.Marshal(reply{Events: page})
		return string(raw)
	}
}

func TestWindowedReadKeepsEveryEventAndTimestampTie(t *testing.T) {
	events := []event{}
	for i := 0; i < 2505; i++ {
		events = append(events, event{Timestamp: int64(i), Value: float64(i), Tags: map[string]string{"server": "a"}})
	}
	for i := 0; i < 3; i++ {
		events = append(events, event{Timestamp: 1024, Value: float64(3000 + i), Tags: map[string]string{"server": "a"}})
	}
	sort.SliceStable(events, func(i, j int) bool { return events[i].Timestamp < events[j].Timestamp })
	var calls atomic.Int32
	handler := fixtureHandler(events, &calls)
	c := fakeDB(t, func(command string) string {
		if !strings.Contains(command, `WHERE server="a"`) {
			return `{"error":"lost filters"}`
		}
		return handler(command)
	})
	c.timeout = 5 * time.Second
	d := &Datasource{client: c, maxScan: 3000}
	got, err := d.readEvents(context.Background(), "system:region:tps", []filter{{Key: "server", Value: "a"}}, time.Unix(0, 0), time.Unix(0, 4096))
	if err != nil || len(got) != len(events) {
		t.Fatalf("got %d, want %d; err=%v", len(got), len(events), err)
	}
	for i := range got {
		if got[i].Timestamp != events[i].Timestamp || got[i].Value != events[i].Value {
			t.Fatalf("lost or duplicated event at %d", i)
		}
	}
	if calls.Load() < 3 {
		t.Fatal("fixture did not exercise multiple time windows")
	}
	d.maxScan = 2000
	got, err = d.readEvents(context.Background(), "system:region:tps", []filter{{Key: "server", Value: "a"}}, time.Unix(0, 0), time.Unix(0, 4096))
	if err == nil || !strings.Contains(err.Error(), "scan limit") || got != nil {
		t.Fatalf("expected limit error without partial results: %d, %v", len(got), err)
	}
}

func TestWindowedReadRecoversFromFrameLargerThan16MiB(t *testing.T) {
	padding := strings.Repeat("x", 22000)
	events := make([]event, 800)
	for i := range events {
		events[i] = event{Timestamp: int64(i + 1), Value: float64(i + 1), Tags: map[string]string{"server": "a", "padding": padding}}
	}
	var calls atomic.Int32
	c := fakeDB(t, fixtureHandler(events, &calls))
	c.timeout = 10 * time.Second
	d := &Datasource{client: c, maxScan: 1000}
	q := backend.DataQuery{Interval: time.Second, TimeRange: backend.TimeRange{From: time.Unix(0, 0), To: time.Unix(0, 1000)}}
	frames, err := d.execute(context.Background(), q, queryModel{Metrics: []string{"system:region:tps"}, Mode: "timeseries", Aggregation: "AVG", GroupBy: []string{"server"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 1 || *frames[0].Fields[1].At(0).(*float64) != 400.5 {
		t.Fatal("lost values while splitting oversized response")
	}
	if calls.Load() < 3 {
		t.Fatal("fixture did not exercise oversize recovery")
	}
}

func TestWholeQueryTimeoutAppliesAcrossWindows(t *testing.T) {
	events := make([]event, 1100)
	for i := range events {
		events[i] = event{Timestamp: int64(i), Value: 1}
	}
	var calls atomic.Int32
	handler := fixtureHandler(events, &calls)
	c := fakeDB(t, func(command string) string { time.Sleep(40 * time.Millisecond); return handler(command) })
	c.timeout = 65 * time.Millisecond
	d := &Datasource{client: c, maxScan: 2000}
	q := backend.DataQuery{TimeRange: backend.TimeRange{From: time.Unix(0, 0), To: time.Unix(0, 2048)}}
	start := time.Now()
	frames, err := d.execute(context.Background(), q, queryModel{Metrics: []string{"tps"}, GroupBy: []string{"server"}})
	if err == nil || frames != nil {
		t.Fatal("query was allowed to reset its timeout for every window")
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("total query timeout was not enforced")
	}
}

func TestSamplingJitterDoesNotCreateOneBucketHole(t *testing.T) {
	m := queryModel{Metrics: []string{"system:regions"}, Aggregation: "AVG", GroupBy: []string{"server"}}
	events := []event{{Timestamp: 1e9, Value: 89, Tags: map[string]string{"server": "a"}}, {Timestamp: 3e9, Value: 90, Tags: map[string]string{"server": "a"}}, {Timestamp: 7e9, Value: 88, Tags: map[string]string{"server": "a"}}}
	frame := groupedSeries(map[string][]event{"system:regions": events}, m, time.Second)[0]
	if frame.Rows() != 4 || *frame.Fields[1].At(1).(*float64) != 90 || frame.Fields[1].At(2).(*float64) != nil {
		t.Fatal("one missing bucket should connect, a longer gap should remain null")
	}
}
