package plugin

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
)

func fakeDB(t *testing.T, handler func(string) string) *wireClient {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				fmt.Fprintln(conn, `{"auth":"required"}`)
				s := bufio.NewScanner(conn)
				if !s.Scan() {
					return
				}
				if s.Text() != "AUTH readonly-test" {
					fmt.Fprintln(conn, `{"error":"invalid key"}`)
					return
				}
				fmt.Fprintln(conn, `{"auth":"ok"}`)
				if s.Scan() {
					fmt.Fprintln(conn, handler(s.Text()))
				}
			}()
		}
	}()
	return &wireClient{address: ln.Addr().String(), apiKey: "readonly-test", timeout: time.Second, slots: make(chan struct{}, 8)}
}
func TestTCPAuthAndNativeSeries(t *testing.T) {
	commands := make(chan string, 1)
	d := &Datasource{client: fakeDB(t, func(cmd string) string {
		commands <- cmd
		return `{"series":[{"ts":2000000000,"value":20,"count":1},{"ts":1000000000,"value":0,"count":0}]}`
	}), maxScan: 100}
	q := backend.DataQuery{Interval: time.Second, MaxDataPoints: 100, TimeRange: backend.TimeRange{From: time.Unix(1, 0), To: time.Unix(10, 0)}}
	frames, err := d.execute(context.Background(), q, queryModel{Metrics: []string{"tps"}, Filters: []filter{{Key: "server", Value: `a" OR x="b`}}})
	if err != nil {
		t.Fatal(err)
	}
	command := <-commands
	if !strings.Contains(command, `WHERE server="a\" OR x=\"b"`) || !strings.Contains(command, "GROUP BY 1s AVG") {
		t.Fatal(command)
	}
	if frames[0].Fields[0].At(0).(time.Time).Unix() != 1 {
		t.Fatal("timestamps must be sorted and decoded as nanoseconds")
	}
	if frames[0].Fields[1].At(0).(*float64) != nil {
		t.Fatal("missing bucket became zero")
	}
	if *frames[0].Fields[1].At(1).(*float64) != 20 {
		t.Fatal("wrong series value")
	}
}
func TestValidationAndFraming(t *testing.T) {
	for _, m := range []queryModel{
		{Metrics: []string{"tps\nDELETE tps"}}, {Metrics: []string{"tps,players"}},
		{Metrics: []string{"tps"}, Aggregation: "DELETE"}, {Metrics: []string{"tps"}, Mode: "raw"},
		{Metrics: []string{"tps"}, Filters: []filter{{Key: "server", Value: "a"}, {Key: "server", Value: "b"}}},
	} {
		if validate(&m) == nil {
			t.Fatalf("accepted invalid query: %+v", m)
		}
	}
	c := fakeDB(t, func(string) string { return `{}` })
	if _, err := c.query(context.Background(), "METRICS\nDELETE x"); err == nil {
		t.Fatal("accepted newline injection")
	}
	_, err := New(context.Background(), backend.DataSourceInstanceSettings{JSONData: []byte(`{}`), DecryptedSecureJSONData: map[string]string{"apiKey": "key\nDELETE x"}})
	if err == nil {
		t.Fatal("accepted key injection")
	}
	c.apiKey = "wrong"
	if _, err := c.query(context.Background(), "METRICS"); err == nil {
		t.Fatal("accepted bad auth")
	}
}
func TestCancellation(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	c := fakeDB(t, func(string) string { <-release; return `{}` })
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	start := time.Now()
	if _, err := c.query(ctx, "METRICS"); err == nil {
		t.Fatal("expected canceled read")
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("cancellation failed to interrupt socket read")
	}
}
func TestTableJoinsLatestValuesAndNulls(t *testing.T) {
	m := queryModel{Metrics: []string{"tps", "players"}, Mode: "table", GroupBy: []string{"server", "region"}}
	all := map[string][]event{
		"tps":     {{Timestamp: 1, Value: 17, Tags: map[string]string{"server": "a", "region": "1"}}, {Timestamp: 3, Value: 20, Tags: map[string]string{"server": "a", "region": "1"}}, {Timestamp: 2, Value: 19, Tags: map[string]string{"server": "b", "region": "2"}}},
		"players": {{Timestamp: 2, Value: 12, Tags: map[string]string{"server": "a", "region": "1"}}},
	}
	frame := latestFrames(all, m)[0]
	if frame.Rows() != 2 || len(frame.Fields) != 6 {
		t.Fatalf("wrong table dimensions: %d x %d", frame.Rows(), len(frame.Fields))
	}
	if *frame.Fields[2].At(0).(*float64) != 20 {
		t.Fatal("not the latest TPS")
	}
	if frame.Fields[4].At(1).(*float64) != nil {
		t.Fatal("missing players became zero")
	}
}
func TestGroupedAggregationAndGaps(t *testing.T) {
	m := queryModel{Metrics: []string{"tps"}, Mode: "timeseries", Aggregation: "AVG", GroupBy: []string{"server"}, Legend: "{{metric}} {{server}}"}
	all := map[string][]event{"tps": {{Timestamp: 1e9, Value: 10, Tags: map[string]string{"server": "a"}}, {Timestamp: 1500000000, Value: 20, Tags: map[string]string{"server": "a"}}, {Timestamp: 4e9, Value: 20, Tags: map[string]string{"server": "a"}}}}
	frame := groupedSeries(all, m, time.Second)[0]
	if frame.Fields[1].Name != "tps a" || *frame.Fields[1].At(0).(*float64) != 15 || frame.Rows() != 3 || frame.Fields[1].At(1).(*float64) != nil {
		t.Fatal("incorrect aggregation, legend or gap")
	}
}
func TestScanLimitDoesNotReturnPartialData(t *testing.T) {
	d := &Datasource{maxScan: 1, client: fakeDB(t, func(string) string { return `{"events":[{"timestamp":1,"value":1},{"timestamp":2,"value":2}]}` })}
	q := backend.DataQuery{TimeRange: backend.TimeRange{From: time.Unix(0, 0), To: time.Unix(10, 0)}}
	if _, err := d.execute(context.Background(), q, queryModel{Metrics: []string{"tps"}, Mode: "table"}); err == nil || !strings.Contains(err.Error(), "scan limit") {
		t.Fatalf("expected explicit limit error, got %v", err)
	}
}
func TestQueryDataPreservesRefIDsAndErrors(t *testing.T) {
	d := &Datasource{client: fakeDB(t, func(string) string { return `{"series":[]}` }), maxScan: 100}
	raw, _ := json.Marshal(queryModel{Metrics: []string{"tps"}, Legend: "{{metric}}"})
	out, err := d.QueryData(context.Background(), &backend.QueryDataRequest{Queries: []backend.DataQuery{{RefID: "A", JSON: raw, TimeRange: backend.TimeRange{From: time.Unix(0, 0), To: time.Unix(10, 0)}}, {RefID: "B", JSON: []byte(`{"metrics":["x\nDELETE x"]}`)}}})
	if err != nil || out.Responses["A"].Error != nil || len(out.Responses["A"].Frames) != 1 || out.Responses["A"].Frames[0].RefID != "A" || out.Responses["B"].Error == nil {
		t.Fatal("lost RefID or query error")
	}
	if out.Responses["A"].Frames[0].Fields[1].Config.DisplayNameFromDS != "tps" {
		t.Fatal("custom legend was not preserved")
	}
}
