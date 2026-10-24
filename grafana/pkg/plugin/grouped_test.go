package plugin

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
)

func TestNativeGroupingBypassesRawScanLimit(t *testing.T) {
	c := fakeDB(t, func(command string) string {
		if !strings.Contains(command, "TAGS server POINTS 20000") {
			return `{"error":"expected server grouping"}`
		}
		return `{"grouped":{"interval_ns":1000000000,"series":[{"tags":{"server":"a"},"points":[{"ts":1000000000,"value":30000,"count":30000},{"ts":2000000000,"value":30000,"count":30000}]}]}}`
	})
	d := &Datasource{client: c, maxScan: 1, serverGrouping: true}
	q := backend.DataQuery{TimeRange: backend.TimeRange{From: time.Unix(0, 0), To: time.Unix(10, 0)}}
	frames, err := d.execute(context.Background(), q, queryModel{Metrics: []string{"regions"}, Aggregation: "COUNT", GroupBy: []string{"server"}})
	if err != nil || len(frames) != 1 || *frames[0].Fields[1].At(0).(*float64) != 30000 {
		t.Fatalf("frames=%v err=%v", frames, err)
	}
}
func TestNativeLatestTableAndUpgradeFallback(t *testing.T) {
	c := fakeDB(t, func(command string) string {
		if strings.Contains(command, "TAGS server LAST") {
			return `{"grouped":{"latest":[{"tags":{"server":"a"},"ts":2000000000,"value":20}]}}`
		}
		return `{"error":"unexpected command"}`
	})
	d := &Datasource{client: c, maxScan: 1, serverGrouping: true}
	q := backend.DataQuery{TimeRange: backend.TimeRange{From: time.Unix(0, 0), To: time.Unix(10, 0)}}
	frames, err := d.execute(context.Background(), q, queryModel{Metrics: []string{"regions"}, Mode: "table", GroupBy: []string{"server"}})
	if err != nil || frames[0].Rows() != 1 || *frames[0].Fields[1].At(0).(*float64) != 20 {
		t.Fatalf("frames=%v err=%v", frames, err)
	}
	d.client = fakeDB(t, func(command string) string {
		if strings.Contains(command, " TAGS ") {
			return `{"error":"unknown keyword: TAGS"}`
		}
		return `{"events":[{"timestamp":1,"value":2,"tags":{"server":"a"}}]}`
	})
	frames, err = d.execute(context.Background(), q, queryModel{Metrics: []string{"regions"}, GroupBy: []string{"server"}})
	if err != nil || len(frames) != 1 {
		t.Fatalf("legacy fallback failed: %v", err)
	}
}
