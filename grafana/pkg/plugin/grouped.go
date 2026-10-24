package plugin

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

func (d *Datasource) groupedFrames(parent context.Context, q backend.DataQuery, m queryModel, interval time.Duration) (data.Frames, bool, error) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	replies := make([]*groupedReply, len(m.Metrics))
	errs := make([]error, len(m.Metrics))
	var unsupported atomic.Bool
	var wg sync.WaitGroup
	for i, metric := range m.Metrics {
		wg.Add(1)
		go func(index int, metric string) {
			defer wg.Done()
			command := "GET " + metric + where(m.Filters) + " FROM " + q.TimeRange.From.UTC().Format(time.RFC3339Nano) + " TO " + q.TimeRange.To.UTC().Format(time.RFC3339Nano)
			if m.Mode == "timeseries" {
				command += " GROUP BY " + interval.String() + " " + m.Aggregation
			}
			command += " TAGS " + strings.Join(m.GroupBy, ",")
			if m.Mode == "timeseries" {
				command += " POINTS " + strconv.Itoa(20000/len(m.Metrics))
			} else {
				command += " LAST"
			}
			r, err := d.client.query(ctx, command)
			if err != nil {
				if strings.Contains(err.Error(), "unknown keyword: TAGS") {
					unsupported.Store(true)
				} else {
					errs[index] = err
				}
				cancel()
				return
			}
			if r.Grouped == nil {
				unsupported.Store(true)
				cancel()
				return
			}
			replies[index] = r.Grouped
		}(i, metric)
	}
	wg.Wait()
	if unsupported.Load() {
		return nil, false, nil
	}
	// Prefer the originating server error to cancellation of sibling metric reads.
	for _, err := range errs {
		if err != nil && !errors.Is(err, context.Canceled) {
			return nil, true, err
		}
	}
	for _, err := range errs {
		if err != nil {
			return nil, true, err
		}
	}
	groups := map[string]bool{}
	all := map[string][]event{}
	var frames data.Frames
	for i, g := range replies {
		if g == nil {
			return nil, true, errors.New("missing grouped metric response")
		}
		metric := m.Metrics[i]
		if m.Mode != "timeseries" {
			all[metric] = []event{}
			for _, value := range g.Latest {
				key, _ := groupKey(value.Tags, m.GroupBy)
				groups[key] = true
				all[metric] = append(all[metric], event{Metric: metric, Timestamp: value.TS, Value: value.Value, Tags: value.Tags})
			}
			continue
		}
		if len(g.Series) > 0 && g.IntervalNS <= 0 {
			return nil, true, errors.New("invalid grouped response interval")
		}
		for _, series := range g.Series {
			key, _ := groupKey(series.Tags, m.GroupBy)
			groups[key] = true
			sort.Slice(series.Points, func(i, j int) bool { return series.Points[i].TS < series.Points[j].TS })
			times := []time.Time{}
			values := []*float64{}
			for j, p := range series.Points {
				if j > 0 && (p.TS-series.Points[j-1].TS)/g.IntervalNS > 2 {
					times = append(times, time.Unix(0, series.Points[j-1].TS+g.IntervalNS).UTC())
					values = append(values, nil)
				}
				v := p.Value
				times = append(times, time.Unix(0, p.TS).UTC())
				values = append(values, &v)
			}
			frame := data.NewFrame(metric, data.NewField("Time", nil, times), data.NewField(seriesName(metric, series.Tags, m.Legend), data.Labels(series.Tags), values))
			frame.Meta = &data.FrameMeta{Type: data.FrameTypeTimeSeriesMulti, Custom: map[string]any{"interval_ns": g.IntervalNS}}
			frames = append(frames, frame)
		}
	}
	limit := 1000
	if m.Mode == "table" {
		limit = 10000
	}
	if len(groups) > limit {
		return nil, true, errors.New("too many grouped series; add server/world/region filters")
	}
	if m.Mode != "timeseries" {
		frames = latestFrames(all, m)
	}
	return frames, true, nil
}
