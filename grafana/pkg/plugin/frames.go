package plugin

import (
	"encoding/json"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"sort"
	"time"
)

func groupKey(tags map[string]string, keys []string) (string, map[string]string) {
	values := make([]string, len(keys))
	labels := map[string]string{}
	for i, k := range keys {
		values[i] = tags[k]
		labels[k] = tags[k]
	}
	encoded, _ := json.Marshal(values)
	return string(encoded), labels
}

type accumulation struct {
	sum   float64
	count int
}

func groupedSeries(all map[string][]event, m queryModel, interval time.Duration) data.Frames {
	var frames data.Frames
	for _, metric := range m.Metrics {
		groups := map[string]map[int64]*accumulation{}
		labels := map[string]map[string]string{}
		for _, e := range all[metric] {
			key, ls := groupKey(e.Tags, m.GroupBy)
			labels[key] = ls
			if groups[key] == nil {
				groups[key] = map[int64]*accumulation{}
			}
			ts := (e.Timestamp / int64(interval)) * int64(interval)
			if groups[key][ts] == nil {
				groups[key][ts] = &accumulation{}
			}
			groups[key][ts].sum += e.Value
			groups[key][ts].count++
		}
		keys := make([]string, 0, len(groups))
		for k := range groups {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, key := range keys {
			buckets := groups[key]
			stamps := make([]int64, 0, len(buckets))
			for ts := range buckets {
				stamps = append(stamps, ts)
			}
			sort.Slice(stamps, func(i, j int) bool { return stamps[i] < stamps[j] })
			times := []time.Time{}
			values := []*float64{}
			for i, ts := range stamps {
				// Explicit null in gaps prevents Grafana drawing a continuous measurement.
				if i > 0 && ts-stamps[i-1] > int64(interval) {
					times = append(times, time.Unix(0, stamps[i-1]+int64(interval)).UTC())
					values = append(values, nil)
				}
				a := buckets[ts]
				value := a.sum
				if m.Aggregation == "AVG" {
					value /= float64(a.count)
				} else if m.Aggregation == "COUNT" {
					value = float64(a.count)
				}
				times = append(times, time.Unix(0, ts).UTC())
				values = append(values, &value)
			}
			frame := data.NewFrame(metric, data.NewField("Time", nil, times), data.NewField(seriesName(metric, labels[key], m.Legend), data.Labels(labels[key]), values))
			frame.Meta = &data.FrameMeta{Type: data.FrameTypeTimeSeriesMulti}
			frames = append(frames, frame)
		}
	}
	return frames
}

func latestFrames(all map[string][]event, m queryModel) data.Frames {
	// Join each metric's latest observation onto its grouping labels. Missing
	// metrics remain null, never an invented zero. The selected time range applies.
	rows := map[string]map[string]event{}
	labels := map[string]map[string]string{}
	for _, metric := range m.Metrics {
		for _, e := range all[metric] {
			key, ls := groupKey(e.Tags, m.GroupBy)
			labels[key] = ls
			if rows[key] == nil {
				rows[key] = map[string]event{}
			}
			old, ok := rows[key][metric]
			if !ok || e.Timestamp > old.Timestamp {
				rows[key][metric] = e
			}
		}
	}
	keys := make([]string, 0, len(rows))
	for key := range rows {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if m.Mode == "latest" {
		var frames data.Frames
		for _, metric := range m.Metrics {
			for _, key := range keys {
				if e, ok := rows[key][metric]; ok {
					frame := data.NewFrame(metric, data.NewField("Time", nil, []time.Time{time.Unix(0, e.Timestamp).UTC()}), data.NewField(seriesName(metric, labels[key], m.Legend), data.Labels(labels[key]), []float64{e.Value}))
					frame.Meta = &data.FrameMeta{Type: data.FrameTypeTimeSeriesMulti}
					frames = append(frames, frame)
				}
			}
		}
		return frames
	}
	frame := data.NewFrame("Latest values")
	for _, tag := range m.GroupBy {
		values := []string{}
		for _, key := range keys {
			values = append(values, labels[key][tag])
		}
		frame.Fields = append(frame.Fields, data.NewField(tag, nil, values))
	}
	for _, metric := range m.Metrics {
		values := []*float64{}
		times := []*time.Time{}
		for _, key := range keys {
			if e, ok := rows[key][metric]; ok {
				v := e.Value
				t := time.Unix(0, e.Timestamp).UTC()
				values = append(values, &v)
				times = append(times, &t)
			} else {
				values = append(values, nil)
				times = append(times, nil)
			}
		}
		frame.Fields = append(frame.Fields, data.NewField(metric, nil, values), data.NewField(metric+" time", nil, times))
	}
	frame.Meta = &data.FrameMeta{Type: data.FrameTypeTable}
	return data.Frames{frame}
}
