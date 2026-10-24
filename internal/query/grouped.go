package query

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/byPixelTV/flamedb/internal/storage"
)

const MaxGroupedPoints = 20000

type GroupedLimits struct{ Points, Series, Latest, Bytes int }

func DefaultGroupedLimits() GroupedLimits {
	return GroupedLimits{Points: MaxGroupedPoints, Series: 1000, Latest: 10000, Bytes: 3 << 20}
}

func validateGrouped(q *Query) error {
	if len(q.GroupTags) == 0 {
		if q.LastPerGroup || q.GroupPointLimit != 0 {
			return errors.New("LAST/POINTS require GET TAGS")
		}
		return nil
	}
	if q.Type != QueryTypeGet || len(q.Metrics) > 1 {
		return errors.New("TAGS requires a single-metric GET")
	}
	if len(q.GroupTags) > 8 {
		return errors.New("TAGS supports at most 8 grouping keys")
	}
	seen := map[string]bool{}
	for _, key := range q.GroupTags {
		if key == "" || seen[key] || strings.ContainsAny(key, ",=\" \t\r\n\x00\x1f") {
			return errors.New("invalid or duplicate grouping tag")
		}
		seen[key] = true
	}
	if q.GroupPointLimit < 0 || q.GroupPointLimit > MaxGroupedPoints {
		return fmt.Errorf("POINTS must be between 1 and %d", MaxGroupedPoints)
	}
	if q.LastPerGroup {
		if q.GroupBySpec != "" || q.Aggregate != "" || q.GroupPointLimit != 0 {
			return errors.New("LAST cannot be combined with time aggregation or POINTS")
		}
		return nil
	}
	spec, err := parseCalendarSpec(q.GroupBySpec)
	if err != nil || spec.years != 0 || spec.months != 0 || spec.dur <= 0 {
		return errors.New("GET TAGS time series require GROUP BY with a fixed positive duration")
	}
	if q.Aggregate == "" {
		q.Aggregate = AggAvg
	}
	if q.Aggregate != AggAvg && q.Aggregate != AggSum && q.Aggregate != AggCount {
		return errors.New("invalid grouped aggregation")
	}
	return nil
}

type groupedCell struct {
	sum   float64
	count int
}
type groupedState struct {
	labels    map[string]string
	cells     map[int64]*groupedCell
	latest    TaggedValue
	hasLatest bool
}

// AggregateGrouped streams an arbitrary number of raw rows while retaining only
// bounded output. If necessary, coarser aligned buckets merge sums AND counts,
// so AVG remains weighted and COUNT/SUM include all observations.
func AggregateGrouped(q *Query, limits GroupedLimits, scan func(func(storage.Event) error) error) (*Result, error) {
	if err := validateGrouped(q); err != nil {
		return nil, err
	}
	if len(q.GroupTags) == 0 {
		return nil, errors.New("grouping tags required")
	}
	if limits.Points <= 0 || limits.Series <= 0 || limits.Latest <= 0 || limits.Bytes <= 0 {
		return nil, errors.New("invalid grouped result limits")
	}
	budget := limits.Points
	if q.GroupPointLimit > 0 && q.GroupPointLimit < budget {
		budget = q.GroupPointLimit
	}
	interval := int64(q.GroupBy)
	if !q.LastPerGroup {
		spec, _ := parseCalendarSpec(q.GroupBySpec)
		interval = int64(spec.dur)
	}
	groups := map[string]*groupedState{}
	points := 0
	labelBytes := 0
	widen := func() error {
		if interval > math.MaxInt64/2 {
			return errors.New("grouped interval overflow")
		}
		interval *= 2
		points = 0
		for _, g := range groups {
			stamps := make([]int64, 0, len(g.cells))
			for ts := range g.cells {
				stamps = append(stamps, ts)
			}
			sort.Slice(stamps, func(i, j int) bool { return stamps[i] < stamps[j] })
			merged := map[int64]*groupedCell{}
			for _, ts := range stamps {
				bucket := ts / interval * interval
				c := merged[bucket]
				if c == nil {
					c = &groupedCell{}
					merged[bucket] = c
				}
				c.sum += g.cells[ts].sum
				c.count += g.cells[ts].count
			}
			g.cells = merged
			points += len(merged)
		}
		return nil
	}
	err := scan(func(e storage.Event) error {
		var key strings.Builder
		for _, tag := range q.GroupTags {
			v := e.Tags[tag]
			key.WriteString(strconv.Itoa(len(v)))
			key.WriteByte(':')
			key.WriteString(v)
		}
		id := key.String()
		g := groups[id]
		if g == nil {
			limit := limits.Series
			if q.LastPerGroup {
				limit = limits.Latest
			} else if budget < limit {
				limit = budget
			}
			if len(groups) >= limit {
				return fmt.Errorf("grouped result exceeds %d groups; add tag filters", limit)
			}
			labels := map[string]string{}
			for _, tag := range q.GroupTags {
				labels[tag] = e.Tags[tag]
			}
			encoded, _ := json.Marshal(labels)
			labelBytes += len(encoded)
			if labelBytes > limits.Bytes/2 {
				return errors.New("grouped labels exceed result byte limit; add tag filters")
			}
			g = &groupedState{labels: labels, cells: map[int64]*groupedCell{}}
			groups[id] = g
		}
		if q.LastPerGroup {
			if !g.hasLatest || e.Timestamp > g.latest.TS {
				g.latest = TaggedValue{Tags: g.labels, TS: e.Timestamp, Value: e.Value}
				g.hasLatest = true
			}
			return nil
		}
		bucket := e.Timestamp / interval * interval
		for g.cells[bucket] == nil && points >= budget {
			if err := widen(); err != nil {
				return err
			}
			bucket = e.Timestamp / interval * interval
		}
		c := g.cells[bucket]
		if c == nil {
			c = &groupedCell{}
			g.cells[bucket] = c
			points++
		}
		c.count++
		if q.Aggregate != AggCount {
			c.sum += e.Value
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for {
		out := &GroupedResult{IntervalNS: interval}
		for _, key := range keys {
			g := groups[key]
			if q.LastPerGroup {
				out.Latest = append(out.Latest, g.latest)
				continue
			}
			series := TaggedSeries{Tags: g.labels, Points: []SeriesPoint{}}
			for ts, c := range g.cells {
				value := c.sum
				if q.Aggregate == AggAvg {
					value /= float64(c.count)
				} else if q.Aggregate == AggCount {
					value = float64(c.count)
				}
				if math.IsNaN(value) || math.IsInf(value, 0) {
					return nil, errors.New("response serialization failed")
				}
				series.Points = append(series.Points, SeriesPoint{TS: ts, Value: value, Count: c.count})
			}
			sort.Slice(series.Points, func(i, j int) bool { return series.Points[i].TS < series.Points[j].TS })
			out.Series = append(out.Series, series)
		}
		result := &Result{Grouped: out}
		raw, err := json.Marshal(result)
		if err != nil {
			return nil, err
		}
		if len(raw) <= limits.Bytes {
			return result, nil
		}
		if q.LastPerGroup || points <= len(groups) {
			return nil, errors.New("grouped result exceeds byte limit; add tag filters")
		}
		if err := widen(); err != nil {
			return nil, err
		}
	}
}
