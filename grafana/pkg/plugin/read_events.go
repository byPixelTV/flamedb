package plugin

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"
)

const rawWindowEvents = 1000

type readWindow struct{ from, to time.Time }

// Read disjoint half-open time windows rather than offset pages. This preserves
// all equal-timestamp observations and avoids rescanning every previous page.
// Each storage request has its own snapshot; this is not an atomic snapshot of
// concurrent historical backfills across the complete dashboard range.
func (d *Datasource) readEvents(ctx context.Context, metric string, filters []filter, from, to time.Time) ([]event, error) {
	windows := []readWindow{{from, to}}
	events := []event{}
	for len(windows) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		w := windows[len(windows)-1]
		windows = windows[:len(windows)-1]
		remaining := d.maxScan - len(events)
		limit := rawWindowEvents + 1
		if remaining < rawWindowEvents || w.to.Sub(w.from) <= time.Nanosecond {
			limit = remaining + 1
		}
		command := "GET " + metric + where(filters) + " FROM " + w.from.UTC().Format(time.RFC3339Nano) + " TO " + w.to.UTC().Format(time.RFC3339Nano) + " LIMIT " + strconv.Itoa(limit) + " ORDER ASC"
		r, err := d.client.query(ctx, command)
		split := errors.Is(err, errResponseTooLarge)
		if err != nil && !split {
			return nil, err
		}
		if err == nil {
			if len(r.Events) > limit {
				return nil, errors.New("FlameDB returned more events than the requested page limit")
			}
			if len(r.Events) > remaining {
				return nil, fmt.Errorf("%s exceeds the scan limit (%d events); add filters or increase Scan limit per metric in the datasource settings (maximum 200000)", metric, d.maxScan)
			}
			split = len(r.Events) == limit
		}
		if split {
			mid := w.from.Add(w.to.Sub(w.from) / 2)
			if !mid.After(w.from) || !mid.Before(w.to) {
				if err != nil {
					return nil, fmt.Errorf("%s has an oversized response at one timestamp; add tag filters: %w", metric, err)
				}
				return nil, fmt.Errorf("%s exceeds the scan limit (%d events)", metric, d.maxScan)
			}
			windows = append(windows, readWindow{mid, w.to}, readWindow{w.from, mid})
			continue
		}
		for _, e := range r.Events {
			if e.Timestamp < w.from.UnixNano() || e.Timestamp >= w.to.UnixNano() {
				return nil, errors.New("FlameDB returned an event outside the requested time window")
			}
		}
		events = append(events, r.Events...)
	}
	return events, nil
}
