package analytics

import (
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/jackc/pgx/v5"
)

type timestampAllocation struct {
	metric, encoded string
	count           int
	candidate       int64
	allocated       []int64
	reserved        map[int64]bool
}

func timestampClasses(states []*timestampAllocation) [2][]*timestampAllocation {
	classes := [2][]*timestampAllocation{}
	for _, state := range states {
		index := 0
		if len(state.encoded) > indexTextBudget {
			index = 1
		}
		classes[index] = append(classes[index], state)
	}
	return classes
}

// Callers hold exclusive metric transaction locks. Three transport phases and
// at most two statements per phase cover all ordinary/large metric identities.
// Occupied positions are fetched in bounded pages; no clock is persisted until
// every metric has a complete allocation. Explicit writes never move a clock.
func (e *Engine) allocateBatchTimestamps(ctx context.Context, tx pgx.Tx, counts map[string]int, reserved map[string]map[int64]bool) (map[string][]int64, error) {
	return e.allocateBatchTimestampsWithClocks(ctx, tx, counts, reserved, nil)
}

func queueTimestampClocks(batch *pgx.Batch, states []*timestampAllocation) int {
	count := 0
	for index, class := range timestampClasses(states) {
		if len(class) == 0 {
			continue
		}
		encoded := make([]string, len(class))
		for i, state := range class {
			encoded[i] = state.encoded
		}
		lookup := `SELECT metric,last_ns FROM flame_metric_clock WHERE metric=ANY($1::text[]) AND octet_length(metric)<=2000`
		if index == 1 {
			lookup = `SELECT metric,last_ns FROM flame_metric_clock WHERE metric=ANY($1::text[]) AND flame_identity_hash(metric)=ANY(ARRAY(SELECT flame_identity_hash(name) FROM unnest($1::text[]) t(name)))`
		}
		batch.Queue(lookup, encoded)
		count++
	}
	return count
}

func scanTimestampClocks(results pgx.BatchResults, count int) (map[string]int64, error) {
	lastValues := map[string]int64{}
	for range count {
		rows, err := results.Query()
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var name string
			var last int64
			if err = rows.Scan(&name, &last); err != nil {
				rows.Close()
				return nil, err
			}
			lastValues[name] = last
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return lastValues, nil
}

func (e *Engine) allocateBatchTimestampsWithClocks(ctx context.Context, tx pgx.Tx, counts map[string]int, reserved map[string]map[int64]bool, lastValues map[string]int64) (map[string][]int64, error) {
	allocated := make(map[string][]int64, len(counts))
	names := make([]string, 0, len(counts))
	for name, count := range counts {
		if count > 0 {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	if len(names) == 0 {
		return allocated, nil
	}
	states := make([]*timestampAllocation, 0, len(names))
	for _, name := range names {
		states = append(states, &timestampAllocation{metric: name, encoded: EncodeString(name), count: counts[name], reserved: reserved[name]})
	}
	classes := timestampClasses(states)
	var results pgx.BatchResults
	if lastValues == nil {
		clocks := &pgx.Batch{}
		count := queueTimestampClocks(clocks, states)
		results = tx.SendBatch(ctx, clocks)
		var err error
		lastValues, err = scanTimestampClocks(results, count)
		if err != nil {
			results.Close()
			return nil, err
		}
		if err = results.Close(); err != nil {
			return nil, err
		}
	}
	for _, state := range states {
		last := lastValues[state.encoded]
		if last >= math.MaxInt64-1 {
			return nil, fmt.Errorf("automatic timestamp space exhausted")
		}
		state.candidate = e.nextTimestamp()
		if state.candidate <= last {
			state.candidate = last + 1
		}
		if state.candidate <= 0 {
			return nil, fmt.Errorf("invalid automatic timestamp clock")
		}
		state.allocated = make([]int64, 0, state.count)
	}
	remaining := states
	for len(remaining) > 0 {
		// Across one page, buffer at most max(1024,32*metric_count) positions.
		// A single densely occupied metric retains the original 1000-row page.
		limit := min(1000, max(32, 1024/len(remaining)))
		ends := make(map[string]int64, len(remaining))
		pages := &pgx.Batch{}
		for index, class := range timestampClasses(remaining) {
			if len(class) == 0 {
				continue
			}
			encoded := make([]string, len(class))
			starts := make([]int64, len(class))
			bounds := make([]int64, len(class))
			for i, state := range class {
				span := max(int64(1000), int64(state.count-len(state.allocated))+int64(len(state.reserved)))
				bound := int64(math.MaxInt64)
				if span < math.MaxInt64-state.candidate {
					bound = state.candidate + span
				}
				encoded[i] = state.encoded
				starts[i] = state.candidate
				bounds[i] = bound
				ends[state.encoded] = bound
			}
			predicate := `metric=t.metric AND octet_length(metric)<=2000`
			if index == 1 {
				predicate = `flame_identity_hash(metric)=flame_identity_hash(t.metric) AND metric=t.metric`
			}
			if len(remaining) == 1 {
				predicate = `metric=$1 AND octet_length(metric)<=2000`
				if index == 1 {
					predicate = `flame_identity_hash(metric)=flame_identity_hash($1) AND metric=$1`
				}
				pages.Queue(`SELECT $1::text AS metric,ts FROM flame_events WHERE `+predicate+` AND ts >= $2 AND ts < $3 ORDER BY ts LIMIT $4`, encoded[0], starts[0], bounds[0], limit)
				continue
			}
			pages.Queue(`SELECT t.metric,o.ts FROM unnest($1::text[],$2::bigint[],$3::bigint[]) t(metric,start_ns,end_ns) CROSS JOIN LATERAL (SELECT ts FROM flame_events WHERE `+predicate+` AND ts>=t.start_ns AND ts < t.end_ns ORDER BY ts LIMIT $4) o ORDER BY t.metric,o.ts`, encoded, starts, bounds, limit)
		}
		results = tx.SendBatch(ctx, pages)
		occupied := make(map[string][]int64, len(remaining))
		for range pages.Len() {
			rows, err := results.Query()
			if err != nil {
				results.Close()
				return nil, err
			}
			for rows.Next() {
				var name string
				var timestamp int64
				if err = rows.Scan(&name, &timestamp); err != nil {
					rows.Close()
					results.Close()
					return nil, err
				}
				occupied[name] = append(occupied[name], timestamp)
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				results.Close()
				return nil, err
			}
		}
		if err := results.Close(); err != nil {
			return nil, err
		}
		pending := make([]*timestampAllocation, 0, len(remaining))
		for _, state := range remaining {
			positions := occupied[state.encoded]
			position := 0
			for len(state.allocated) < state.count && state.candidate < ends[state.encoded] {
				for position < len(positions) && positions[position] < state.candidate {
					position++
				}
				// A full page may have adjacent occupied positions beyond its end.
				if position == len(positions) && len(positions) == limit {
					break
				}
				if !state.reserved[state.candidate] && (position == len(positions) || positions[position] != state.candidate) {
					state.allocated = append(state.allocated, state.candidate)
				}
				state.candidate++
			}
			if len(state.allocated) < state.count {
				if state.candidate >= math.MaxInt64 {
					return nil, fmt.Errorf("automatic timestamp space exhausted")
				}
				pending = append(pending, state)
			}
		}
		remaining = pending
	}
	updates := &pgx.Batch{}
	for index, class := range classes {
		if len(class) == 0 {
			continue
		}
		encoded := make([]string, len(class))
		lasts := make([]int64, len(class))
		for i, state := range class {
			encoded[i] = state.encoded
			lasts[i] = state.allocated[len(state.allocated)-1]
			allocated[state.metric] = state.allocated
		}
		conflict := `(metric) WHERE octet_length(metric)<=2000`
		if index == 1 {
			conflict = `(row_id)`
		}
		updates.Queue(`INSERT INTO flame_metric_clock(metric,last_ns) SELECT metric,last_ns FROM unnest($1::text[],$2::bigint[]) t(metric,last_ns) ORDER BY metric ON CONFLICT `+conflict+` DO UPDATE SET last_ns=excluded.last_ns`, encoded, lasts)
	}
	results = tx.SendBatch(ctx, updates)
	for range updates.Len() {
		if _, err := results.Exec(); err != nil {
			results.Close()
			return nil, err
		}
	}
	if err := results.Close(); err != nil {
		return nil, err
	}
	return allocated, nil
}
