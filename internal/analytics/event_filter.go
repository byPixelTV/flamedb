package analytics

import (
	"context"
	_ "embed"
	"encoding/json"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"
)

//go:embed event_filter.sql
var eventFilterSchema string

// Cached cardinalities select an index anchor only; they never cache an event,
// aggregate or freshness decision. Stale estimates can affect cost, not results.
type filterPlanner struct {
	mu      sync.Mutex
	metrics map[string]filterEstimate
}
type filterEstimate struct {
	until  time.Time
	counts map[string]int64
}

func (e *Engine) filterAnchor(ctx context.Context, metric string, tags map[string]string) string {
	keys := make([]string, 0, len(tags))
	for key := range tags {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) < 2 {
		return keys[0]
	}
	p := &e.filters
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.metrics == nil {
		p.metrics = make(map[string]filterEstimate)
	}
	estimate, ok := p.metrics[metric]
	if !ok || time.Now().After(estimate.until) {
		estimate = filterEstimate{until: time.Now().Add(time.Minute), counts: make(map[string]int64)}
		rows, err := e.db.Query(ctx, `SELECT tag,count(*) FROM flame_tag_values WHERE flame_identity_hash(metric)=flame_identity_hash($1) AND metric=$1 GROUP BY tag ORDER BY tag LIMIT 64`, metric)
		if err == nil {
			bytes := len(metric)
			for rows.Next() {
				var key string
				var count int64
				if err = rows.Scan(&key, &count); err != nil {
					break
				}
				bytes += len(key) + 24
				if bytes <= 64<<10 {
					estimate.counts[key] = count
				}
			}
			if err == nil {
				err = rows.Err()
			}
			rows.Close()
		}
		if err != nil {
			return keys[0]
		}
		if len(metric) <= 8192 {
			// At most 64 metrics, each at most 64 KiB including tag names.
			if len(p.metrics) >= 64 {
				for old := range p.metrics {
					delete(p.metrics, old)
					break
				}
			}
			p.metrics[metric] = estimate
		}
	}
	best := keys[0]
	for _, key := range keys[1:] {
		if estimate.counts[key] > estimate.counts[best] {
			best = key
		}
	}
	return best
}

func (e *Engine) aggregateCandidates(ctx context.Context, metric string, where map[string]string, from, to int64) (string, []any) {
	encoded := EncodeTags(where)
	raw, _ := json.Marshal(encoded)
	if len(encoded) == 0 {
		return `flame_events WHERE flame_identity_hash(metric)=flame_identity_hash($1) AND metric=$1 AND ts >= $2 AND ts < $3`, []any{EncodeString(metric), from, to}
	}
	name := EncodeString(metric)
	anchor := e.filterAnchor(ctx, name, encoded)
	// Generic plans estimate an unknown timestamp interval as highly selective.
	// They can then intersect the tag bitmap with a full-history timestamp scan.
	// Omit unbounded candidate ranges; render bounded int64 endpoints as safe
	// literals so chunk pruning and index costs use the actual range. Retain the
	// exact parameterized boundaries on the outer residual check in all cases.
	bounds := ""
	if from != 0 {
		bounds += " AND ts >= " + strconv.FormatInt(from, 10)
	}
	if to != math.MaxInt64 {
		bounds += " AND ts < " + strconv.FormatInt(to, 10)
	}
	// Materialization deliberately separates the selective index lookup from
	// exact residual checks. A prepared generic plan cannot scan an entire metric
	// or intersect the anchor with a ubiquitous second tag's posting list.
	return `(WITH candidates AS MATERIALIZED (SELECT metric,ts,value,tags FROM flame_events WHERE tag_tokens @> ARRAY[md5(jsonb_build_array($1::text,$5::text,$6::text)::text)]` + bounds + `) SELECT ts,value FROM candidates WHERE metric=$1 AND ts >= $2 AND ts < $3 AND tags @> $4::jsonb) matched`, []any{name, from, to, string(raw), anchor, encoded[anchor]}
}
