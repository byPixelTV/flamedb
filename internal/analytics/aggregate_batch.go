package analytics

import (
	"context"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/byPixelTV/flamedb/internal/query"
	"github.com/jackc/pgx/v5"
)

// Share candidate scans across concurrent historical reads, not their results.
// The statement snapshot is acquired after every participating request arrived;
// an acknowledged write before a request therefore remains visible to it.
type aggregateReader struct {
	e      *Engine
	ctx    context.Context
	cancel context.CancelFunc
	queue  chan *aggregateRead
	done   chan struct{}
	once   sync.Once
}
type aggregateRead struct {
	ctx                                  context.Context
	metric, encoded, key, value, filters string
	q                                    query.Query
	result                               chan aggregateReply
}
type aggregateReply struct {
	value *query.AggregateResult
	err   error
}

func newAggregateReader(e *Engine) *aggregateReader {
	ctx, cancel := context.WithCancel(context.Background())
	r := &aggregateReader{e: e, ctx: ctx, cancel: cancel, queue: make(chan *aggregateRead, 128), done: make(chan struct{})}
	go r.run()
	return r
}
func (r *aggregateReader) close() { r.once.Do(r.cancel); <-r.done }
func (r *aggregateReader) submit(ctx context.Context, q *query.Query, metric string) (*query.AggregateResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := r.ctx.Err(); err != nil {
		return nil, err
	}
	where := make(map[string]string, len(q.Where))
	for k, v := range q.Where {
		where[strings.Clone(k)] = strings.Clone(v)
	}
	encoded := EncodeTags(where)
	raw, _ := json.Marshal(encoded)
	if len(raw)+len(metric) > 64<<10 {
		return r.e.aggregateDirect(ctx, q, metric)
	}
	metric = strings.Clone(metric)
	name := EncodeString(metric)
	key := r.e.filterAnchor(ctx, name, encoded)
	item := &aggregateRead{ctx: ctx, metric: metric, encoded: name, key: key, value: encoded[key], filters: string(raw), q: query.Query{Aggregate: q.Aggregate, Where: where, From: q.From, To: q.To}, result: make(chan aggregateReply, 1)}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.ctx.Done():
		return nil, r.ctx.Err()
	case r.queue <- item:
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.done:
		return nil, context.Canceled
	case result := <-item.result:
		return result.value, result.err
	}
}
func (r *aggregateReader) run() {
	defer close(r.done)
	waves := make(chan []*aggregateRead)
	var workers sync.WaitGroup
	for range min(4, int(r.e.db.Config().MaxConns)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for wave := range waves {
				r.execute(wave)
			}
		}()
	}
	defer func() {
		close(waves)
		workers.Wait()
		for {
			select {
			case item := <-r.queue:
				item.result <- aggregateReply{err: context.Canceled}
			default:
				return
			}
		}
	}()
	for {
		var first *aggregateRead
		select {
		case <-r.ctx.Done():
			return
		case first = <-r.queue:
		}
		wave := []*aggregateRead{first}
		// A short collection window amortizes the database round trip across
		// concurrent requests; full waves leave immediately.
		timer := time.NewTimer(100 * time.Microsecond)
	collect:
		for len(wave) < 32 {
			select {
			case <-r.ctx.Done():
				break collect
			case <-timer.C:
				break collect
			case item := <-r.queue:
				wave = append(wave, item)
			}
		}
		timer.Stop()
		select {
		case waves <- wave:
		case <-r.ctx.Done():
			for _, item := range wave {
				item.result <- aggregateReply{err: context.Canceled}
			}
			return
		}
	}
}
func (r *aggregateReader) execute(wave []*aggregateRead) {
	groups := make(map[[2]string][]*aggregateRead)
	for _, item := range wave {
		if err := item.ctx.Err(); err != nil {
			item.result <- aggregateReply{err: err}
		} else {
			key := [2]string{item.encoded, item.key}
			groups[key] = append(groups[key], item)
		}
	}
	for _, items := range groups {
		if len(items) == 1 {
			r.direct(items[0])
			continue
		}
		r.group(items)
	}
}
func (r *aggregateReader) direct(item *aggregateRead) {
	if err := item.ctx.Err(); err != nil {
		item.result <- aggregateReply{err: err}
		return
	}
	ctx, cancel := context.WithTimeout(r.ctx, r.e.timeout)
	defer cancel()
	stop := context.AfterFunc(item.ctx, cancel)
	defer stop()
	value, err := r.e.aggregateDirect(ctx, &item.q, item.metric)
	if callerErr := item.ctx.Err(); callerErr != nil {
		err = callerErr
		value = nil
	}
	item.result <- aggregateReply{value, err}
}
func (r *aggregateReader) group(items []*aggregateRead) {
	deadline := time.Now().Add(r.e.timeout)
	values, filters, froms, tos, counts := []string{}, []string{}, []int64{}, []int64{}, []bool{}
	lower, upper := int64(math.MaxInt64), int64(0)
	for _, item := range items {
		to := item.q.To
		if to == 0 {
			to = math.MaxInt64
		}
		lower = min(lower, item.q.From)
		upper = max(upper, to)
		values = append(values, item.value)
		filters = append(filters, item.filters)
		froms = append(froms, item.q.From)
		tos = append(tos, to)
		counts = append(counts, item.q.Aggregate == query.AggCount)
		if until, ok := item.ctx.Deadline(); ok && until.Before(deadline) {
			deadline = until
		}
	}
	ctx, cancel := context.WithDeadline(r.ctx, deadline)
	var live atomic.Int32
	live.Store(int32(len(items)))
	stops := make([]func() bool, 0, len(items))
	for _, item := range items {
		stops = append(stops, context.AfterFunc(item.ctx, func() {
			if live.Add(-1) == 0 {
				cancel()
			}
		}))
	}
	defer func() {
		for _, stop := range stops {
			stop()
		}
		cancel()
	}()
	bounds := ""
	if lower != 0 {
		bounds += " AND ts >= " + strconv.FormatInt(lower, 10)
	}
	if upper != math.MaxInt64 {
		bounds += " AND ts < " + strconv.FormatInt(upper, 10)
	}
	// Match the anchor as a JSON string, rather than coercing JSON numbers to
	// text. The residual predicate then checks only the other filter keys.
	sql := `WITH requests AS MATERIALIZED (SELECT value,to_jsonb(value) AS anchor,filters-$2::text AS remaining,lo,hi,count_only,ordinal FROM unnest($3::text[],$4::jsonb[],$5::bigint[],$6::bigint[],$7::boolean[]) WITH ORDINALITY t(value,filters,lo,hi,count_only,ordinal)), candidates AS MATERIALIZED (SELECT ts,value,tags FROM flame_events WHERE tag_tokens && ARRAY(SELECT md5(jsonb_build_array($1::text,$2::text,value)::text) FROM requests) AND metric=$1` + bounds + `) SELECT r.ordinal,count(c.ts),COALESCE(sum(CASE WHEN r.count_only THEN 0::double precision ELSE c.value END ORDER BY c.ts),0) FROM requests r LEFT JOIN candidates c ON c.tags->$2=r.anchor AND c.tags @> r.remaining AND c.ts>=r.lo AND c.ts<r.hi GROUP BY r.ordinal,r.count_only ORDER BY r.ordinal`
	batch := &pgx.Batch{}
	batch.Queue(`SET LOCAL plan_cache_mode=force_generic_plan`)
	batch.Queue(sql, items[0].encoded, items[0].key, values, filters, froms, tos, counts)
	results := r.e.db.SendBatch(ctx, batch)
	_, err := results.Exec()
	answers := make([]*query.AggregateResult, len(items))
	if err == nil {
		rows, queryErr := results.Query()
		err = queryErr
		if err == nil {
			for rows.Next() {
				var index int
				var count int
				var value float64
				if err = rows.Scan(&index, &count, &value); err != nil {
					break
				}
				if index < 1 || index > len(items) {
					err = pgx.ErrTooManyRows
					break
				}
				item := items[index-1]
				if item.q.Aggregate == query.AggCount {
					value = float64(count)
				} else if item.q.Aggregate == query.AggAvg && count > 0 {
					value /= float64(count)
				}
				if value == 0 {
					value = 0
				}
				answers[index-1] = &query.AggregateResult{Type: string(item.q.Aggregate), Count: count, Value: value}
			}
			if err == nil {
				err = rows.Err()
			}
			rows.Close()
		}
	}
	if closeErr := results.Close(); err == nil {
		err = closeErr
	}
	for _, answer := range answers {
		if answer == nil && err == nil {
			err = pgx.ErrNoRows
		}
		if answer != nil && (math.IsNaN(answer.Value) || math.IsInf(answer.Value, 0)) {
			err = pgx.ErrTooManyRows
		}
	}
	if err != nil {
		// An overflow or canceled participant must not poison other independent
		// reads. Read-only retries are safe and obtain a later valid snapshot.
		for _, item := range items {
			r.direct(item)
		}
		return
	}
	for i, item := range items {
		item.result <- aggregateReply{value: answers[i]}
	}
}
