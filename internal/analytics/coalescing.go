package analytics

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/byPixelTV/flamedb/internal/query"
)

// A finite queue amortizes small WRITE_BATCH transactions across callers. It
// holds at most 32 admitted chunks, one collected wave and two active waves.
// Every wave is batchSize-bounded; two workers allow independent requests to
// progress when a metric in another wave is blocked. Metric workers stay bounded.
// Waiting for the response is mandatory even if a caller cancels after dispatch:
// another caller can share its transaction, and nobody may race a late mutation
// of the Query timestamp or treat admission as a durable acknowledgement.
type writeCoalescer struct {
	e      *Engine
	queue  chan *ingestRequest
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
	merged atomic.Uint64
	active atomic.Int32
}

type ingestRequest struct {
	ctx    context.Context
	items  []*query.Query
	result chan ingestResult
}
type ingestResult struct {
	rejected map[int]error
	err      error
}

func newWriteCoalescer(e *Engine) *writeCoalescer {
	ctx, cancel := context.WithCancel(context.Background())
	c := &writeCoalescer{e: e, queue: make(chan *ingestRequest, 32), ctx: ctx, cancel: cancel, done: make(chan struct{})}
	go c.run()
	return c
}

func (c *writeCoalescer) close() {
	c.once.Do(c.cancel)
	<-c.done
}

func (c *writeCoalescer) submit(ctx context.Context, items []*query.Query) (map[int]error, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r := &ingestRequest{ctx: ctx, items: items, result: make(chan ingestResult, 1)}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.ctx.Done():
		return nil, c.ctx.Err()
	case c.queue <- r:
	}
	// Shutdown may race enqueue. run drains the queue, but a send that wins
	// after that drain still settles here, after all database workers stopped.
	select {
	case result := <-r.result:
		return result.rejected, result.err
	case <-c.done:
		select {
		case result := <-r.result:
			return result.rejected, result.err
		default:
			return nil, context.Canceled
		}
	}
}

func (c *writeCoalescer) run() {
	defer close(c.done)
	type wave struct {
		requests []*ingestRequest
		size     int
	}
	waves := make(chan wave)
	var workers sync.WaitGroup
	for range min(2, int(c.e.db.Config().MaxConns)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for wave := range waves {
				c.execute(wave.requests, wave.size)
			}
		}()
	}
	defer func() { close(waves); workers.Wait() }()
	defer func() {
		for {
			select {
			case r := <-c.queue:
				r.result <- ingestResult{err: context.Canceled}
			default:
				return
			}
		}
	}()
	var carry *ingestRequest
	for {
		first := carry
		carry = nil
		if first == nil {
			select {
			case <-c.ctx.Done():
				return
			case first = <-c.queue:
			}
		}
		if err := first.ctx.Err(); err != nil {
			first.result <- ingestResult{err: err}
			continue
		}
		requests := []*ingestRequest{first}
		size := len(first.items)
		timer := time.NewTimer(time.Millisecond)
	collect:
		for size < c.e.batchSize && len(requests) < 32 {
			select {
			case <-c.ctx.Done():
				break collect
			case <-timer.C:
				break collect
			case r := <-c.queue:
				if err := r.ctx.Err(); err != nil {
					r.result <- ingestResult{err: err}
					continue
				}
				if size+len(r.items) > c.e.batchSize {
					carry = r
					break collect
				}
				requests = append(requests, r)
				size += len(r.items)
			}
		}
		timer.Stop()
		select {
		case waves <- wave{requests, size}:
		case <-c.ctx.Done():
			for _, r := range requests {
				r.result <- ingestResult{err: context.Canceled}
			}
		}
		if c.ctx.Err() != nil {
			if carry != nil {
				carry.result <- ingestResult{err: context.Canceled}
			}
			return
		}
	}
}

func (c *writeCoalescer) execute(requests []*ingestRequest, size int) {
	c.active.Add(1)
	defer c.active.Add(-1)
	deadline := time.Now().Add(c.e.timeout)
	items := make([]*query.Query, 0, size)
	active := requests[:0]
	for _, r := range requests {
		if err := r.ctx.Err(); err != nil {
			r.result <- ingestResult{err: err}
			continue
		}
		if until, ok := r.ctx.Deadline(); ok && until.Before(deadline) {
			deadline = until
		}
		active = append(active, r)
		items = append(items, r.items...)
	}
	if len(active) == 0 {
		return
	}
	if len(active) > 1 {
		c.merged.Add(uint64(len(active)))
	}
	// The earliest caller deadline bounds the shared work. Explicit cancellation
	// of one dispatched caller cannot abort another caller's durable transaction.
	ctx, cancel := context.WithDeadline(c.ctx, deadline)
	defer cancel()
	// Abort immediately when EVERY participant cancels, without allowing one
	// disconnected client to poison the remaining clients' transaction.
	var participants atomic.Int32
	participants.Store(int32(len(active)))
	stops := make([]func() bool, 0, len(active))
	for _, r := range active {
		stops = append(stops, context.AfterFunc(r.ctx, func() {
			if participants.Add(-1) == 0 {
				cancel()
			}
		}))
	}
	defer func() {
		for _, stop := range stops {
			stop()
		}
	}()
	rejected, err := c.e.copyGroupedWrites(ctx, items)
	offset := 0
	for _, r := range active {
		local := map[int]error{}
		for i := range r.items {
			if rejection := rejected[offset+i]; rejection != nil {
				local[i] = rejection
			}
		}
		offset += len(r.items)
		r.result <- ingestResult{rejected: local, err: err}
	}
}

// IngestionStats is bounded telemetry without per-metric cardinality.
func (e *Engine) IngestionStats() (queued, active int, merged uint64) {
	if e.ingest == nil {
		return 0, 0, 0
	}
	return len(e.ingest.queue), int(e.ingest.active.Load()), e.ingest.merged.Load()
}
