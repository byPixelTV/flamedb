package analytics

import (
	"context"
	"hash/fnv"
	"sort"
	"sync"

	"github.com/byPixelTV/flamedb/internal/query"
)

type metricWrites struct {
	items   []*query.Query
	indices []int
}

type committedProjection struct {
	metric   string
	previous int64
	changes  []scoreChange
}

// Explicit raw-only batches share metric locks and benefit from one large COPY.
// Automatic timestamps and scores need exclusive locks and independent metric
// transactions: holding every
// metric lock at once serializes otherwise independent multi-metric requests.
// Within a metric, input order, rejection and event/score atomicity stay intact.
// Across metrics, WRITE_BATCH retains its public partial-success contract.
func (e *Engine) copyOrdinaryWrites(ctx context.Context, items []*query.Query) (map[int]error, error) {
	if e.ingest != nil && len(items) < min(256, e.batchSize) {
		for _, q := range items {
			if q.UpdateLB || len(q.LBEntities) > 0 {
				return e.ingest.submit(ctx, items)
			}
		}
	}
	return e.copyGroupedWrites(ctx, items)
}

func (e *Engine) copyGroupedWrites(ctx context.Context, items []*query.Query) (map[int]error, error) {
	hasScores, hasAutomatic, multipleMetrics := false, false, false
	for _, q := range items {
		hasScores = hasScores || q.UpdateLB || len(q.LBEntities) > 0
		hasAutomatic = hasAutomatic || q.Timestamp == 0
		multipleMetrics = multipleMetrics || q.Metric != items[0].Metric
	}
	if (!hasScores && !hasAutomatic) || !multipleMetrics {
		return e.copyWrites(ctx, items)
	}
	groups := map[string]*metricWrites{}
	for i, q := range items {
		group := groups[q.Metric]
		if group == nil {
			group = &metricWrites{}
			groups[q.Metric] = group
		}
		group.items = append(group.items, q)
		group.indices = append(group.indices, i)
	}
	metricCount := len(groups)
	if len(items) < 64 {
		// Tiny waves of single WRITE callers still need transaction amortization.
		return e.copyWrites(ctx, items)
	}
	// Sparse requests use four deterministic stripes: avoid both one transaction
	// locking every metric and sixteen tiny transactions. A metric's writes stay
	// in one stripe and in input order; locks within each transaction stay sorted.
	density := len(items) / len(groups)
	stripeCount := 0
	if !hasScores || density < 32 || (hasAutomatic && density < 64) {
		stripeCount = 4
	}
	if hasScores && density == 32 && len(groups) > 8 {
		// Amortize the boundary's 32-item jobs while retaining more than one
		// SQL worker wave and a single shared Redis publication budget.
		stripeCount = 16
	}
	if stripeCount > 0 {
		stripes := make([]*metricWrites, min(stripeCount, len(groups)))
		for i, q := range items {
			h := fnv.New32a()
			_, _ = h.Write([]byte(q.Metric))
			index := int(h.Sum32() % uint32(len(stripes)))
			if stripes[index] == nil {
				stripes[index] = &metricWrites{}
			}
			stripes[index].items = append(stripes[index].items, q)
			stripes[index].indices = append(stripes[index].indices, i)
		}
		groups = map[string]*metricWrites{}
		for _, stripe := range stripes {
			if stripe != nil {
				groups[stripe.items[0].Metric] = stripe
			}
		}
	}
	names := make([]string, 0, len(groups))
	for name := range groups {
		names = append(names, name)
	}
	sort.Strings(names)
	// Queue and workers are bounded by the admitted chunk and connection pool.
	// Every job reports a result even after cancellation; callers never wait for
	// a missing result or silently discard the original batch item positions.
	jobs := make(chan *metricWrites, len(groups))
	results := make(chan map[int]error, len(groups))
	for _, name := range names {
		jobs <- groups[name]
	}
	close(jobs)
	workers := min(8, len(groups), int(e.db.Config().MaxConns))
	// Cache publication overlaps remaining SQL jobs in a separate bounded
	// worker. A stalled Redis cannot occupy a worker needed by the next SQL wave.
	// Each metric belongs to one job and publishes at most one change list.
	// Capacity covers every original metric, including multi-metric stripes;
	// publication cannot block a database worker or grow beyond the chunk.
	publications := make(chan committedProjection, metricCount)
	publish := func(metric string, previous int64, changes []scoreChange) {
		publications <- committedProjection{metric, previous, changes}
	}
	published := make(chan struct{})
	go func() {
		defer close(published)
		var cacheCtx context.Context
		var cancel context.CancelFunc
		var epoch string
		var epochErr error
		defer func() {
			if cancel != nil {
				cancel()
			}
		}()
		for publication := range publications {
			// Only the first durable commit starts the ONE cache budget.
			if cacheCtx == nil {
				cacheCtx, cancel = e.cacheAttempt(ctx)
				if e.cache != nil {
					epoch, epochErr = e.epoch(cacheCtx)
				}
			}
			ready := []committedProjection{publication}
		collect:
			for len(ready) < 32 {
				select {
				case next, ok := <-publications:
					if !ok {
						break collect
					}
					ready = append(ready, next)
				default:
					break collect
				}
			}
			if e.cache != nil && epochErr == nil {
				e.projectBatchWithSource(cacheCtx, epoch, ready, applyProjectionLua)
			}
		}
	}()
	var finished sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		finished.Add(1)
		go func() {
			defer finished.Done()
			for group := range jobs {
				local, err := e.copyWritesWithPublisher(ctx, group.items, publish)
				failed := make(map[int]error, len(local))
				for i, index := range group.indices {
					if rejection := local[i]; rejection != nil {
						failed[index] = rejection
					} else if err != nil {
						failed[index] = err
					}
				}
				results <- failed
			}
		}()
	}
	rejected := map[int]error{}
	for range groups {
		for index, err := range <-results {
			rejected[index] = err
		}
	}
	finished.Wait()
	close(publications)
	<-published
	return rejected, nil
}
