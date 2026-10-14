package analytics

import (
	"context"
	"github.com/jackc/pgx/v5"
)

// The shared allocator keeps single replica/quorum writes and ordinary batches
// on the same collision-safe, bounded lookup path under the metric lock.
func (e *Engine) allocateTimestamps(ctx context.Context, tx pgx.Tx, metric string, count int, reserved map[int64]bool) ([]int64, error) {
	allocated, err := e.allocateBatchTimestamps(ctx, tx, map[string]int{metric: count}, map[string]map[int64]bool{metric: reserved})
	return allocated[metric], err
}
