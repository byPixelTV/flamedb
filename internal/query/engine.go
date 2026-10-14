package query

import (
	"context"
	"github.com/byPixelTV/flamedb/internal/storage"
)

// Engine preserves the wire-facing query contract across storage implementations.
type Engine interface {
	Execute(*Query) (*Result, error)
	ExecuteBatch([]*Query) (*BatchResult, error)
	ListMetrics(map[string]string) ([]string, error)
	GetAllMetrics() []string
}

type ContextEngine interface {
	ExecuteContext(context.Context, *Query) (*Result, error)
	ExecuteBatchContext(context.Context, []*Query) (*BatchResult, error)
}

// AggregateSeriesUTC uses the frozen protocol's calendar bucket semantics.
func AggregateSeriesUTC(events []storage.Event, spec string, agg AggType) []SeriesPoint {
	return aggregateSeriesUTC(events, spec, agg)
}
