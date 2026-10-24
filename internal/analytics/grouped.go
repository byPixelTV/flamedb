package analytics

import (
	"context"
	"encoding/json"
	"fmt"
	"math"

	"github.com/byPixelTV/flamedb/internal/query"
	"github.com/byPixelTV/flamedb/internal/storage"
)

func (e *Engine) grouped(ctx context.Context, q *query.Query) (*query.Result, error) {
	limits := query.DefaultGroupedLimits()
	if e.max < limits.Points {
		limits.Points = e.max
	}
	if e.max < limits.Series {
		limits.Series = e.max
	}
	if e.max < limits.Latest {
		limits.Latest = e.max
	}
	if e.maxBytes < limits.Bytes {
		limits.Bytes = e.maxBytes
	}
	return query.AggregateGrouped(q, limits, func(visit func(storage.Event) error) error {
		to := q.To
		if to == 0 {
			to = math.MaxInt64
		}
		tags, _ := json.Marshal(EncodeTags(q.Where))
		if q.Where == nil {
			tags = []byte(`{}`)
		}
		rows, err := e.db.Query(ctx, `SELECT ts,value,tags FROM flame_events WHERE flame_identity_hash(metric)=flame_identity_hash($1) AND metric=$1 AND ts >= $2 AND ts < $3 AND tags @> $4::jsonb ORDER BY ts`, EncodeString(q.Metric), q.From, to, string(tags))
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var event storage.Event
			var raw []byte
			if err := rows.Scan(&event.Timestamp, &event.Value, &raw); err != nil {
				return err
			}
			if len(raw) > e.maxBytes {
				return fmt.Errorf("one event exceeds configured byte limit")
			}
			if err := json.Unmarshal(raw, &event.Tags); err != nil {
				return err
			}
			event.Tags, err = decodeTags(event.Tags)
			if err != nil {
				return err
			}
			event.Metric = q.Metric
			if err := visit(event); err != nil {
				return err
			}
		}
		return rows.Err()
	})
}
