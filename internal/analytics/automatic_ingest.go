package analytics

import (
	"context"
	"encoding/json"
	"sort"

	"github.com/byPixelTV/flamedb/internal/query"
	"github.com/jackc/pgx/v5"
)

type ingestTag struct{ metric, tag, value string }
type ingestIdentities struct{ metrics, names, values []string }
type ingestScores struct {
	metrics, boards, entities []string
	values                    []float64
}

// Automatic positions were reserved under exclusive metric locks and cannot
// collide. COPY durable events once, then upsert deduplicated tags and staged
// absolute scores with typed arrays. No temporary relation is needed. Explicit
// timestamps and aliased Query pointers keep the overwrite-capable path.
func (e *Engine) copyAutomaticRows(ctx context.Context, tx pgx.Tx, items []*query.Query, scores []scoreKey, current map[scoreKey]float64) error {
	tagSet := map[ingestTag]bool{}
	metricSet := map[string]bool{}
	for _, q := range items {
		metric := EncodeString(q.Metric)
		metricSet[metric] = true
		for tag, value := range q.Tags {
			tagSet[ingestTag{metric, EncodeString(tag), EncodeString(value)}] = true
		}
	}
	metrics := make([]string, 0, len(metricSet))
	for metric := range metricSet {
		metrics = append(metrics, metric)
	}
	sort.Strings(metrics)
	tags := [2]ingestIdentities{}
	for tag := range tagSet {
		index := 0
		if len(tag.metric)+len(tag.tag)+len(tag.value) > indexTextBudget {
			index = 1
		}
		t := &tags[index]
		t.metrics = append(t.metrics, tag.metric)
		t.names = append(t.names, tag.tag)
		t.values = append(t.values, tag.value)
	}
	values := [2]ingestScores{}
	scoreMetrics := map[string]bool{}
	smallMetrics := true
	for _, key := range scores {
		metric, board, entity := EncodeString(key.metric), EncodeString(key.board), EncodeString(key.entity)
		index := 0
		if len(metric)+len(board)+len(entity) > indexTextBudget {
			index = 1
		}
		s := &values[index]
		s.metrics = append(s.metrics, metric)
		s.boards = append(s.boards, board)
		s.entities = append(s.entities, entity)
		s.values = append(s.values, current[key])
		scoreMetrics[metric] = true
		smallMetrics = smallMetrics && len(metric) <= indexTextBudget
	}
	changedMetrics := make([]string, 0, len(scoreMetrics))
	for metric := range scoreMetrics {
		changedMetrics = append(changedMetrics, metric)
	}
	sort.Strings(changedMetrics)
	batch := &pgx.Batch{}
	if len(scores) > 0 && len(items) > 32 && (len(items) >= 64 || len(metrics) == 1) {
		// Small per-metric score jobs benefit from a single prepared SQL batch
		// for events, tags, absolute scores and generation, rather than COPY's
		// additional protocol exchanges. JSON strings are encoded as JSONB.
		names := make([]string, len(items))
		timestamps := make([]int64, len(items))
		values := make([]float64, len(items))
		tags := make([]string, len(items))
		for i, q := range items {
			names[i] = EncodeString(q.Metric)
			timestamps[i] = q.Timestamp
			values[i] = q.Value
			encoded, err := json.Marshal(EncodeTags(q.Tags))
			if err != nil {
				return err
			}
			tags[i] = string(encoded)
		}
		batch.Queue(`INSERT INTO flame_events(metric,ts,value,tags) SELECT metric,ts,value,tags FROM unnest($1::text[],$2::bigint[],$3::double precision[],$4::jsonb[]) t(metric,ts,value,tags)`, names, timestamps, values, tags)
	} else {
		if _, err := tx.CopyFrom(ctx, pgx.Identifier{"flame_events"}, []string{"metric", "ts", "value", "tags"}, pgx.CopyFromSlice(len(items), func(i int) ([]any, error) {
			q := items[i]
			return []any{EncodeString(q.Metric), q.Timestamp, q.Value, EncodeTags(q.Tags)}, nil
		})); err != nil {
			return err
		}
	}
	for index, t := range tags {
		if len(t.metrics) == 0 {
			continue
		}
		conflict := `(metric,tag,value) WHERE octet_length(metric)+octet_length(tag)+octet_length(value)<=2000`
		if index == 1 {
			conflict = `(row_id)`
		}
		batch.Queue(`INSERT INTO flame_tag_values(metric,tag,value) SELECT metric,tag,value FROM unnest($1::text[],$2::text[],$3::text[]) t(metric,tag,value) ORDER BY metric,tag,value ON CONFLICT `+conflict+` DO NOTHING`, t.metrics, t.names, t.values)
	}
	for index, s := range values {
		if len(s.metrics) == 0 {
			continue
		}
		conflict := `(metric,board,entity) WHERE octet_length(metric)+octet_length(board)+octet_length(entity)<=2000`
		if index == 1 {
			conflict = `(row_id)`
		}
		batch.Queue(`INSERT INTO flame_scores(metric,board,entity,value) SELECT metric,board,entity,value FROM unnest($1::text[],$2::text[],$3::text[],$4::double precision[]) t(metric,board,entity,value) ON CONFLICT `+conflict+` DO UPDATE SET value=excluded.value`, s.metrics, s.boards, s.entities, s.values)
	}
	batch.Queue(`INSERT INTO flame_metrics(metric,version) SELECT metric,0 FROM unnest($1::text[]) t(metric) ORDER BY metric ON CONFLICT DO NOTHING`, metrics)
	if len(changedMetrics) > 0 {
		lookup := `UPDATE flame_metrics SET version=version+1 WHERE metric=ANY($1::text[]) AND octet_length(metric)<=2000`
		if !smallMetrics {
			lookup = `UPDATE flame_metrics SET version=version+1 WHERE metric=ANY($1::text[]) AND flame_identity_hash(metric)=ANY(ARRAY(SELECT flame_identity_hash(name) FROM unnest($1::text[]) t(name)))`
		}
		batch.Queue(lookup, changedMetrics)
	}
	results := tx.SendBatch(ctx, batch)
	for range batch.Len() {
		if _, err := results.Exec(); err != nil {
			results.Close()
			return err
		}
	}
	return results.Close()
}
