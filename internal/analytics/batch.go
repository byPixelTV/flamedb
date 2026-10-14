package analytics

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/byPixelTV/flamedb/internal/query"
	"github.com/jackc/pgx/v5"
)

type scoreKey struct{ metric, board, entity string }

// copyWrites validates each mutation against staged scores in input order. Bad
// items leave neither an event nor an increment, while accepted items share one
// synchronous transaction. A commit error is returned without retrying it.
func (e *Engine) copyWrites(ctx context.Context, items []*query.Query) (map[int]error, error) {
	return e.copyWritesWithPublisher(ctx, items, nil)
}

func (e *Engine) copyWritesWithPublisher(ctx context.Context, items []*query.Query, publish func(string, int64, []scoreChange)) (map[int]error, error) {
	rejected := map[int]error{}
	snapshot, tx, err := e.beginIngest(ctx, items)
	if err != nil {
		return rejected, err
	}
	defer tx.Rollback(ctx)
	versions, current, initial := snapshot.versions, snapshot.current, snapshot.initial
	accepted := make([]*query.Query, 0, len(items))
	changed := map[scoreKey]bool{}
	for i, q := range items {
		proposed := map[scoreKey]float64{}
		for board, entity := range boards(q) {
			key := scoreKey{q.Metric, board, entity}
			value := current[key] + q.Value
			if math.IsInf(value, 0) || math.IsNaN(value) {
				rejected[i] = fmt.Errorf("leaderboard value must be finite")
				break
			}
			if value == 0 {
				value = 0
			}
			proposed[key] = value
		}
		if rejected[i] != nil {
			continue
		}
		for key, value := range proposed {
			current[key] = value
			changed[key] = true
		}
		accepted = append(accepted, q)
	}
	if len(accepted) == 0 {
		return rejected, nil
	}
	// Reserve auto timestamps only after validation, while the shared metric
	// locks are held. Explicit timestamps preserve overwrite semantics and
	// reserve their positions for this batch without advancing the clock.
	autoCounts := map[string]int{}
	explicit := map[string]map[int64]bool{}
	unaliased := true
	seenQueries := make(map[*query.Query]bool, len(accepted))
	for _, q := range accepted {
		if seenQueries[q] {
			unaliased = false
		}
		seenQueries[q] = true
		if q.Timestamp == 0 {
			autoCounts[q.Metric]++
			continue
		}
		if explicit[q.Metric] == nil {
			explicit[q.Metric] = map[int64]bool{}
		}
		explicit[q.Metric][q.Timestamp] = true
	}
	allocated, err := e.allocateBatchTimestampsWithClocks(ctx, tx, autoCounts, explicit, snapshot.clocks)
	if err != nil {
		return rejected, err
	}
	positions := map[string]int{}
	for _, q := range accepted {
		if q.Timestamp == 0 {
			q.Timestamp = allocated[q.Metric][positions[q.Metric]]
			positions[q.Metric]++
		}
	}
	finalKeys := make([]scoreKey, 0, len(changed))
	for key := range changed {
		finalKeys = append(finalKeys, key)
	}
	if len(explicit) == 0 && unaliased {
		err = e.copyAutomaticRows(ctx, tx, accepted, finalKeys, current)
	} else {
		// Keep each connection's staging relations and clear their rows at commit.
		// Repeated COPY requests then avoid create/drop churn in PostgreSQL catalogs.
		staging := `SET LOCAL synchronous_commit=on; CREATE TEMP TABLE IF NOT EXISTS flame_ingest(ord bigint,metric text,ts bigint,value double precision,tags jsonb) ON COMMIT DELETE ROWS`
		if len(changed) > 0 {
			staging += `; CREATE TEMP TABLE IF NOT EXISTS flame_score_ingest(metric text,board text,entity text,value double precision) ON COMMIT DELETE ROWS`
		}
		if !snapshot.staged {
			if _, err = tx.Exec(ctx, staging); err != nil {
				return rejected, err
			}
		}
		_, err = tx.CopyFrom(ctx, pgx.Identifier{"flame_ingest"}, []string{"ord", "metric", "ts", "value", "tags"}, pgx.CopyFromSlice(len(accepted), func(i int) ([]any, error) {
			q := accepted[i]
			return []any{int64(i), EncodeString(q.Metric), q.Timestamp, q.Value, EncodeTags(q.Tags)}, nil
		}))
		if err != nil {
			return rejected, err
		}
		if len(finalKeys) > 0 {
			_, err = tx.CopyFrom(ctx, pgx.Identifier{"flame_score_ingest"}, []string{"metric", "board", "entity", "value"}, pgx.CopyFromSlice(len(finalKeys), func(i int) ([]any, error) {
				key := finalKeys[i]
				return []any{EncodeString(key.metric), EncodeString(key.board), EncodeString(key.entity), current[key]}, nil
			}))
			if err != nil {
				return rejected, err
			}
		}
		_, err = tx.Exec(ctx, ingestionSQL(accepted, finalKeys))
	}
	if err != nil {
		return rejected, err
	}
	if err = tx.Commit(ctx); err != nil {
		return rejected, err
	}
	projections := map[string][]scoreChange{}
	for _, key := range finalKeys {
		value := current[key]
		projections[key.metric] = append(projections[key.metric], scoreChange{board: key.board, entity: key.entity, old: initial[key], new: &value})
	}
	// One shared budget covers every metric after the durable commit, rather
	// than granting each unavailable projection its own full Redis timeout.
	if len(projections) == 0 {
		return rejected, nil
	}
	if publish != nil {
		for name, changes := range projections {
			publish(name, versions[name], changes)
		}
		return rejected, nil
	}
	cacheCtx, cacheCancel := e.cacheAttempt(ctx)
	defer cacheCancel()
	publications := make([]committedProjection, 0, len(projections))
	for name, changes := range projections {
		publications = append(publications, committedProjection{name, versions[name], changes})
	}
	e.projectManyWithin(cacheCtx, publications)
	return rejected, nil
}

// Select only populated identity classes. Ordinary batches must not scan the
// staging rows/JSON tags again for empty large-key and leaderboard branches.
// Classification uses the encoded byte lengths used by the SQL indexes; the
// fixed statements retain the exact conflict targets for mixed and large keys.
func ingestionSQL(items []*query.Query, scores []scoreKey) string {
	var smallEvents, largeEvents, smallTags, largeTags, smallScores, largeScores bool
	for _, q := range items {
		metricBytes := len(EncodeString(q.Metric))
		if metricBytes <= indexTextBudget {
			smallEvents = true
		} else {
			largeEvents = true
		}
		for key, value := range q.Tags {
			if metricBytes+len(EncodeString(key))+len(EncodeString(value)) <= indexTextBudget {
				smallTags = true
			} else {
				largeTags = true
			}
		}
	}
	for _, key := range scores {
		if len(EncodeString(key.metric))+len(EncodeString(key.board))+len(EncodeString(key.entity)) <= indexTextBudget {
			smallScores = true
		} else {
			largeScores = true
		}
	}
	statements := make([]string, 0, 8)
	if smallEvents {
		statements = append(statements, `INSERT INTO flame_events(metric,ts,value,tags) SELECT metric,ts,value,tags FROM (SELECT DISTINCT ON(metric,ts) metric,ts,value,tags FROM flame_ingest WHERE octet_length(metric)<=2000 ORDER BY metric,ts,ord DESC) chosen ON CONFLICT(metric,ts) WHERE octet_length(metric)<=2000 DO UPDATE SET value=excluded.value,tags=excluded.tags`)
	}
	if largeEvents {
		statements = append(statements, `INSERT INTO flame_events(metric,ts,value,tags) SELECT metric,ts,value,tags FROM (SELECT DISTINCT ON(metric,ts) metric,ts,value,tags FROM flame_ingest WHERE octet_length(metric)>2000 ORDER BY metric,ts,ord DESC) chosen ON CONFLICT(row_id,ts) DO UPDATE SET value=excluded.value,tags=excluded.tags`)
	}
	if smallTags {
		statements = append(statements, `INSERT INTO flame_tag_values(metric,tag,value) SELECT DISTINCT metric,t.key,t.value FROM flame_ingest CROSS JOIN LATERAL jsonb_each_text(tags) t WHERE octet_length(metric)+octet_length(t.key)+octet_length(t.value)<=2000 ORDER BY metric,t.key,t.value ON CONFLICT(metric,tag,value) WHERE octet_length(metric)+octet_length(tag)+octet_length(value)<=2000 DO NOTHING`)
	}
	if largeTags {
		statements = append(statements, `INSERT INTO flame_tag_values(metric,tag,value) SELECT DISTINCT metric,t.key,t.value FROM flame_ingest CROSS JOIN LATERAL jsonb_each_text(tags) t WHERE octet_length(metric)+octet_length(t.key)+octet_length(t.value)>2000 ORDER BY metric,t.key,t.value ON CONFLICT(row_id) DO NOTHING`)
	}
	if smallScores {
		statements = append(statements, `INSERT INTO flame_scores(metric,board,entity,value) SELECT metric,board,entity,value FROM flame_score_ingest WHERE octet_length(metric)+octet_length(board)+octet_length(entity)<=2000 ON CONFLICT(metric,board,entity) WHERE octet_length(metric)+octet_length(board)+octet_length(entity)<=2000 DO UPDATE SET value=excluded.value`)
	}
	if largeScores {
		statements = append(statements, `INSERT INTO flame_scores(metric,board,entity,value) SELECT metric,board,entity,value FROM flame_score_ingest WHERE octet_length(metric)+octet_length(board)+octet_length(entity)>2000 ON CONFLICT(row_id) DO UPDATE SET value=excluded.value`)
	}
	statements = append(statements, `INSERT INTO flame_metrics(metric,version) SELECT DISTINCT metric,0 FROM flame_ingest ORDER BY metric ON CONFLICT DO NOTHING`)
	if len(scores) > 0 {
		statements = append(statements, `UPDATE flame_metrics SET version=version+1 WHERE metric IN(SELECT DISTINCT metric FROM flame_score_ingest)`)
	}
	return strings.Join(statements, ";")
}
