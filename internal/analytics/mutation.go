package analytics

import (
	"context"
	"encoding/json"
	"fmt"
	"math"

	"github.com/byPixelTV/flamedb/internal/query"
	"github.com/jackc/pgx/v5"
)

type scoreChange struct {
	board, entity string
	old, new      *float64
}

func (e *Engine) validateWrite(q *query.Query) error {
	if q.Quorum && e.quorumReplicationFactor == 0 {
		return fmt.Errorf("QUORUM is unsupported by the PostgreSQL backend; configure synchronous PostgreSQL replication instead")
	}
	if math.IsNaN(q.Value) || math.IsInf(q.Value, 0) || q.Timestamp < 0 {
		return fmt.Errorf("invalid write value or timestamp")
	}
	if q.Metric == "" {
		return fmt.Errorf("metric is required")
	}
	return nil
}
func boards(q *query.Query) map[string]string {
	out := make(map[string]string, len(q.LBEntities)+1)
	if q.UpdateLB {
		out[""] = q.LBEntityID
	}
	for b, id := range q.LBEntities {
		out[b] = id
	}
	return out
}
func (e *Engine) mutate(ctx context.Context, q *query.Query) (*query.Result, error) {
	metric := EncodeString(q.Metric)
	if err := e.validateWrite(q); err != nil {
		return nil, err
	}
	if q.Type == query.QueryTypeWrite && !q.Quorum && !q.IsReplica && e.ingest != nil {
		rejected, err := e.ingest.submit(ctx, []*query.Query{q})
		if err != nil {
			return nil, err
		}
		if err = rejected[0]; err != nil {
			return nil, err
		}
		return &query.Result{}, nil
	}
	automatic := q.Type == query.QueryTypeWrite && q.Timestamp == 0
	tx, err := e.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if err = lockMetric(ctx, tx, q.Metric); err != nil {
		return nil, err
	}
	if err = e.configureDurability(ctx, tx, q.Quorum); err != nil {
		return nil, err
	}
	if q.IsReplica && q.OperationID != "" && !q.DryRun {
		tag, receiptErr := tx.Exec(ctx, `INSERT INTO flame_receipts(operation) VALUES($1) ON CONFLICT DO NOTHING`, EncodeString(q.OperationID))
		if receiptErr != nil {
			return nil, receiptErr
		}
		if tag.RowsAffected() == 0 {
			return &query.Result{}, nil
		}
	}
	if automatic {
		timestamps, allocateErr := e.allocateTimestamps(ctx, tx, q.Metric, 1, nil)
		if allocateErr != nil {
			return nil, allocateErr
		}
		q.Timestamp = timestamps[0]
	}
	// Read-only/no-op mutations do not create phantom metric names.
	if q.Type == query.QueryTypeWrite || (q.Type == query.QueryTypeSet && len(boards(q)) > 0) {
		if _, err = tx.Exec(ctx, `INSERT INTO flame_metrics(metric) VALUES($1) ON CONFLICT DO NOTHING`, metric); err != nil {
			return nil, err
		}
	}
	var previous int64
	if len(boards(q)) > 0 {
		if err = tx.QueryRow(ctx, `SELECT version FROM flame_metrics WHERE flame_identity_hash(metric)=flame_identity_hash($1) AND metric=$1`, metric).Scan(&previous); err != nil && err != pgx.ErrNoRows {
			return nil, err
		}
	}
	deleted := &query.DeleteResult{DryRun: q.DryRun}
	changes := []scoreChange{}
	if q.Type == query.QueryTypeWrite {
		tags, encodeErr := json.Marshal(EncodeTags(q.Tags))
		if encodeErr != nil {
			return nil, encodeErr
		}
		if q.Tags == nil {
			tags = []byte(`{}`)
		}
		conflict := `(metric,ts) WHERE octet_length(metric)<=2000`
		if len(metric) > indexTextBudget {
			conflict = `(row_id,ts)`
		}
		if _, err = tx.Exec(ctx, `INSERT INTO flame_events(metric,ts,value,tags) VALUES($1,$2,$3,$4::jsonb) ON CONFLICT `+conflict+` DO UPDATE SET value=excluded.value,tags=excluded.tags`, metric, q.Timestamp, q.Value, string(tags)); err != nil {
			return nil, err
		}
		for k, v := range q.Tags {
			key, value := EncodeString(k), EncodeString(v)
			conflict := `(metric,tag,value) WHERE octet_length(metric)+octet_length(tag)+octet_length(value)<=2000`
			if len(metric)+len(key)+len(value) > indexTextBudget {
				conflict = `(row_id)`
			}
			if _, err = tx.Exec(ctx, `INSERT INTO flame_tag_values(metric,tag,value) VALUES($1,$2,$3) ON CONFLICT `+conflict+` DO NOTHING`, metric, key, value); err != nil {
				return nil, err
			}
		}
	}
	for board, entity := range boards(q) {
		var old *float64
		var current float64
		err = tx.QueryRow(ctx, `SELECT value FROM flame_scores WHERE flame_identity_hash(metric)=flame_identity_hash($1) AND metric=$1 AND flame_identity_hash(board)=flame_identity_hash($2) AND board=$2 AND flame_identity_hash(entity)=flame_identity_hash($3) AND entity=$3`, metric, EncodeString(board), EncodeString(entity)).Scan(&current)
		if err != nil && err != pgx.ErrNoRows {
			return nil, err
		}
		if err == nil {
			v := current
			old = &v
		}
		if q.Type == query.QueryTypeDelete {
			if old != nil {
				deleted.LeaderboardEntries++
			}
			if !q.DryRun {
				if _, err = tx.Exec(ctx, `DELETE FROM flame_scores WHERE flame_identity_hash(metric)=flame_identity_hash($1) AND metric=$1 AND flame_identity_hash(board)=flame_identity_hash($2) AND board=$2 AND flame_identity_hash(entity)=flame_identity_hash($3) AND entity=$3`, metric, EncodeString(board), EncodeString(entity)); err != nil {
					return nil, err
				}
				changes = append(changes, scoreChange{board: board, entity: entity, old: old})
			}
		} else {
			value := q.Value
			if q.Type == query.QueryTypeWrite {
				value += current
			}
			if math.IsInf(value, 0) || math.IsNaN(value) {
				return nil, fmt.Errorf("leaderboard value must be finite")
			}
			if value == 0 {
				value = 0
			}
			conflict := `(metric,board,entity) WHERE octet_length(metric)+octet_length(board)+octet_length(entity)<=2000`
			if len(metric)+len(EncodeString(board))+len(EncodeString(entity)) > indexTextBudget {
				conflict = `(row_id)`
			}
			if _, err = tx.Exec(ctx, `INSERT INTO flame_scores(metric,board,entity,value) VALUES($1,$2,$3,$4) ON CONFLICT `+conflict+` DO UPDATE SET value=excluded.value`, metric, EncodeString(board), EncodeString(entity), value); err != nil {
				return nil, err
			}
			changes = append(changes, scoreChange{board: board, entity: entity, old: old, new: &value})
		}
	}
	if q.Type == query.QueryTypeDelete && (len(boards(q)) == 0 || len(q.Where) > 0) {
		to := q.To
		if to == 0 {
			to = math.MaxInt64
		}
		tags, _ := json.Marshal(EncodeTags(q.Where))
		if q.Where == nil {
			tags = []byte(`{}`)
		}
		if q.DryRun {
			err = tx.QueryRow(ctx, `SELECT count(*) FROM flame_events WHERE flame_identity_hash(metric)=flame_identity_hash($1) AND metric=$1 AND ts >= $2 AND ts < $3 AND tags @> $4::jsonb`, metric, q.From, to, string(tags)).Scan(&deleted.Events)
		} else {
			tag, deleteErr := tx.Exec(ctx, `DELETE FROM flame_events WHERE flame_identity_hash(metric)=flame_identity_hash($1) AND metric=$1 AND ts >= $2 AND ts < $3 AND tags @> $4::jsonb`, metric, q.From, to, string(tags))
			err = deleteErr
			deleted.Events = tag.RowsAffected()
		}
		if err != nil {
			return nil, err
		}
	}
	if q.DryRun {
		return &query.Result{Delete: deleted}, nil
	}
	if len(changes) > 0 {
		if _, err = tx.Exec(ctx, `UPDATE flame_metrics SET version=version+1 WHERE flame_identity_hash(metric)=flame_identity_hash($1) AND metric=$1`, metric); err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	if len(changes) > 0 {
		e.project(ctx, q.Metric, previous, changes)
	}
	if q.Type == query.QueryTypeDelete {
		return &query.Result{Delete: deleted}, nil
	}
	return &query.Result{}, nil
}
func (e *Engine) ExecuteBatch(queries []*query.Query) (*query.BatchResult, error) {
	return e.ExecuteBatchContext(context.Background(), queries)
}
func (e *Engine) ExecuteBatchContext(ctx context.Context, queries []*query.Query) (*query.BatchResult, error) {
	ctx, cancel := e.bounded(ctx)
	defer cancel()
	result := &query.BatchResult{OK: true}
	fail := func(i int, err error) {
		result.Failed++
		result.Errors = append(result.Errors, query.BatchItemError{Index: i, Error: err.Error()})
	}
	// Contiguous ordinary writes use ordered staged state and native COPY.
	for i := 0; i < len(queries); {
		q := queries[i]
		if q == nil || q.Type != query.QueryTypeWrite {
			fail(i, fmt.Errorf("batch only supports WRITE"))
			i++
			continue
		}
		if err := e.validateWrite(q); err != nil {
			fail(i, err)
			i++
			continue
		}
		if q.IsReplica || q.Quorum {
			_, err := e.mutate(ctx, q)
			if err != nil {
				fail(i, err)
			} else {
				result.Accepted++
			}
			i++
			continue
		}
		start := i
		for i < len(queries) && i-start < e.batchSize {
			item := queries[i]
			if item == nil || item.Type != query.QueryTypeWrite || item.IsReplica || item.Quorum || e.validateWrite(item) != nil {
				break
			}
			i++
		}
		rejected, err := e.copyOrdinaryWrites(ctx, queries[start:i])
		for j := start; j < i; j++ {
			if itemErr := rejected[j-start]; itemErr != nil {
				fail(j, itemErr)
			} else if err != nil {
				fail(j, err)
			} else {
				result.Accepted++
			}
		}
	}
	result.OK = result.Failed == 0
	return result, nil
}
