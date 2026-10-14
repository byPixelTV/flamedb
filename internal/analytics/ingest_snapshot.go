package analytics

import (
	"context"
	"sort"

	"github.com/byPixelTV/flamedb/internal/query"
	"github.com/jackc/pgx/v5"
)

type ingestSnapshot struct {
	versions map[string]int64
	current  map[scoreKey]float64
	initial  map[scoreKey]*float64
	clocks   map[string]int64
	staged   bool
}

// Batch all dependent reads after the sorted lock statements. PostgreSQL READ
// COMMITTED takes each statement snapshot after the preceding lock completed,
// as in lockProjectionVersion. Preparation of temp relations happens before
// any metric lock, reducing the serialized part of large score transactions.
func (e *Engine) beginIngest(ctx context.Context, items []*query.Query) (*ingestSnapshot, pgx.Tx, error) {
	metrics := map[string]bool{}
	targets := map[scoreKey]bool{}
	automatic := map[string]bool{}
	seen := map[*query.Query]bool{}
	allAutomatic, aliased := true, false
	for _, q := range items {
		entities := boards(q)
		metrics[q.Metric] = metrics[q.Metric] || q.Timestamp == 0 || len(entities) > 0 || needsLargeIdentityLock(q)
		if q.Timestamp == 0 {
			automatic[q.Metric] = true
		} else {
			allAutomatic = false
		}
		if seen[q] {
			aliased = true
		}
		seen[q] = true
		for board, entity := range entities {
			targets[scoreKey{q.Metric, board, entity}] = true
		}
	}
	s := &ingestSnapshot{versions: map[string]int64{}, current: map[scoreKey]float64{}, initial: map[scoreKey]*float64{}, clocks: map[string]int64{}}
	s.staged = !allAutomatic || aliased
	begin := `BEGIN ISOLATION LEVEL READ COMMITTED; SET LOCAL synchronous_commit=on`
	if s.staged {
		begin += `; CREATE TEMP TABLE IF NOT EXISTS flame_ingest(ord bigint,metric text,ts bigint,value double precision,tags jsonb) ON COMMIT DELETE ROWS`
		if len(targets) > 0 {
			begin += `; CREATE TEMP TABLE IF NOT EXISTS flame_score_ingest(metric text,board text,entity text,value double precision) ON COMMIT DELETE ROWS`
		}
	}
	tx, err := e.db.BeginTx(ctx, pgx.TxOptions{BeginQuery: begin})
	if err != nil {
		return nil, nil, err
	}
	ok := false
	defer func() {
		if !ok {
			tx.Rollback(ctx)
		}
	}()
	names := make([]string, 0, len(metrics))
	for name := range metrics {
		names = append(names, name)
	}
	sort.Strings(names)
	batch := &pgx.Batch{}
	encoded := make([]string, 0, len(names))
	exclusive := make([]bool, 0, len(names))
	for _, name := range names {
		encoded = append(encoded, EncodeString(name))
		exclusive = append(exclusive, metrics[name])
	}
	lockStatements := len(names)
	if len(names) > 1 {
		// PostgreSQL evaluates these volatile output expressions after sorting
		// by the independent ordinal. Preserve Go's raw-name order even when
		// encoding a byte string changes its text order. Subsequent statements
		// take fresh READ COMMITTED snapshots only after all locks are granted.
		batch.Queue(`SELECT CASE WHEN exclusive THEN pg_advisory_xact_lock(hashtextextended(metric,0)) ELSE pg_advisory_xact_lock_shared(hashtextextended(metric,0)) END FROM unnest($1::text[],$2::boolean[]) WITH ORDINALITY t(metric,exclusive,ordinal) ORDER BY ordinal`, encoded, exclusive)
		lockStatements = 1
	} else if len(names) == 1 {
		lock := `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`
		if !exclusive[0] {
			lock = `SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))`
		}
		batch.Queue(lock, encoded[0])
	}
	if len(targets) > 0 {
		versionSQL := `SELECT metric,version FROM flame_metrics WHERE flame_identity_hash(metric)=ANY(ARRAY(SELECT flame_identity_hash(name) FROM unnest($1::text[]) names(name))) AND metric=ANY($1::text[])`
		allSmall := len(items) <= 32
		for _, name := range encoded {
			allSmall = allSmall && len(name) <= indexTextBudget
		}
		if allSmall {
			versionSQL = `SELECT metric,version FROM flame_metrics WHERE metric=ANY($1::text[]) AND octet_length(metric)<=2000`
		}
		batch.Queue(versionSQL, encoded)
		m, b, id := []string{}, []string{}, []string{}
		allSmall = len(items) <= 32
		for key := range targets {
			metric, board, entity := EncodeString(key.metric), EncodeString(key.board), EncodeString(key.entity)
			m = append(m, metric)
			b = append(b, board)
			id = append(id, entity)
			allSmall = allSmall && len(metric)+len(board)+len(entity) <= indexTextBudget
		}
		lookup := `SELECT s.metric,s.board,s.entity,s.value FROM flame_scores s JOIN unnest($1::text[],$2::text[],$3::text[]) AS t(metric,board,entity) ON s.metric=t.metric AND s.board=t.board AND s.entity=t.entity AND flame_identity_hash(s.metric)=flame_identity_hash(t.metric) AND flame_identity_hash(s.board)=flame_identity_hash(t.board) AND flame_identity_hash(s.entity)=flame_identity_hash(t.entity)`
		if allSmall {
			lookup = `SELECT s.metric,s.board,s.entity,s.value FROM flame_scores s JOIN unnest($1::text[],$2::text[],$3::text[]) AS t(metric,board,entity) ON s.metric=t.metric AND s.board=t.board AND s.entity=t.entity WHERE octet_length(s.metric)+octet_length(s.board)+octet_length(s.entity)<=2000`
		}
		batch.Queue(lookup, m, b, id)
	}
	clockStates := make([]*timestampAllocation, 0, len(automatic))
	for _, name := range names {
		if automatic[name] {
			clockStates = append(clockStates, &timestampAllocation{metric: name, encoded: EncodeString(name)})
		}
	}
	clockCount := queueTimestampClocks(batch, clockStates)
	results := tx.SendBatch(ctx, batch)
	for range lockStatements {
		if _, err = results.Exec(); err != nil {
			results.Close()
			return nil, nil, err
		}
	}
	if len(targets) > 0 {
		rows, readErr := results.Query()
		if readErr != nil {
			results.Close()
			return nil, nil, readErr
		}
		for rows.Next() {
			var name string
			var version int64
			if err = rows.Scan(&name, &version); err == nil {
				name, err = DecodeString(name)
			}
			if err != nil {
				rows.Close()
				results.Close()
				return nil, nil, err
			}
			s.versions[name] = version
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			results.Close()
			return nil, nil, err
		}
		rows, readErr = results.Query()
		if readErr != nil {
			results.Close()
			return nil, nil, readErr
		}
		for rows.Next() {
			var key scoreKey
			var value float64
			if err = rows.Scan(&key.metric, &key.board, &key.entity, &value); err == nil {
				key.metric, err = DecodeString(key.metric)
			}
			if err == nil {
				key.board, err = DecodeString(key.board)
			}
			if err == nil {
				key.entity, err = DecodeString(key.entity)
			}
			if err != nil {
				rows.Close()
				results.Close()
				return nil, nil, err
			}
			s.current[key] = value
			v := value
			s.initial[key] = &v
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			results.Close()
			return nil, nil, err
		}
	}
	s.clocks, err = scanTimestampClocks(results, clockCount)
	if err != nil {
		results.Close()
		return nil, nil, err
	}
	if err = results.Close(); err != nil {
		return nil, nil, err
	}
	ok = true
	return s, tx, nil
}
