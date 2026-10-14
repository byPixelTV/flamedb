// import-postgres atomically imports a stopped legacy database into an empty
// PostgreSQL database, preserving absolute scores independently of raw events.
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	"time"

	"github.com/byPixelTV/flamedb/internal/analytics"
	"github.com/byPixelTV/flamedb/internal/keyspace"
	"github.com/byPixelTV/flamedb/internal/storage"
	"github.com/jackc/pgx/v5"
)

type rows struct {
	iter   storage.SnapshotIterator
	kind   string
	first  bool
	values []any
	err    error
}

func (r *rows) Next() bool {
	for {
		var valid bool
		if !r.first {
			r.first = true
			valid = r.iter.First()
		} else {
			valid = r.iter.Next()
		}
		if !valid {
			return false
		}
		values, match, err := decode(r.kind, r.iter.Key(), r.iter.Value())
		if err != nil {
			r.err = err
			return false
		}
		if match {
			r.values = values
			return true
		}
	}
}
func (r *rows) Values() ([]any, error) { return r.values, nil }
func (r *rows) Err() error {
	if r.err != nil {
		return r.err
	}
	return r.iter.Error()
}

func decode(kind string, key, value []byte) ([]any, bool, error) {
	text := string(key)
	switch kind {
	case "events":
		i := bytes.IndexByte(key, ':')
		if i < 0 || len(key)-i-1 != 8 {
			return nil, false, nil
		}
		prefix := text[:i]
		for _, reserved := range []string{"lb", "lb-entity", "idx", "card", "card-count", "repl-outbox", "repl-applied"} {
			if prefix == reserved {
				return nil, false, nil
			}
		}
		metric := keyspace.DecodeComponent(prefix)
		event, err := storage.DecodeSnapshotEvent(value, metric)
		if err != nil {
			return nil, false, err
		}
		if event.Timestamp != int64(binary.BigEndian.Uint64(key[i+1:])) {
			return nil, false, fmt.Errorf("event timestamp differs from primary key")
		}
		if math.IsNaN(event.Value) || math.IsInf(event.Value, 0) {
			return nil, false, fmt.Errorf("nonfinite event in %s", metric)
		}
		tags := event.Tags
		if tags == nil {
			tags = map[string]string{}
		}
		return []any{analytics.EncodeString(metric), event.Timestamp, event.Value, analytics.EncodeTags(tags)}, true, nil
	case "scores":
		if !strings.HasPrefix(text, "lb-entity:") {
			return nil, false, nil
		}
		parts := strings.SplitN(strings.TrimPrefix(text, "lb-entity:"), ":", 2)
		if len(parts) != 2 || len(value) != 8 {
			return nil, false, fmt.Errorf("corrupt legacy score")
		}
		metricBoard := strings.SplitN(keyspace.DecodeComponent(parts[0]), "\x1f", 2)
		board := ""
		if len(metricBoard) == 2 {
			board = metricBoard[1]
		}
		score := math.Float64frombits(binary.BigEndian.Uint64(value))
		if math.IsNaN(score) || math.IsInf(score, 0) {
			return nil, false, fmt.Errorf("nonfinite legacy score")
		}
		return []any{analytics.EncodeString(metricBoard[0]), analytics.EncodeString(board), analytics.EncodeString(parts[1]), score}, true, nil
	case "tags":
		if !strings.HasPrefix(text, "card:") {
			return nil, false, nil
		}
		parts := strings.SplitN(text, ":", 4)
		if len(parts) != 4 {
			return nil, false, fmt.Errorf("corrupt cardinality key")
		}
		return []any{analytics.EncodeString(keyspace.DecodeComponent(parts[1])), analytics.EncodeString(keyspace.DecodeComponent(parts[2])), analytics.EncodeString(parts[3])}, true, nil
	}
	return nil, false, nil
}

func importSnapshot(ctx context.Context, source, dsn string, timescale bool) error {
	iter, closeDB, err := storage.OpenReadOnlySnapshot(source)
	if err != nil {
		return err
	}
	defer closeDB()
	defer iter.Close()
	// Drain replication first; an unresolved outbox would make the source ambiguous.
	for iter.First(); iter.Valid(); iter.Next() {
		if bytes.HasPrefix(iter.Key(), []byte("repl-outbox:")) {
			return fmt.Errorf("source has pending replication; drain its outbox before migration")
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}
	engine, err := analytics.Open(ctx, analytics.Options{DSN: dsn, Timescale: timescale})
	if err != nil {
		return err
	}
	defer engine.Close()
	db, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer db.Close(ctx)
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SET LOCAL synchronous_commit=on; LOCK TABLE flame_metrics,flame_events,flame_scores,flame_tag_values,flame_receipts,flame_metric_clock IN ACCESS EXCLUSIVE MODE`); err != nil {
		return err
	}
	var exists bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM flame_events) OR EXISTS(SELECT 1 FROM flame_scores) OR EXISTS(SELECT 1 FROM flame_metrics) OR EXISTS(SELECT 1 FROM flame_tag_values) OR EXISTS(SELECT 1 FROM flame_receipts) OR EXISTS(SELECT 1 FROM flame_metric_clock)`).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("destination must be empty; no data was imported")
	}
	counts := map[string]int64{}
	for _, spec := range []struct {
		kind, table string
		columns     []string
	}{{"events", "flame_events", []string{"metric", "ts", "value", "tags"}}, {"scores", "flame_scores", []string{"metric", "board", "entity", "value"}}, {"tags", "flame_tag_values", []string{"metric", "tag", "value"}}} {
		count, err := tx.CopyFrom(ctx, pgx.Identifier{spec.table}, spec.columns, &rows{iter: iter, kind: spec.kind})
		if err != nil {
			return fmt.Errorf("import %s: %w", spec.kind, err)
		}
		counts[spec.kind] = count
	}
	// Older snapshots without the entity lookup index still carry JSON score keys.
	for iter.First(); iter.Valid(); iter.Next() {
		if !bytes.HasPrefix(iter.Key(), []byte("lb:")) {
			continue
		}
		parts := bytes.SplitN(iter.Key()[3:], []byte(":"), 2)
		if len(parts) != 2 {
			return fmt.Errorf("corrupt legacy board key")
		}
		metricBoard := strings.SplitN(keyspace.DecodeComponent(string(parts[0])), "\x1f", 2)
		board := ""
		if len(metricBoard) == 2 {
			board = metricBoard[1]
		}
		var entry struct {
			EntityID string  `json:"entity_id"`
			Value    float64 `json:"value"`
		}
		if err = json.Unmarshal(iter.Value(), &entry); err != nil {
			return err
		}
		if math.IsNaN(entry.Value) || math.IsInf(entry.Value, 0) {
			return fmt.Errorf("nonfinite leaderboard")
		}
		tag, err := tx.Exec(ctx, `INSERT INTO flame_scores(metric,board,entity,value) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`, analytics.EncodeString(metricBoard[0]), analytics.EncodeString(board), analytics.EncodeString(entry.EntityID), entry.Value)
		if err != nil {
			return err
		}
		counts["scores"] += tag.RowsAffected()
	}
	if err := iter.Error(); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO flame_tag_values(metric,tag,value) SELECT DISTINCT metric,t.key,t.value FROM flame_events CROSS JOIN LATERAL jsonb_each_text(tags) t ON CONFLICT DO NOTHING; INSERT INTO flame_metrics(metric,version) SELECT metric,1 FROM (SELECT metric FROM flame_events UNION SELECT metric FROM flame_scores UNION SELECT metric FROM flame_tag_values) m`); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("import commit outcome may be unknown: %w", err)
	}
	log.Printf("import committed events=%d scores=%d historical_tags=%d; source was opened read-only", counts["events"], counts["scores"], counts["tags"])
	return nil
}
func main() {
	source := flag.String("source", "", "stopped legacy data_path; read-only")
	dsn := flag.String("dsn", os.Getenv("FLAMEDB_POSTGRES_DSN"), "empty destination PostgreSQL DSN; defaults to FLAMEDB_POSTGRES_DSN")
	timescale := flag.Bool("timescale", false, "create TimescaleDB hypertable")
	timeout := flag.Duration("timeout", time.Hour, "whole migration deadline")
	flag.Parse()
	if *source == "" || *dsn == "" {
		log.Fatal("-source and PostgreSQL DSN are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	if err := importSnapshot(ctx, *source, *dsn, *timescale); err != nil {
		log.Fatal(err)
	}
}
