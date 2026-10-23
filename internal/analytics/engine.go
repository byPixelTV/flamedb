// Package analytics implements durable PostgreSQL analytics and a disposable Redis ranking projection.
package analytics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/byPixelTV/flamedb/internal/query"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type Options struct {
	DSN                     string
	MaxConnections          int32
	Timescale               bool
	RedisOptions            redis.UniversalOptions
	RedisMode               string
	RequestTimeout          time.Duration
	MaxQueryEvents          int
	MaxResultBytes          int
	BatchSize               int
	QuorumReplicationFactor int
}
type Engine struct {
	db                      *pgxpool.Pool
	cache                   redis.UniversalClient
	timeout                 time.Duration
	max                     int
	maxBytes                int
	clock                   atomic.Int64
	batchSize               int
	quorumReplicationFactor int
	now                     func() int64
	ingest                  *writeCoalescer
	filters                 filterPlanner
	reads                   *aggregateReader
}

const schema = `
CREATE TABLE IF NOT EXISTS flame_metrics(metric text COLLATE "C" PRIMARY KEY, version bigint NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS flame_events(metric text COLLATE "C" NOT NULL, ts bigint NOT NULL, value double precision NOT NULL, tags jsonb NOT NULL DEFAULT '{}', PRIMARY KEY(metric,ts));
CREATE INDEX IF NOT EXISTS flame_events_tags ON flame_events USING gin(tags jsonb_path_ops);
CREATE TABLE IF NOT EXISTS flame_tag_values(metric text NOT NULL, tag text NOT NULL, value text NOT NULL, PRIMARY KEY(metric,tag,value));
CREATE TABLE IF NOT EXISTS flame_scores(metric text COLLATE "C" NOT NULL, board text COLLATE "C" NOT NULL, entity text COLLATE "C" NOT NULL, value double precision NOT NULL, PRIMARY KEY(metric,board,entity));
CREATE TABLE IF NOT EXISTS flame_receipts(operation text PRIMARY KEY);
CREATE TABLE IF NOT EXISTS flame_metric_clock(metric text COLLATE "C" PRIMARY KEY,last_ns bigint NOT NULL);
CREATE OR REPLACE FUNCTION flame_entity_bytes(input text) RETURNS bytea LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$ SELECT CASE WHEN left(input,9)=chr(30)||'Flame64:' THEN decode(substr(input,10),'base64') ELSE convert_to(input,'UTF8') END $$;
`

func Open(ctx context.Context, options Options) (*Engine, error) {
	if options.RequestTimeout <= 0 {
		options.RequestTimeout = 30 * time.Second
	}
	if options.MaxQueryEvents <= 0 {
		options.MaxQueryEvents = 100000
	}
	if options.MaxResultBytes <= 0 {
		options.MaxResultBytes = 64 << 20
	}
	if options.BatchSize <= 0 {
		options.BatchSize = 1000
	}
	if options.QuorumReplicationFactor < 0 {
		return nil, fmt.Errorf("quorum replication factor must be nonnegative")
	}
	config, err := pgxpool.ParseConfig(options.DSN)
	if err != nil {
		return nil, fmt.Errorf("postgres configuration: %w", err)
	}
	if options.MaxConnections > 0 {
		config.MaxConns = options.MaxConnections
	}
	db, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	e := &Engine{db: db, timeout: options.RequestTimeout, max: options.MaxQueryEvents, maxBytes: options.MaxResultBytes, batchSize: options.BatchSize, quorumReplicationFactor: options.QuorumReplicationFactor}
	if err = db.Ping(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("postgres unavailable: %w", err)
	}
	var fsync string
	if err = db.QueryRow(ctx, `SHOW fsync`).Scan(&fsync); err != nil || fsync != "on" {
		db.Close()
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("PostgreSQL fsync must be enabled for durable acknowledgements")
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		db.Close()
		return nil, err
	}
	defer tx.Rollback(ctx)
	// Serialize only migrations; runtime work takes per-metric locks.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(724018901)`); err == nil {
		_, err = tx.Exec(ctx, schema)
	}
	if err == nil {
		_, err = tx.Exec(ctx, largeKeySchema)
	}
	if err == nil {
		_, err = tx.Exec(ctx, eventFilterSchema)
	}
	if err == nil && options.Timescale {
		_, err = tx.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS timescaledb; SELECT create_hypertable('flame_events','ts',chunk_time_interval => 86400000000000::bigint,if_not_exists => true,migrate_data => true)`)
	}
	if err == nil {
		err = tx.Commit(ctx)
	}
	if err != nil {
		// Release the borrowed transaction before closing its pool. Close waits
		// for outstanding borrowers; a deferred rollback would otherwise deadlock
		// here and hide the actual migration error.
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = tx.Rollback(cleanup)
		cancel()
		db.Close()
		return nil, fmt.Errorf("analytics schema: %w", err)
	}
	if len(options.RedisOptions.Addrs) > 0 {
		// Redis is optional, but its failure path still shares the request's
		// deadline. go-redis otherwise ignores context deadlines for socket I/O.
		options.RedisOptions.ContextTimeoutEnabled = true
		if options.RedisMode == "cluster" || (len(options.RedisOptions.Addrs) > 1 && options.RedisOptions.MasterName == "") {
			e.cache = newClusterCache(options.RedisOptions.Cluster())
		} else {
			e.cache = redis.NewUniversalClient(&options.RedisOptions)
		}
	}
	e.ingest = newWriteCoalescer(e)
	e.reads = newAggregateReader(e)
	return e, nil
}

func newClusterCache(options *redis.ClusterOptions) *redis.ClusterClient {
	clusterOptions := *options
	// FlameDB uses core keyed commands and explicit-key Lua. Optional module
	// routing policies fetch COMMAND metadata with their own 5-second background
	// deadline, which would bypass our cache budget on a cold unavailable client.
	clusterOptions.DisableRoutingPolicies = true
	// Projection consistency requires the current master, including after
	// failover; asynchronous replica reads cannot establish an exact version.
	clusterOptions.ReadOnly = false
	clusterOptions.RouteByLatency = false
	clusterOptions.RouteRandomly = false
	return redis.NewClusterClient(&clusterOptions)
}
func (e *Engine) Close() {
	if e.reads != nil {
		e.reads.close()
	}
	if e.ingest != nil {
		e.ingest.close()
	}
	if e.cache != nil {
		if err := e.cache.Close(); err != nil {
			slog.Warn("closing Redis", "error", err)
		}
	}
	e.db.Close()
}
func (e *Engine) Ready(ctx context.Context) error { return e.db.Ping(ctx) }
func (e *Engine) PoolStats() *pgxpool.Stat        { return e.db.Stat() }
func (e *Engine) nextTimestamp() int64 {
	for {
		old := e.clock.Load()
		now := time.Now().UnixNano()
		if e.now != nil {
			now = e.now()
		}
		if now <= old {
			now = old + 1
		}
		if e.clock.CompareAndSwap(old, now) {
			return now
		}
	}
}
func (e *Engine) bounded(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, e.timeout)
}

// cacheAttempt reserves most of the request for PostgreSQL. Redis failover and
// a blackholed socket must not exhaust the authoritative query's deadline.
func (e *Engine) cacheAttempt(ctx context.Context) (context.Context, context.CancelFunc) {
	budget := e.timeout / 4
	if budget <= 0 || budget > 250*time.Millisecond {
		budget = 250 * time.Millisecond
	}
	if deadline, ok := ctx.Deadline(); ok {
		remainingBudget := time.Until(deadline) / 4
		if remainingBudget < budget {
			budget = remainingBudget
		}
	}
	return context.WithTimeout(ctx, budget)
}
func lockMetric(ctx context.Context, tx pgx.Tx, metric string) error {
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, EncodeString(metric))
	return err
}
func (e *Engine) Execute(q *query.Query) (*query.Result, error) {
	return e.ExecuteContext(context.Background(), q)
}
func (e *Engine) ExecuteContext(ctx context.Context, q *query.Query) (*query.Result, error) {
	ctx, cancel := e.bounded(ctx)
	defer cancel()
	if q == nil {
		return nil, fmt.Errorf("nil query")
	}
	if q.Offset < 0 || q.Limit < 0 || q.From < 0 || q.To < 0 {
		return nil, fmt.Errorf("invalid pagination or timestamp")
	}
	switch q.Type {
	case query.QueryTypeWrite, query.QueryTypeSet, query.QueryTypeDelete:
		return e.mutate(ctx, q)
	case query.QueryTypeGet:
		result, err := e.get(ctx, q)
		return result, readError(err)
	case query.QueryTypeMetrics:
		keys, err := e.listMetrics(ctx, q.Where)
		return &query.Result{MetricKeys: keys}, err
	case query.QueryTypeStats:
		return e.stats(ctx, q)
	case query.QueryTypeLeaderboard, query.QueryTypeGroupLeaderboard, query.QueryTypeRank, query.QueryTypeScore, query.QueryTypeAround, query.QueryTypeLeaderboardCount:
		result, err := e.leaderboard(ctx, q)
		return result, readError(err)
	default:
		return nil, fmt.Errorf("unsupported query %q", q.Type)
	}
}

// The frozen engine accumulates float64 values, then its JSON serializer
// rejects nonfinite derived results. Preserve that wire error when PostgreSQL
// reports the same floating-point overflow during its ordered SUM.
func readError(err error) error {
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) && postgresError.Code == "22003" {
		return fmt.Errorf("response serialization failed")
	}
	return err
}
func (e *Engine) GetAllMetrics() []string {
	metrics, err := e.ListMetrics(nil)
	if err != nil {
		slog.Error("listing analytics metrics", "error", err)
	}
	return metrics
}
func (e *Engine) ListMetrics(where map[string]string) ([]string, error) {
	ctx, cancel := e.bounded(context.Background())
	defer cancel()
	return e.listMetrics(ctx, where)
}

var _ query.Engine = (*Engine)(nil)
var _ query.ContextEngine = (*Engine)(nil)
