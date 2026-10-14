package analytics

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strconv"

	"github.com/byPixelTV/flamedb/internal/query"
	"github.com/byPixelTV/flamedb/internal/types"
	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
)

// Exact float64 ordering is encoded in the member; Redis scores are always zero.
// The reference scans negative ties backwards, hence the reversed entity bytes.
func encodeMember(value float64, entity string) string {
	if value == 0 {
		value = 0
	}
	bits := math.Float64bits(value)
	if bits>>63 != 0 {
		bits = ^bits
	} else {
		bits ^= 1 << 63
	}
	bits = ^bits
	data := make([]byte, 8, 9+len(entity))
	binary.BigEndian.PutUint64(data, bits)
	if value < 0 {
		for i := 0; i < len(entity); i++ {
			data = append(data, 0, ^entity[i])
		}
		data = append(data, 1)
	} else {
		data = append(data, entity...)
	}
	return string(data)
}
func decodeMember(member string) (types.LeaderboardEntry, error) {
	if len(member) < 8 {
		return types.LeaderboardEntry{}, fmt.Errorf("corrupt Redis rank member")
	}
	bits := ^binary.BigEndian.Uint64([]byte(member[:8]))
	if bits>>63 != 0 {
		bits ^= 1 << 63
	} else {
		bits = ^bits
	}
	value := math.Float64frombits(bits)
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return types.LeaderboardEntry{}, fmt.Errorf("nonfinite Redis rank member")
	}
	entity := []byte(member[8:])
	if value < 0 {
		if len(entity)%2 != 1 || entity[len(entity)-1] != 1 {
			return types.LeaderboardEntry{}, fmt.Errorf("corrupt reversed entity")
		}
		decoded := make([]byte, 0, len(entity)/2)
		for i := 0; i < len(entity)-1; i += 2 {
			if entity[i] != 0 {
				return types.LeaderboardEntry{}, fmt.Errorf("corrupt reversed entity")
			}
			decoded = append(decoded, ^entity[i+1])
		}
		entity = decoded
	}
	return types.LeaderboardEntry{Value: value, EntityID: string(entity)}, nil
}
func token() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
func metricKey(metric string) string {
	sum := sha256.Sum256([]byte(metric))
	return "flamedb:{" + hex.EncodeToString(sum[:]) + "}"
}
func boardKey(metric, board string) string {
	sum := sha256.Sum256([]byte(board))
	return metricKey(metric) + ":board:" + hex.EncodeToString(sum[:])
}

// Short request deadlines can interrupt ClusterClient before its retry path
// notices a failed node. Ask the client's coalesced background loader to refresh
// the slot map independently of that expired request, without delaying SQL.
func (e *Engine) refreshCacheTopology(ctx context.Context) {
	if cluster, ok := e.cache.(*redis.ClusterClient); ok {
		cluster.ReloadState(ctx)
	}
}

func (e *Engine) epoch(ctx context.Context) (string, error) {
	if e.cache == nil {
		return "", fmt.Errorf("Redis disabled")
	}
	value, err := e.cache.Get(ctx, "flamedb:epoch").Result()
	if err == nil {
		return value, nil
	}
	if !errors.Is(err, redis.Nil) {
		e.refreshCacheTopology(ctx)
		return "", err
	}
	if err := e.cache.SetNX(ctx, "flamedb:epoch", token(), 0).Err(); err != nil {
		e.refreshCacheTopology(ctx)
		return "", err
	}
	value, err = e.cache.Get(ctx, "flamedb:epoch").Result()
	if err != nil {
		e.refreshCacheTopology(ctx)
	}
	return value, err
}

const applyProjectionLua = `
if redis.call('HGET',KEYS[1],'epoch')~=ARGV[1] or redis.call('HGET',KEYS[1],'version')~=ARGV[2] then return 0 end
local n=tonumber(ARGV[4])
local checked={}
for i=1,n do
 local a=5+(i-1)*5
 local k=tonumber(ARGV[a])
 if not checked[k] then
  local board=string.sub(KEYS[k],1,-3)
  local expected=tonumber(redis.call('HGET',KEYS[1],board) or '0')
  if redis.call('ZCARD',KEYS[k])~=expected or redis.call('HLEN',KEYS[k+1])~=expected then
   redis.call('HDEL',KEYS[1],'version'); return 0
  end
  checked[k]=board
 end
end
for i=1,n do
 local a=5+(i-1)*5
 local k=tonumber(ARGV[a])
 if ARGV[a+2]~='' then redis.call('ZREM',KEYS[k],ARGV[a+2]) end
 if ARGV[a+3]=='1' then
  redis.call('ZADD',KEYS[k],0,ARGV[a+4]); redis.call('HSET',KEYS[k+1],ARGV[a+1],ARGV[a+4])
 else redis.call('HDEL',KEYS[k+1],ARGV[a+1]) end
end
for k,board in pairs(checked) do
 redis.call('HSET',KEYS[1],board,redis.call('ZCARD',KEYS[k]))
end
redis.call('HSET',KEYS[1],'version',ARGV[3]); return 1
`

var applyProjection = redis.NewScript(applyProjectionLua)

func (e *Engine) project(ctx context.Context, metric string, previous int64, changes []scoreChange) {
	cacheCtx, cancel := e.cacheAttempt(ctx)
	defer cancel()
	e.projectWithin(cacheCtx, metric, previous, changes)
}

func (e *Engine) projectWithin(ctx context.Context, metric string, previous int64, changes []scoreChange) {
	if e.cache == nil {
		return
	}
	epoch, err := e.epoch(ctx)
	if err != nil {
		slog.Debug("Redis projection deferred", "error", err)
		return
	}
	keys, args := projectionArguments(metric, previous, changes, epoch)
	if _, err = applyProjection.Run(ctx, e.cache, keys, args...).Result(); err != nil {
		e.refreshCacheTopology(ctx)
		slog.Debug("Redis projection deferred", "error", err)
	}
}

func projectionArguments(metric string, previous int64, changes []scoreChange, epoch string) ([]string, []any) {
	keys := []string{metricKey(metric) + ":meta"}
	args := []any{epoch, strconv.FormatInt(previous, 10), strconv.FormatInt(previous+1, 10), len(changes)}
	boardIndices := make(map[string]int)
	for _, change := range changes {
		index := boardIndices[change.board]
		if index == 0 {
			index = len(keys) + 1
			boardIndices[change.board] = index
			key := boardKey(metric, change.board)
			keys = append(keys, key+":z", key+":h")
		}
		old, new, exists := "", "", "0"
		if change.old != nil {
			old = encodeMember(*change.old, change.entity)
		}
		if change.new != nil {
			new = encodeMember(*change.new, change.entity)
			exists = "1"
		}
		args = append(args, index, change.entity, old, exists, new)
	}
	return keys, args
}

// An epoch observed once per publication budget is sufficient: each atomic
// script checks its metric's epoch and previous generation before mutation.
// A reset/rebuild rejects older publications; reads still check SQL versions.
func (e *Engine) projectManyWithin(ctx context.Context, publications []committedProjection) {
	if e.cache == nil || len(publications) == 0 {
		return
	}
	epoch, err := e.epoch(ctx)
	if err != nil {
		return
	}
	e.projectBatchWithSource(ctx, epoch, publications, applyProjectionLua)
}

func (e *Engine) projectBatchWithSource(ctx context.Context, epoch string, publications []committedProjection, source string) {
	if e.cache == nil {
		return
	}
	script := redis.NewScript(source)
	for start := 0; start < len(publications) && ctx.Err() == nil; start += 32 {
		part := publications[start:min(start+32, len(publications))]
		keys := make([][]string, len(part))
		args := make([][]any, len(part))
		commands := make([]*redis.Cmd, len(part))
		_, err := e.cache.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			for i, p := range part {
				keys[i], args[i] = projectionArguments(p.metric, p.previous, p.changes, epoch)
				commands[i] = pipe.EvalSha(ctx, script.Hash(), keys[i], args[i]...)
			}
			return nil
		})
		missing := make([]int, 0, len(part))
		for i, cmd := range commands {
			if redis.HasErrorPrefix(cmd.Err(), "NOSCRIPT") {
				missing = append(missing, i)
			}
		}
		if len(missing) > 0 && ctx.Err() == nil {
			// NOSCRIPT guarantees these commands did not execute. Re-send only
			// them with source; absolute updates and generation checks also make
			// transport retries safe if a response is lost after execution.
			_, retryErr := e.cache.Pipelined(ctx, func(pipe redis.Pipeliner) error {
				for _, i := range missing {
					commands[i] = pipe.Eval(ctx, source, keys[i], args[i]...)
				}
				return nil
			})
			if retryErr != nil {
				err = retryErr
			} else {
				err = nil
				for _, cmd := range commands {
					if cmd.Err() != nil {
						err = cmd.Err()
						break
					}
				}
			}
		}
		if err != nil {
			e.refreshCacheTopology(ctx)
			slog.Debug("Redis projection batch deferred", "error", err)
		}
	}
}

// Rebuild is streaming and publishes each metric snapshot atomically. Independent
// metrics remain writable; transactions lock only the metric being projected.
func (e *Engine) Rebuild(ctx context.Context) error {
	if e.cache == nil {
		return fmt.Errorf("Redis is not configured")
	}
	// Maintenance is independent of public response limits. Close each bounded
	// page before acquiring a transaction so a one-connection pool also works.
	var cursor int64
	for {
		rows, err := e.db.Query(ctx, `SELECT row_id,metric FROM flame_metrics WHERE row_id>$1 ORDER BY row_id LIMIT 256`, cursor)
		if err != nil {
			return err
		}
		names := make([]string, 0, 256)
		for rows.Next() {
			var encoded string
			if err = rows.Scan(&cursor, &encoded); err != nil {
				rows.Close()
				return err
			}
			metric, decodeErr := DecodeString(encoded)
			if decodeErr != nil {
				rows.Close()
				return decodeErr
			}
			names = append(names, metric)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if len(names) == 0 {
			return nil
		}
		for _, metric := range names {
			tx, beginErr := e.db.Begin(ctx)
			if beginErr != nil {
				return beginErr
			}
			err = lockMetric(ctx, tx, metric)
			if err == nil {
				var version int64
				err = tx.QueryRow(ctx, `SELECT version FROM flame_metrics WHERE flame_identity_hash(metric)=flame_identity_hash($1) AND metric=$1`, EncodeString(metric)).Scan(&version)
				if err == nil {
					epoch, epochErr := e.epoch(ctx)
					err = epochErr
					if err == nil {
						err = e.rebuildMetric(ctx, tx, metric, version, epoch)
					}
				}
			}
			rollbackErr := tx.Rollback(ctx)
			if err != nil {
				return err
			}
			if rollbackErr != nil {
				return rollbackErr
			}
		}
	}
}

var publishProjection = redis.NewScript(`
for i=1,tonumber(ARGV[3]) do
 local k=2+(i-1)*4
 local expected=tonumber(ARGV[5+(i-1)*2])
 if redis.call('ZCARD',KEYS[k])~=expected or redis.call('HLEN',KEYS[k+1])~=expected then return 0 end
end
for k=2+tonumber(ARGV[3])*4,#KEYS,2 do
 redis.call('DEL',KEYS[k],KEYS[k+1])
end
redis.call('DEL',KEYS[1])
for i=1,tonumber(ARGV[3]) do
 local k=2+(i-1)*4
 redis.call('DEL',KEYS[k+2],KEYS[k+3])
 if redis.call('EXISTS',KEYS[k])==1 then redis.call('RENAME',KEYS[k],KEYS[k+2]); redis.call('PERSIST',KEYS[k+2]) end
 if redis.call('EXISTS',KEYS[k+1])==1 then redis.call('RENAME',KEYS[k+1],KEYS[k+3]); redis.call('PERSIST',KEYS[k+3]) end
 redis.call('HSET',KEYS[1],ARGV[4+(i-1)*2],redis.call('ZCARD',KEYS[k+2]))
end
-- Publish validity last: Lua errors do not roll back prior commands. A failed
-- rename/count write must leave metadata invalid so readers use PostgreSQL.
redis.call('HSET',KEYS[1],'epoch',ARGV[1],'version',ARGV[2])
return 1
`)

func (e *Engine) rebuildMetric(ctx context.Context, tx pgx.Tx, metric string, version int64, epoch string) error {
	return e.rebuildMetricWithCacheContext(ctx, ctx, tx, metric, version, epoch)
}

func (e *Engine) rebuildMetricWithCacheContext(ctx, cacheCtx context.Context, tx pgx.Tx, metric string, version int64, epoch string) error {
	rows, err := tx.Query(ctx, `SELECT board,entity,value FROM flame_scores WHERE flame_identity_hash(metric)=flame_identity_hash($1) AND metric=$1 ORDER BY board,entity`, EncodeString(metric))
	if err != nil {
		return err
	}
	defer rows.Close()
	generation := token()
	boards := []string{}
	seen := map[string]bool{}
	counts := map[string]int64{}
	pipe := e.cache.Pipeline()
	pending := 0
	pendingBytes := 0
	flush := func() error {
		if pending == 0 {
			return nil
		}
		_, err := pipe.Exec(cacheCtx)
		if err != nil {
			e.refreshCacheTopology(cacheCtx)
		}
		pipe = e.cache.Pipeline()
		pending = 0
		pendingBytes = 0
		return err
	}
	for rows.Next() {
		if err = cacheCtx.Err(); err != nil {
			return err
		}
		var board, entity string
		var value float64
		if err = rows.Scan(&board, &entity, &value); err != nil {
			return err
		}
		board, err = DecodeString(board)
		if err != nil {
			return err
		}
		entity, err = DecodeString(entity)
		if err != nil {
			return err
		}
		key := boardKey(metric, board)
		temp := key + ":tmp:" + generation
		if !seen[board] {
			seen[board] = true
			boards = append(boards, board)
		}
		member := encodeMember(value, entity)
		estimatedBytes := len(member)*2 + len(entity) + len(temp)*4 + 128
		if pendingBytes+estimatedBytes > 1<<20 && pending > 0 {
			if err = flush(); err != nil {
				return err
			}
		}
		counts[board]++
		pipe.ZAdd(cacheCtx, temp+":z", redis.Z{Score: 0, Member: member})
		pipe.HSet(cacheCtx, temp+":h", entity, member)
		pipe.Expire(cacheCtx, temp+":z", e.timeout*4)
		pipe.Expire(cacheCtx, temp+":h", e.timeout*4)
		pending++
		pendingBytes += estimatedBytes
		if pending >= 500 || pendingBytes >= 1<<20 {
			if err = flush(); err != nil {
				return err
			}
		}
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if err = flush(); err != nil {
		return err
	}
	keys := []string{metricKey(metric) + ":meta"}
	args := []any{epoch, strconv.FormatInt(version, 10), len(boards)}
	for _, board := range boards {
		key := boardKey(metric, board)
		temp := key + ":tmp:" + generation
		keys = append(keys, temp+":z", temp+":h", key+":z", key+":h")
		args = append(args, key, counts[board])
	}
	// Cluster scripts must declare every key they access. Metadata stores board
	// names as fields; never construct undeclared Redis keys inside Lua. The
	// PostgreSQL metric lock prevents a concurrent rebuild or newer mutation.
	fields, err := e.cache.HKeys(cacheCtx, keys[0]).Result()
	if err != nil {
		e.refreshCacheTopology(cacheCtx)
		return err
	}
	for _, field := range fields {
		if field != "epoch" && field != "version" {
			keys = append(keys, field+":z", field+":h")
		}
	}
	var published int64
	published, err = publishProjection.Run(cacheCtx, e.cache, keys, args...).Int64()
	if err != nil {
		e.refreshCacheTopology(cacheCtx)
	}
	if err == nil && published != 1 {
		return fmt.Errorf("Redis rebuild staging expired or was evicted; existing projection was preserved")
	}
	return err
}

var errStaleProjection = errors.New("Redis ranking projection needs rebuilding")

// Lock and version are separate statements in one network round trip. The
// READ COMMITTED version statement gets its snapshot after the lock is granted;
// a single SELECT combining them could take a snapshot before waiting.
func lockProjectionVersion(ctx context.Context, tx pgx.Tx, metric string, shared bool) (int64, error) {
	batch := &pgx.Batch{}
	lock := `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`
	if shared {
		lock = `SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))`
	}
	batch.Queue(lock, EncodeString(metric))
	batch.Queue(`SELECT version FROM flame_metrics WHERE flame_identity_hash(metric)=flame_identity_hash($1) AND metric=$1`, EncodeString(metric))
	results := tx.SendBatch(ctx, batch)
	if _, err := results.Exec(); err != nil {
		results.Close()
		return 0, err
	}
	var version int64
	err := results.QueryRow().Scan(&version)
	if err == pgx.ErrNoRows {
		err = nil
	}
	if closeErr := results.Close(); err == nil {
		err = closeErr
	}
	return version, err
}

func (e *Engine) durableProjectionVersion(ctx context.Context, metric string) (int64, error) {
	var version int64
	err := e.db.QueryRow(ctx, `SELECT version FROM flame_metrics WHERE flame_identity_hash(metric)=flame_identity_hash($1) AND metric=$1`, EncodeString(metric)).Scan(&version)
	if err == pgx.ErrNoRows {
		return 0, nil
	}
	return version, err
}
func (e *Engine) leaderboard(ctx context.Context, q *query.Query) (*query.Result, error) {
	if q.Radius < 0 || q.Radius > e.max/2 {
		return nil, fmt.Errorf("invalid around radius")
	}
	if q.Limit > e.max || q.Offset > e.max {
		return nil, fmt.Errorf("leaderboard pagination exceeds configured limit %d", e.max)
	}
	if q.Type == query.QueryTypeGroupLeaderboard && len(q.Groups) == 0 {
		return &query.Result{Leaderboard: []types.LeaderboardEntry{}}, nil
	}
	if q.From != 0 || q.To != 0 {
		entries, err := e.window(ctx, q)
		if err != nil {
			return nil, err
		}
		if q.Type == query.QueryTypeGroupLeaderboard {
			return e.groups(ctx, nil, q, entries)
		}
		if q.Type == query.QueryTypeLeaderboard {
			return &query.Result{Leaderboard: entries}, nil
		}
		return rankingResult(q, entries)
	}
	var tx pgx.Tx
	if e.cache != nil && q.Type != query.QueryTypeGroupLeaderboard {
		// A warm atomic Redis read may return exactly the durable generation
		// observed by this SELECT. A concurrent commit can move Redis forward,
		// in which case the script rejects it. If Redis still holds the observed
		// generation, that immutable snapshot linearizes before the commit.
		// A request starting after an ACK cannot observe an older SQL version.
		version, err := e.durableProjectionVersion(ctx, q.Metric)
		if err != nil {
			return nil, err
		}
		cacheCtx, cacheCancel := e.cacheAttempt(ctx)
		defer cacheCancel()
		var epoch string
		epoch, err = e.epoch(cacheCtx)
		var result *query.Result
		if err == nil {
			result, err = e.cachedBoard(cacheCtx, q, epoch, version)
		}
		if errors.Is(err, errStaleProjection) {
			// Rebuilding still requires the exclusive metric lock and a fresh
			// version; mutations may have committed since the initial SELECT.
			tx, err = e.db.Begin(ctx)
			if err != nil {
				return nil, err
			}
			defer tx.Rollback(ctx)
			version, err = lockProjectionVersion(ctx, tx, q.Metric, false)
			if err != nil {
				return nil, err
			}
			if err == nil {
				epoch, err = e.epoch(cacheCtx)
			}
			if err == nil {
				err = e.rebuildMetricWithCacheContext(ctx, cacheCtx, tx, q.Metric, version, epoch)
			}
			if err == nil {
				result, err = e.cachedBoard(cacheCtx, q, epoch, version)
			}
		}
		if err == nil {
			return result, nil
		}
		e.refreshCacheTopology(cacheCtx)
		slog.Debug("using PostgreSQL ranking", "error", err)
	}
	// SQL fallbacks contain several statements. Hold the metric lock across
	// those statements so count, score, rank and pagination share one state.
	if tx == nil {
		var err error
		tx, err = e.db.Begin(ctx)
		if err != nil {
			return nil, err
		}
		defer tx.Rollback(ctx)
		if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))`, EncodeString(q.Metric)); err != nil {
			return nil, err
		}
	}
	if q.Type == query.QueryTypeGroupLeaderboard {
		return e.groups(ctx, tx, q, nil)
	}

	return e.sqlBoard(ctx, tx, q)
}

var readProjection = redis.NewScript(`
if redis.call('HGET',KEYS[1],'epoch')~=ARGV[1] or redis.call('HGET',KEYS[1],'version')~=ARGV[2] then return false end
local size=redis.call('ZCARD',KEYS[2])
local expected=tonumber(redis.call('HGET',KEYS[1],ARGV[3]) or '0')
if size~=expected or redis.call('HLEN',KEYS[3])~=expected then return false end
local rank=-1
local member=''
local start=tonumber(ARGV[5])
local stop=start+tonumber(ARGV[6])-1
if ARGV[4]~='LEADERBOARD' and ARGV[4]~='LEADERBOARD_COUNT' then
 member=redis.call('HGET',KEYS[3],ARGV[7]) or ''
 if member~='' then
  local found=redis.call('ZRANK',KEYS[2],member)
  if not found then return false end
  rank=found
 end
 start=math.max(0,rank-tonumber(ARGV[8]));stop=rank+tonumber(ARGV[8])
end
local result={size,rank,member,start}
local bytes=string.len(member)+128
if bytes>tonumber(ARGV[9]) then return redis.error_reply('ranking exceeds configured byte limit') end
if ARGV[4]=='LEADERBOARD' or (ARGV[4]=='AROUND' and rank>=0) then
 for i=start,math.min(stop,size-1) do
  local value=redis.call('ZRANGE',KEYS[2],i,i)[1]
  bytes=bytes+string.len(value)+64
  if bytes>tonumber(ARGV[9]) then return redis.error_reply('ranking exceeds configured byte limit') end
  table.insert(result,value)
 end
end
return result
`)

func (e *Engine) cachedBoard(ctx context.Context, q *query.Query, epoch string, version int64) (*query.Result, error) {
	key := boardKey(q.Metric, q.Board)
	meta := metricKey(q.Metric) + ":meta"
	limit := q.Limit
	if limit == 0 {
		limit = e.max + 1
	}
	raw, err := readProjection.Run(ctx, e.cache, []string{meta, key + ":z", key + ":h"}, epoch, strconv.FormatInt(version, 10), key, string(q.Type), q.Offset, limit, q.EntityID, q.Radius, e.maxBytes).Result()
	if errors.Is(err, redis.Nil) {
		return nil, errStaleProjection
	}
	if err != nil {
		return nil, err
	}
	values, ok := raw.([]any)
	if !ok || len(values) < 4 {
		return nil, errStaleProjection
	}
	size, ok := values[0].(int64)
	if !ok {
		return nil, fmt.Errorf("invalid projection count")
	}
	position, ok := values[1].(int64)
	if !ok {
		return nil, fmt.Errorf("invalid projection rank")
	}
	member, ok := values[2].(string)
	if !ok {
		return nil, fmt.Errorf("invalid projection member")
	}
	start, ok := values[3].(int64)
	if !ok {
		return nil, fmt.Errorf("invalid projection offset")
	}
	members := make([]string, 0, len(values)-4)
	for _, v := range values[4:] {
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("invalid projection entry")
		}
		members = append(members, s)
	}
	if q.Type == query.QueryTypeLeaderboard {
		if len(members) > e.max {
			return nil, fmt.Errorf("leaderboard result exceeds configured limit")
		}
		entries := []types.LeaderboardEntry{}
		for _, member := range members {
			entry, err := decodeMember(member)
			if err != nil {
				return nil, err
			}
			entries = append(entries, entry)
		}
		return &query.Result{Leaderboard: entries}, nil
	}
	result := &query.RankingResult{Count: size}
	if q.Type == query.QueryTypeAround {
		result.Around = []query.RankedEntry{}
	}
	if q.Type == query.QueryTypeLeaderboardCount {
		return &query.Result{Ranking: result}, nil
	}
	if position < 0 {
		return &query.Result{Ranking: result}, nil
	}
	entry, err := decodeMember(member)
	if err != nil {
		return nil, err
	}
	rank := position + 1
	result.Rank = &rank
	result.Score = &entry.Value
	if q.Type == query.QueryTypeAround {
		for i, member := range members {
			entry, err := decodeMember(member)
			if err != nil {
				return nil, err
			}
			result.Around = append(result.Around, query.RankedEntry{EntityID: entry.EntityID, Value: entry.Value, Rank: start + int64(i) + 1})
		}
	}
	return &query.Result{Ranking: result}, nil
}

const scoreOrder = `value DESC,CASE WHEN value>=0 THEN flame_entity_bytes(entity) END ASC,CASE WHEN value<0 THEN flame_entity_bytes(entity) END DESC`

func (e *Engine) sqlEntries(ctx context.Context, tx pgx.Tx, q *query.Query, offset, limit int) ([]types.LeaderboardEntry, error) {
	rows, err := tx.Query(ctx, `SELECT entity,value FROM flame_scores WHERE flame_identity_hash(metric)=flame_identity_hash($1) AND metric=$1 AND flame_identity_hash(board)=flame_identity_hash($2) AND board=$2 ORDER BY `+scoreOrder+` LIMIT $3 OFFSET $4`, EncodeString(q.Metric), EncodeString(q.Board), limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	entries := []types.LeaderboardEntry{}
	bytes := 0
	for rows.Next() {
		var entry types.LeaderboardEntry
		if err = rows.Scan(&entry.EntityID, &entry.Value); err != nil {
			return nil, err
		}
		bytes += len(entry.EntityID) + 64
		if bytes > e.maxBytes {
			return nil, fmt.Errorf("leaderboard exceeds configured byte limit")
		}
		entry.EntityID, err = DecodeString(entry.EntityID)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, rows.Err()
}
func (e *Engine) sqlBoard(ctx context.Context, tx pgx.Tx, q *query.Query) (*query.Result, error) {
	if q.Type == query.QueryTypeLeaderboard {
		limit := q.Limit
		if limit == 0 {
			limit = e.max + 1
		}
		entries, err := e.sqlEntries(ctx, tx, q, q.Offset, limit)
		if err != nil {
			return nil, err
		}
		if len(entries) > e.max {
			return nil, fmt.Errorf("leaderboard result exceeds configured limit")
		}
		return &query.Result{Leaderboard: entries}, nil
	}
	result := &query.RankingResult{}
	if q.Type == query.QueryTypeAround {
		result.Around = []query.RankedEntry{}
	}
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM flame_scores WHERE flame_identity_hash(metric)=flame_identity_hash($1) AND metric=$1 AND flame_identity_hash(board)=flame_identity_hash($2) AND board=$2`, EncodeString(q.Metric), EncodeString(q.Board)).Scan(&result.Count); err != nil {
		return nil, err
	}
	if q.Type == query.QueryTypeLeaderboardCount {
		return &query.Result{Ranking: result}, nil
	}
	var value float64
	err := tx.QueryRow(ctx, `SELECT value FROM flame_scores WHERE flame_identity_hash(metric)=flame_identity_hash($1) AND metric=$1 AND flame_identity_hash(board)=flame_identity_hash($2) AND board=$2 AND flame_identity_hash(entity)=flame_identity_hash($3) AND entity=$3`, EncodeString(q.Metric), EncodeString(q.Board), EncodeString(q.EntityID)).Scan(&value)
	if err == pgx.ErrNoRows {
		return &query.Result{Ranking: result}, nil
	}
	if err != nil {
		return nil, err
	}
	var rank int64
	err = tx.QueryRow(ctx, `SELECT count(*)+1 FROM flame_scores WHERE flame_identity_hash(metric)=flame_identity_hash($1) AND metric=$1 AND flame_identity_hash(board)=flame_identity_hash($2) AND board=$2 AND (value > $3 OR (value=$3 AND (($3>=0 AND flame_entity_bytes(entity) < flame_entity_bytes($4)) OR ($3<0 AND flame_entity_bytes(entity) > flame_entity_bytes($4)))))`, EncodeString(q.Metric), EncodeString(q.Board), value, EncodeString(q.EntityID)).Scan(&rank)
	if err != nil {
		return nil, err
	}
	result.Rank = &rank
	result.Score = &value
	if q.Type == query.QueryTypeAround {
		offset := int(rank) - 1 - q.Radius
		if offset < 0 {
			offset = 0
		}
		limit := int(rank) - 1 + q.Radius - offset + 1
		entries, err := e.sqlEntries(ctx, tx, q, offset, limit)
		if err != nil {
			return nil, err
		}
		for i, entry := range entries {
			result.Around = append(result.Around, query.RankedEntry{EntityID: entry.EntityID, Value: entry.Value, Rank: int64(offset + i + 1)})
		}
	}
	return &query.Result{Ranking: result}, nil
}
func (e *Engine) groups(ctx context.Context, tx pgx.Tx, q *query.Query, window []types.LeaderboardEntry) (*query.Result, error) {
	if len(q.Groups) > e.max {
		return nil, fmt.Errorf("too many groups")
	}
	scores := map[string]float64{}
	for _, entry := range window {
		scores[entry.EntityID] = entry.Value
	}
	entries := []types.LeaderboardEntry{}
	members := 0
	bytes := 0
	for _, group := range q.Groups {
		bytes += len(group.Name) + 64
		if bytes > e.maxBytes {
			return nil, fmt.Errorf("group leaderboard exceeds configured byte limit")
		}
		sum := float64(0)
		for _, id := range group.Members {
			members++
			if members > e.max {
				return nil, fmt.Errorf("group members exceed configured limit")
			}
			value, known := scores[id]
			if !known && tx != nil {
				err := tx.QueryRow(ctx, `SELECT value FROM flame_scores WHERE flame_identity_hash(metric)=flame_identity_hash($1) AND metric=$1 AND flame_identity_hash(board)=flame_identity_hash($2) AND board=$2 AND flame_identity_hash(entity)=flame_identity_hash($3) AND entity=$3`, EncodeString(q.Metric), EncodeString(q.Board), EncodeString(id)).Scan(&value)
				if err != nil && err != pgx.ErrNoRows {
					return nil, err
				}
				scores[id] = value
			}
			sum += value
		}
		if math.IsInf(sum, 0) || math.IsNaN(sum) {
			return nil, fmt.Errorf("response serialization failed")
		}
		entries = append(entries, types.LeaderboardEntry{EntityID: group.Name, Value: sum})
	}
	sortEntries(entries, false)
	return &query.Result{Leaderboard: page(entries, q.Limit, q.Offset)}, nil
}
