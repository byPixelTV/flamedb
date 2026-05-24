cat > /mnt/user-data/outputs/FLAMEDB_INSTRUCTIONS.md << 'HEREDOC'
# FlameDB — Project Instructions for Coding Agents

## What is FlameDB?

FlameDB is a lightweight, distributed time-series and event database written in Go, designed as an open-source alternative to ClickHouse, InfluxDB, and TimescaleDB. The goal is to be fast, simple, and cluster-ready out of the box — with leaderboards, aggregates, tag filtering, and graph-ready time-series as first-class features.

Target use cases: Minecraft server networks (player stats, kills, deaths, money, TPS, player counts), but designed to be general-purpose. Designed to handle donut SMP scale: 55k concurrent players, millions of writes/day, billions of events over time.

**Module path:** `github.com/byPixelTV/flamedb`  
**Language:** Go (latest)  
**Storage engine:** [Pebble](https://github.com/cockroachdb/pebble) (RocksDB-compatible, pure Go, embedded)  
**Config format:** YAML  
**Protocol:** TCP, newline-delimited JSON responses, custom query language  

---

## Project Structure

```
flamedb/
├── cmd/
│   └── flamedb/
│       └── main.go                   # entrypoint
├── internal/
│   ├── aggregates/
│   │   └── leaderboard.go            # pre-computed leaderboards with per-entity mutex
│   ├── auth/
│   │   └── auth.go                   # API key auth + read/write permissions
│   ├── cluster/
│   │   ├── cluster.go                # cluster state, node management, forwarding
│   │   ├── ring.go                   # consistent hashing ring with virtual nodes
│   │   ├── discovery.go              # seed-based join, gossip propagation, heartbeat
│   │   ├── replication.go            # async replication + quorum writes
│   │   └── rebalance.go              # data rebalancing when new node joins
│   ├── config/
│   │   └── config.go                 # YAML config loading
│   ├── query/
│   │   ├── query.go                  # query types + structs
│   │   ├── parser.go                 # query string → Query struct
│   │   └── executor.go               # Query → Result, GetAllMetrics
│   ├── server/
│   │   └── server.go                 # TCP server, connection handling, CLUSTER commands
│   └── storage/
│       ├── storage.go                # pebble wrapper, read/write events, export/import
│       ├── event.go                  # Event struct
│       └── cardinality.go            # cardinality tracking for tag index selection
├── config.yml                        # node-1 config (seed node)
├── config2.yml                       # node-2 config
├── config3.yml                       # node-3 config
├── config4.yml                       # node-4 config (example for adding nodes)
└── go.mod
```

---

## Query Language

FlameDB uses a custom query language over TCP. Every connection requires auth first.

### Auth
```
← {"auth":"required"}
→ AUTH <api_key>
← {"auth":"ok","name":"eramc"}
```

### Commands

#### WRITE
```
WRITE <metric> <value> [lb="<entityID>"] [tag="value"] [ts=<unix_nano>] [QUORUM] [__replica]
```
- Writes a raw event to storage
- If `lb=` is set, also increments the leaderboard for that entity
- `ts=` overrides timestamp (default: now)
- `QUORUM` — wait for majority of replicas to confirm before returning ok
- `__replica` — internal flag, marks write as replica write (no further replication)

#### GET
```
GET <metric> [WHERE key="value" AND key2="value2"] [FROM YYYY-MM-DD] [TO YYYY-MM-DD] [LIMIT n] [OFFSET n] [ORDER ASC|DESC]
```
- Returns raw events, filtered by tags and time range
- Tag filtering uses cardinality-based secondary index (lowest cardinality tag used as primary index)
- Supports multiple WHERE clauses with AND

#### LEADERBOARD
```
LEADERBOARD <metric> [LIMIT n] [OFFSET n]
```
- Returns pre-computed sorted leaderboard entries
- Instant regardless of data size (aggregates on write, not on query)
- Supports pagination

#### SET
```
SET <metric> <value> [lb="<entityID>"]
```
- Sets an absolute value (not increment) on the leaderboard

#### DELETE
```
DELETE <metric> [lb="<entityID>"] [FROM YYYY-MM-DD] [TO YYYY-MM-DD]
```
- Deletes a leaderboard entry or raw events in a time range

#### STATS
```
STATS <metric> TAGS <tag1> <tag2> ...
```
- Returns cardinality stats for the given tags on a metric
- Useful for understanding index selection and query performance

#### CLUSTER (internal)
```
CLUSTER {"type":"JOIN","node_id":"node-2","addr":"192.168.1.2:7778"}
```
- Internal command for node discovery
- Response includes `peers` list so joining node learns about all existing nodes

#### CLUSTER_METRICS (internal)
- Returns JSON array of all metric names stored on this node
- Used during rebalancing

#### CLUSTER_EXPORT <metric> (internal)
- Returns all raw pebble KV data for a metric (events + leaderboard)
- Used during rebalancing

#### GROUP_LEADERBOARD
```
GROUP_LEADERBOARD <metric> GROUP "group1:member1,member2" GROUP "group2:memberA,memberB" [LIMIT n] [OFFSET n]
```

---

## Config Format

```yaml
auth:
  keys:
    - name: "eramc"
      key: "flame_abc123"
      permissions: ["read", "write"]
    - name: "readonly-dashboard"
      key: "flame_xyz789"
      permissions: ["read"]

server:
  port: 7777
  host: "0.0.0.0"
  advertise_addr: "192.168.178.199:7777"  # IP that other nodes use to reach this node
  node_id: "node-1"
  data_path: "./data-node1"

cluster:
  seeds: []                  # node-1 has no seeds, it IS the seed
  replication_factor: 3      # how many nodes store each metric (1 = no replication)
```

- `seeds` — list of existing node addresses to announce to on startup. Empty for the first node.
- `replication_factor: 1` — single node mode, no replication, works standalone
- `advertise_addr` — must be set to the actual IP, not `0.0.0.0`, for multi-node clusters

---

## Architecture

### Storage Layer (`internal/storage`)
- Wraps Pebble (embedded LSM-tree key-value store)
- **Event key format:** `metric:timestamp` (big-endian uint64 for correct range scan ordering)
- **Secondary index key format:** `idx:metric:tagkey:tagvalue:timestamp` → primary key
- **Leaderboard key format:** `lb:metric:inverted_score:entityID` (inverted for descending order)
- **Cardinality key format:** `card:metric:tagkey:tagvalue` and `card-count:metric:tagkey`
- Export/import methods for raw KV data (used in rebalancing)

### Leaderboard Layer (`internal/aggregates`)
- Pre-computed sorted leaderboards stored in same Pebble instance
- Aggregate on write (Increment/Set/Delete), never on query → instant reads regardless of data size
- Per-entity mutex via `sync.Map` to prevent read-modify-write race conditions
- Uses Pebble Batch for atomic delete-old-key + set-new-key operations

### Query Layer (`internal/query`)
- `parser.go`: tokenizer (handles quoted strings, `=` as separate token) + parser
- `executor.go`: executes Query against storage + leaderboard
- `GetAllMetrics()`: scans pebble for all metric names (used by rebalancing)
- Cardinality-based tag selection: lowest cardinality tag used as primary index for GET+WHERE

### Auth Layer (`internal/auth`)
- API key → Session mapping loaded at startup
- Session has permissions map: `read`, `write`
- Write commands (WRITE, SET, DELETE) require `write` permission
- Read commands (GET, LEADERBOARD, STATS) require `read` permission

### Cluster Layer (`internal/cluster`)

#### consistent hashing (`ring.go`)
- 150 virtual nodes per physical node by default
- `Get(metric)` → primary node
- `GetN(metric, n)` → N nodes (primary + replicas), deduplicated
- Duplicate node detection in `Add()` prevents double-registration
- Thread-safe with `sync.RWMutex`

#### node management (`cluster.go`)
- `IsLocal(metric)` — true if this node is primary OR replica for this metric
- `IsPrimaryFor(metric)` — true if this node is the first node in the ring for this metric
- `GetReplicaNodes(metric)` — all nodes except primary
- `GetReadNode(metric)` — round-robin over all replica nodes for load balancing
- `ForwardWithFailover(metric, query)` — tries each replica in order until one succeeds
- `ForwardToPrimary(metric, query)` — sends write to primary node
- `SendToNode(node, query)` — sends query to specific node via connection pool
- `GetAllNodes()` — returns all known nodes (used in JOIN response)
- `Knows(nodeID)` — checks if node is in ring

#### discovery (`discovery.go`)
- Seed-based join: node announces itself to seed nodes on startup
- JOIN response includes full peer list so new node learns about all existing nodes
- Gossip propagation: when node A learns about node C, it tells all other known nodes
- Loop prevention: `Knows()` check before propagating
- Heartbeat every 5 seconds, node removed from ring after 3 consecutive failures (`recordFailure`)
- `clearFailure()` resets failure count when node responds

#### replication (`replication.go`)
- `ReplicateAsync(metric, query)` — fire and forget to all replica nodes
- `ReplicateQuorum(metric, query)` — waits until majority (n/2+1) of nodes confirm
- `ReplicateWrite(metric, query, quorum)` — picks async or quorum based on flag
- Replica writes tagged with `__replica` suffix to prevent replication loops

#### rebalancing (`rebalance.go`)
- Triggered on startup when seeds are configured (i.e. not the first node)
- Waits 2 seconds after join for gossip to propagate
- Asks each known node for their metric list via `CLUSTER_METRICS`
- For each metric that now belongs to this node as primary: requests full data via `CLUSTER_EXPORT`
- `HasMetric(metric)` check to skip metrics already present locally
- **KNOWN BUG:** rebalancing currently logs "starting rebalance" but `rebalanceFromNode` gets no results — `CLUSTER_METRICS` and `CLUSTER_EXPORT` are handled in server.go but the response parsing in `rebalance.go` may be receiving the auth handshake response instead of the actual data. The pool connection reuse during rebalance likely causes the scanner to read a stale response. Fix: rebalance should open a fresh connection instead of using the pool, OR the pool needs to handle the auth handshake transparently for internal commands.

### Server Layer (`internal/server`)
- TCP server, one goroutine per connection
- Auth handshake on every new connection (`{"auth":"required"}` → `AUTH key` → `{"auth":"ok"}`)
- Session nil check after auth loop (handles disconnects during handshake)
- CLUSTER command handling before query parsing
- Write path:
  1. If not local → `ForwardWithFailover`
  2. If local but not primary and not replica → `ForwardToPrimary`
  3. If primary → execute locally → `ReplicateWrite` to replicas
  4. If replica (tagged `__replica`) → execute locally, no further replication
- Read path: `GetReadNode` for round-robin load balancing across replicas
- `store` field exposed on Server for `CLUSTER_EXPORT` handling

---

## What is Already Implemented ✅

- [x] Pebble-backed event storage with time-range reads
- [x] Pre-computed leaderboards with pagination
- [x] Custom query language: WRITE, GET, LEADERBOARD, SET, DELETE, STATS
- [x] Tag filtering with multiple AND clauses
- [x] Cardinality-based secondary index for tag filtering (lowest cardinality = primary index)
- [x] FROM/TO date range filtering
- [x] LIMIT/OFFSET pagination
- [x] Timestamp override on WRITE (`ts=`)
- [x] TCP server with newline-delimited JSON protocol
- [x] API key auth with read/write permissions
- [x] Consistent hashing ring for metric-based sharding (150 virtual nodes)
- [x] Automatic query forwarding between nodes with failover
- [x] Seed-based dynamic node discovery (no hardcoded cluster topology)
- [x] Gossip propagation (new nodes announced to all known nodes)
- [x] Full peer list exchange on JOIN (new node learns all existing nodes)
- [x] Heartbeat with failure detection (remove after 3 failures)
- [x] Graceful shutdown (SIGINT/SIGTERM)
- [x] Per-entity mutex for leaderboard race condition prevention
- [x] Atomic leaderboard updates via Pebble Batch
- [x] Config path as CLI argument
- [x] `advertise_addr` config (separate bind addr from advertised addr)
- [x] Eventual consistency replication (async, fire-and-forget to replicas)
- [x] Quorum writes (`QUORUM` flag, waits for majority confirmation)
- [x] `__replica` flag to prevent replication loops
- [x] Failover reads (if primary down, try replicas)
- [x] Round-robin read load balancing across replicas
- [x] Connection pooling with retry on dead connections
- [x] Rebalancing infrastructure (CLUSTER_METRICS, CLUSTER_EXPORT, CLUSTER_IMPORT commands, RebalanceStore interface, storage export/import methods)
- [x] Single-node mode (replication_factor: 1, works standalone with no cluster config)
- [x] Ad-hoc group leaderboards (GROUP_LEADERBOARD, per-query groups, sum aggregation)

---

## What is NOT Yet Implemented / Broken ❌

### Query Language
- [ ] **ORDER ASC/DESC** for GET queries (currently GET doesn't sort, only leaderboard is sorted)
- [ ] **COUNT aggregation** — `GET kills COUNT` to count events
- [ ] **SUM/AVG aggregation** — for dashboard graph data
- [ ] **GROUP BY time bucket** — e.g. `GROUP BY 1h` for time-series graphs
- [ ] **Multi-metric queries** — `GET kills, deaths WHERE player="pixel"`
- [ ] **Relative time** — `FROM now-7d TO now` instead of absolute dates

### SDKs (not started)
- [ ] **Kotlin/Java SDK** — primary target, for Minecraft plugin devs
- [ ] **Go SDK** — simple wrapper around the TCP protocol
- [ ] **C# SDK** — for Unity / .NET users

### Operational
- [ ] **HTTP API** — optional REST/JSON API layer on top of TCP for dashboards and web clients
- [ ] **`__internal` metrics** — expose FlameDB's own stats (queries/sec, storage size, node count)
- [ ] **Backup/restore** — snapshot Pebble data to S3 or local disk
- [ ] **Data TTL** — auto-expire raw events older than X days (leaderboards kept forever)
- [ ] **Compression** — Pebble supports block compression, should be enabled and configurable
- [ ] **TLS** — encrypt node-to-node and client-to-node traffic
- [ ] **Dashboard UI** — web-based dashboard for graphs and leaderboards

### Performance
- [ ] **Write batching** — buffer writes and flush in batches for higher throughput
- [ ] **Read cache** — LRU cache for frequently-read leaderboards
- [ ] **Benchmark suite** — test with 1000 concurrent writers, measure p99 latency

---

## Known Issues / Tech Debt

- Leaderboard lookups use an O(1) lb-entity index (self-heal on first read), falls back to scan for legacy data
- Config is loaded once at startup, no hot reload
- Internal node-to-node auth uses the first API key from config — should have a dedicated `internal_key` config field

---

## Development Setup

### Requirements
- Go 1.22+
- Git
- Windows with WSL recommended for testing (use WSL for nc commands, Windows for running the exe)

### Build & Run
```powershell
# build
go build -o flamedb.exe cmd/flamedb/main.go

# node-1 (seed node)
.\flamedb.exe config.yml

# node-2 (joins node-1)
.\flamedb.exe config2.yml

# node-3
.\flamedb.exe config3.yml
```

### Testing with nc (from WSL)
```bash
# write with leaderboard update
printf 'AUTH flame_abc123\nWRITE kills 10 lb="pixel" player="pixel" region="eu"\n' | nc 192.168.178.199 7777

# quorum write
printf 'AUTH flame_abc123\nWRITE kills 10 lb="pixel" QUORUM\n' | nc 192.168.178.199 7777

# leaderboard (should return same result from all nodes if replication works)
printf 'AUTH flame_abc123\nLEADERBOARD kills LIMIT 10 OFFSET 0\n' | nc 192.168.178.199 7777
printf 'AUTH flame_abc123\nLEADERBOARD kills LIMIT 10 OFFSET 0\n' | nc 192.168.178.199 7778
printf 'AUTH flame_abc123\nLEADERBOARD kills LIMIT 10 OFFSET 0\n' | nc 192.168.178.199 7779

# tag stats (shows cardinality for index selection)
printf 'AUTH flame_abc123\nSTATS kills TAGS player region\n' | nc 192.168.178.199 7777

# GET with tag filter
printf 'AUTH flame_abc123\nGET kills WHERE region="eu" AND player="pixel" FROM 2020-01-01 TO 2030-01-01\n' | nc 192.168.178.199 7777
```

### Reset data between tests
```powershell
Remove-Item -Recurse -Force data-node1, data-node2, data-node3, data-node4
```

### Dependencies
```
github.com/cockroachdb/pebble  — storage engine
gopkg.in/yaml.v3               — config parsing
```

---

## Important Design Decisions

- **Eventual consistency by default** — writes return ok after primary write, replicas updated async. Use `QUORUM` flag for stronger guarantees.
- **Cardinality-based index selection** — when filtering by multiple tags, the tag with lowest cardinality (fewest unique values) is used as the primary index. This is tracked automatically on every write.
- **Consistent hashing** — metric name determines which node is primary. Different metrics land on different nodes naturally distributing load.
- **No coordinator** — fully peer-to-peer, no single point of failure in routing layer.
- **Single node mode** — `replication_factor: 1` and empty `seeds` = standalone mode, no cluster overhead.
