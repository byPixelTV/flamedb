# FlameDB — Project Instructions for Coding Agents

## What is FlameDB?

FlameDB is a lightweight, distributed time-series and event database written in Go, designed as an open-source alternative to ClickHouse, InfluxDB, and TimescaleDB. The goal is to be fast, simple, and cluster-ready out of the box — with leaderboards, aggregates, tag filtering, and graph-ready time-series as first-class features.

Performance target: support 100k ops/s or more on write-heavy workloads.

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
│   │   ├── pool.go                   # TCP connection pool with auth + retry
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
├── config4.yml                       # node-4 config
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
GET <metric>[,metric2,...] [WHERE key="value" AND key2="value2"] [FROM YYYY-MM-DD] [TO YYYY-MM-DD] [LIMIT n] [OFFSET n] [ORDER ASC|DESC]
```
- Returns raw events, filtered by tags and time range
- Supports comma-separated multi-metric queries
- Tag filtering uses cardinality-based secondary index
- Supports multiple WHERE clauses with AND

#### LEADERBOARD
```
LEADERBOARD <metric> [LIMIT n] [OFFSET n]
```
- Returns pre-computed sorted leaderboard entries
- Instant regardless of data size (aggregates on write, not on query)
- Supports pagination

#### GROUP_LEADERBOARD
```
GROUP_LEADERBOARD <metric> GROUP "groupname:member1,member2" GROUP "groupname2:memberA" [LIMIT n] [OFFSET n]
```
- Ad-hoc group leaderboards, sums member values per group
- Groups defined per-query, no pre-configuration needed

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
CLUSTER {"type":"CLUSTER_METRICS"}
CLUSTER {"type":"CLUSTER_EXPORT","metric":"kills"}
CLUSTER {"type":"SET_CONFIG","replication_factor":3}
```
- Internal command for node discovery, config sync, and rebalancing
- JOIN response includes full peer list so joining node learns all existing nodes

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
  advertise_addr: "192.168.178.199:7777"  # actual IP other nodes use to reach this node
  node_id: "node-1"
  data_path: "./data-node1"

cluster:
  seeds: []                  # empty = this is the seed node
  replication_factor: 3      # 1 = no replication (single node mode)
```

---

## Architecture

### Storage Layer (`internal/storage`)
- Wraps Pebble (embedded LSM-tree key-value store)
- **Event key format:** `metric:timestamp` (big-endian uint64 for correct range scan ordering)
- **Secondary index key format:** `idx:metric:tagkey:tagvalue:timestamp` → primary key
- **Leaderboard key format:** `lb:metric:inverted_score:entityID` (inverted for descending order)
- **Cardinality key format:** `card:metric:tagkey:tagvalue` and `card-count:metric:tagkey`
- `ExportMetricData(metric)` — exports all raw KV pairs for a metric (events + leaderboard)
- `ImportRebalanceData(data)` — bulk imports raw KV pairs via pebble batch
- `HasMetric(metric)` — checks if any events exist for a metric locally

### Leaderboard Layer (`internal/aggregates`)
- Pre-computed sorted leaderboards in same Pebble instance
- Aggregate on write (Increment/Set/Delete), never on query → instant reads
- Per-entity mutex via `sync.Map` to prevent read-modify-write race conditions
- Uses Pebble Batch for atomic delete-old-key + set-new-key operations

### Query Layer (`internal/query`)
- `parser.go`: tokenizer + parser, handles quoted strings, `=` as separate token, standalone flags (QUORUM, __replica)
- `executor.go`: executes Query against storage + leaderboard, `GetAllMetrics()` for rebalancing
- Cardinality-based tag selection: lowest cardinality tag = primary index for GET+WHERE

### Auth Layer (`internal/auth`)
- API key → Session mapping loaded at startup
- `read` permission: GET, LEADERBOARD, GROUP_LEADERBOARD, STATS
- `write` permission: WRITE, SET, DELETE

### Cluster Layer (`internal/cluster`)

#### consistent hashing (`ring.go`)
- 150 virtual nodes per physical node
- `Get(metric)` → primary node
- `GetN(metric, n)` → N nodes deduplicated (primary + replicas)
- Duplicate node detection in `Add()` prevents double-registration
- Thread-safe with `sync.RWMutex`

#### node management (`cluster.go`)
- `IsLocal(metric)` — true if this node is primary OR replica
- `IsPrimaryFor(metric)` — true if first node in ring for this metric
- `GetReplicaNodes(metric)` — all nodes except primary
- `GetReadNode(metric)` — round-robin over all nodes for read load balancing
- `ForwardWithFailover` — tries each replica until one succeeds
- `ForwardToPrimary` — sends write to primary node
- `SendToNode` — sends to specific node via pool
- `GetAllNodes()` — all known nodes
- `Knows(nodeID)` — checks ring membership
- `rebalancing sync.Map` — prevents duplicate metric imports during rebalance

#### connection pool (`pool.go`)
- Persistent authenticated TCP connections per node
- Auto-evicts and reconnects on dead connections
- `Send(node, query)` — thread-safe send with retry on failure

#### discovery (`discovery.go`)
- Seed-based join: announces to seeds, gets full peer list in response
- Gossip: new node announced to all known peers, loop prevention via `Knows()`
- Heartbeat every 5s, node removed after 3 consecutive failures
- `DiscoveryMessage` struct handles all internal message types

#### replication (`replication.go`)
- `ReplicateAsync` — fire and forget to all replicas
- `ReplicateQuorum` — waits for majority (n/2+1) confirmation
- `ReplicateWrite` — dispatches to async or quorum based on flag
- `__replica` suffix on forwarded writes prevents replication loops

#### rebalancing (`rebalance.go`)
- Triggered on startup when seeds configured (not first node)
- Waits 2s for gossip to settle
- Opens **fresh dedicated TCP connections** (not pool) to avoid race conditions
- Uses **64MB scanner buffer** for large metric exports
- Per-metric `rebalancing sync.Map` lock prevents duplicate imports from parallel goroutines
- Flow: connect → auth → CLUSTER_METRICS → for each local metric not yet present → CLUSTER_EXPORT → import

### Server Layer (`internal/server`)
- TCP server, one goroutine per connection
- Auth handshake → session nil check → main loop
- CLUSTER command handling before query parsing
- Write path: not local → ForwardWithFailover | local+not primary+not replica → ForwardToPrimary | primary → execute+ReplicateWrite | replica → execute only
- Read path: GetReadNode for round-robin load balancing
- `store` field on Server for CLUSTER_EXPORT handling

---

## What is Already Implemented ✅

- [x] Pebble-backed event storage with time-range reads
- [x] Pre-computed leaderboards with pagination
- [x] Custom query language: WRITE, GET, LEADERBOARD, GROUP_LEADERBOARD, SET, DELETE, STATS
- [x] Multi-metric GET (comma-separated)
- [x] Ad-hoc group leaderboards (GROUP_LEADERBOARD)
- [x] Tag filtering with multiple AND clauses
- [x] Cardinality-based secondary index for tag filtering
- [x] FROM/TO date range filtering
- [x] LIMIT/OFFSET pagination
- [x] Timestamp override on WRITE
- [x] TCP server with newline-delimited JSON protocol
- [x] API key auth with read/write permissions
- [x] Consistent hashing ring (150 virtual nodes)
- [x] Automatic query forwarding with failover
- [x] Seed-based dynamic node discovery
- [x] Gossip propagation with loop prevention
- [x] Full peer list exchange on JOIN
- [x] Heartbeat with failure detection (3 strikes)
- [x] Graceful shutdown (SIGINT/SIGTERM)
- [x] Per-entity mutex for leaderboard race condition prevention
- [x] Atomic leaderboard updates via Pebble Batch
- [x] Config path as CLI argument
- [x] `advertise_addr` config
- [x] Eventual consistency replication (async)
- [x] Quorum writes (QUORUM flag)
- [x] Failover reads
- [x] Round-robin read load balancing
- [x] Connection pooling with retry
- [x] **Data rebalancing** — fully working, new node imports metrics it owns from existing nodes
- [x] Per-metric import deduplication during rebalance (sync.Map lock)
- [x] Single-node mode (replication_factor: 1)
- [x] ORDER ASC/DESC for GET queries
- [x] COUNT/SUM/AVG aggregation
- [x] GROUP BY time bucket (e.g. `GROUP BY 1h`)
- [x] Relative time (`FROM now-7d TO now`)
- [x] Pebble block compression
- [x] **Compression** — Pebble block compression configured

---

## What is NOT Yet Implemented ❌

### Next Up
- [ ] **`__internal` metrics** — expose FlameDB's own stats as queryable metrics:
  - `__internal.writes_per_sec` — write throughput
  - `__internal.reads_per_sec` — read throughput
  - `__internal.latency_p99` — query latency percentiles
  - `__internal.storage_bytes` — pebble storage size per node
  - `__internal.node_count` — active nodes in cluster
  - `__internal.replication_lag` — how far behind replicas are
  - Query via normal GET/LEADERBOARD syntax

### SDKs (not started)
- [ ] **Kotlin/Java SDK** — primary target for Minecraft plugin devs
- [ ] **Go SDK** — simple wrapper
- [ ] **C# SDK**

### Operational
- [ ] **Backup/restore** — snapshot to S3 or local disk
- [ ] **Data TTL** — auto-expire raw events older than X days
- [ ] **TLS** — encrypt node-to-node and client traffic
- [ ] **Dashboard UI**

---

## Known Issues / Tech Debt

- `Get` on Leaderboard does full prefix scan to find entity's current value — needs secondary index `lb-entity:metric:entityID → value` for O(1) lookup
- Internal node-to-node auth uses first API key — should have dedicated `internal_key` config field
- Config loaded once at startup, no hot reload
- `CLUSTER_METRICS` and `CLUSTER_EXPORT` bypass permission check — should require valid auth session (they do go through auth handshake but no permission level check)

---

## Development Setup

### Requirements
- Go 1.22+
- Windows with WSL for testing (WSL for nc, Windows for exe)

### Build & Run
```powershell
go build -o flamedb.exe cmd/flamedb/main.go

.\flamedb.exe config.yml   # node-1 (seed)
.\flamedb.exe config2.yml  # node-2
.\flamedb.exe config3.yml  # node-3
.\flamedb.exe config4.yml  # node-4 (joins and rebalances)
```

### Reset between tests
```powershell
Remove-Item -Recurse -Force data-node1, data-node2, data-node3, data-node4
```

### Testing with nc (WSL)
```bash
# write
printf 'AUTH flame_abc123\nWRITE kills 10 lb="pixel" player="pixel" region="eu"\n' | nc 192.168.178.199 7777

# quorum write
printf 'AUTH flame_abc123\nWRITE kills 10 lb="pixel" QUORUM\n' | nc 192.168.178.199 7777

# leaderboard from all nodes (should match if replication works)
for port in 7777 7778 7779 7780; do
  printf 'AUTH flame_abc123\nLEADERBOARD kills LIMIT 3 OFFSET 0\n' | nc 192.168.178.199 $port
done

# cardinality stats
printf 'AUTH flame_abc123\nSTATS kills TAGS player region\n' | nc 192.168.178.199 7777

# group leaderboard
printf 'AUTH flame_abc123\nGROUP_LEADERBOARD kills GROUP "team_red:pixel,notch" GROUP "team_blue:dream"\n' | nc 192.168.178.199 7777
```

### Dependencies
```
github.com/cockroachdb/pebble  — storage engine
gopkg.in/yaml.v3               — config parsing
```

---

## Important Design Decisions

- **Eventual consistency by default** — writes return ok after primary write, replicas async. Use `QUORUM` for stronger guarantees.
- **Cardinality-based index** — lowest cardinality tag used as primary index, tracked automatically on every write.
- **Consistent hashing** — metric name determines primary node. No coordinator, fully peer-to-peer.
- **Single node mode** — `replication_factor: 1` + empty `seeds` = standalone, zero cluster overhead.
- **Rebalancing uses fresh connections** — dedicated TCP connections separate from pool to avoid race conditions with normal traffic.