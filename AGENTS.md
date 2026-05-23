# FlameDB — Project Instructions for Coding Agents

## What is FlameDB?

FlameDB is a lightweight, distributed time-series and event database written in Go, designed as an open-source alternative to ClickHouse, InfluxDB, and TimescaleDB. The goal is to be fast, simple, and cluster-ready out of the box — with leaderboards, aggregates, tag filtering, and graph-ready time-series as first-class features.

Target use cases: Minecraft server networks (player stats, kills, deaths, money, TPS, player counts), but designed to be general-purpose.

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
│       └── main.go               # entrypoint
├── internal/
│   ├── aggregates/
│   │   └── leaderboard.go        # pre-computed leaderboards
│   ├── auth/
│   │   └── auth.go               # API key auth + permissions
│   ├── cluster/
│   │   ├── cluster.go            # cluster state, node management
│   │   ├── ring.go               # consistent hashing ring
│   │   └── discovery.go          # seed-based join + heartbeat
│   ├── config/
│   │   └── config.go             # YAML config loading
│   ├── query/
│   │   ├── query.go              # query types + structs
│   │   ├── parser.go             # query string → Query struct
│   │   └── executor.go           # Query → Result
│   ├── server/
│   │   └── server.go             # TCP server, connection handling
│   └── storage/
│       └── storage.go            # pebble wrapper, read/write events
├── config.yaml                   # node-1 config
├── config2.yaml                  # node-2 config
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
WRITE <metric> <value> [lb="<entityID>"] [tag="value"] [ts=<unix_nano>]
```
- Writes a raw event to storage
- If `lb=` is set, also increments the leaderboard for that entity
- `ts=` overrides timestamp (default: now)

#### GET
```
GET <metric> [WHERE key="value" AND key2="value2"] [FROM YYYY-MM-DD] [TO YYYY-MM-DD] [LIMIT n] [OFFSET n] [ORDER ASC|DESC]
```
- Returns raw events, filtered by tags and time range
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

#### CLUSTER (internal)
```
CLUSTER {"type":"JOIN","node_id":"node-2","addr":"192.168.1.2:7778"}
```
- Internal command for node discovery, not for external clients

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
  node_id: "node-1"
  data_path: "./data-node1"

cluster:
  seeds: []  # node-1 has no seeds, it IS the seed
              # other nodes list seed addresses here: ["192.168.1.1:7777"]
```

---

## Architecture

### Storage Layer (`internal/storage`)
- Wraps Pebble (embedded LSM-tree key-value store)
- Events stored with key format: `metric:timestamp` (big-endian uint64 for correct range scan ordering)
- Values stored as JSON-encoded `Event` structs
- Range scans for GET queries (time-based)

### Leaderboard Layer (`internal/aggregates`)
- Pre-computed sorted leaderboards stored in same Pebble instance
- Key format: `lb:metric:inverted_score:entityID` — inverted score so Pebble's natural key ordering = descending leaderboard
- Aggregate on write (Increment/Set/Delete), never on query → instant reads
- Per-entity mutex via `sync.Map` to prevent read-modify-write race conditions
- Uses Pebble Batch for atomic delete-old-key + set-new-key operations

### Query Layer (`internal/query`)
- `parser.go`: tokenizer + parser, produces `Query` struct
- `executor.go`: executes Query against storage + leaderboard
- Tokenizer handles quoted strings and `=` as separate tokens

### Auth Layer (`internal/auth`)
- API key → Session mapping
- Session has permissions map: `read`, `write`
- Write commands (WRITE, SET, DELETE) require `write` permission
- Read commands (GET, LEADERBOARD) require `read` permission

### Cluster Layer (`internal/cluster`)
- Consistent hashing ring (`ring.go`) with configurable virtual nodes (default: 150)
- Each node hashes metric names to determine ownership
- If a query arrives for a metric owned by another node, it is automatically forwarded
- Forwarding is transparent to the client
- `discovery.go`: seed-based join (like Cassandra) — nodes announce themselves to seeds on startup
- Heartbeat every 5 seconds, node removed from ring after 3 consecutive failures

### Server Layer (`internal/server`)
- TCP server, one goroutine per connection
- Auth handshake on every new connection
- Handles CLUSTER messages for node discovery
- Forwards queries to correct node if not local

---

## What is Already Implemented ✅

- [x] Pebble-backed event storage with time-range reads
- [x] Pre-computed leaderboards with pagination
- [x] Custom query language: WRITE, GET, LEADERBOARD, SET, DELETE
- [x] Tag filtering with multiple AND clauses
- [x] FROM/TO date range filtering
- [x] LIMIT/OFFSET pagination
- [x] Timestamp override on WRITE
- [x] TCP server with newline-delimited JSON protocol
- [x] API key auth with read/write permissions
- [x] Consistent hashing ring for metric-based sharding
- [x] Automatic query forwarding between nodes
- [x] Seed-based dynamic node discovery (no hardcoded cluster topology)
- [x] Heartbeat with failure detection (remove after 3 failures)
- [x] Graceful shutdown (SIGINT/SIGTERM)
- [x] Per-entity mutex for leaderboard race condition prevention
- [x] Atomic leaderboard updates via Pebble Batch
- [x] Config path as CLI argument

---

## What is NOT Yet Implemented ❌

### High Priority
- [ ] **Replication** — if a node dies, its data is gone. Need at least 1 replica per shard. Consider using Raft (hashicorp/raft) or a simpler primary-backup model
- [ ] **Node rejoining** — when a node comes back after being removed from the ring, it needs to re-announce and potentially sync missed data
- [ ] **Data rebalancing** — when a new node joins, metrics that now hash to it should be migrated from the old owner
- [ ] **CLUSTER JOIN propagation** — when node-2 joins node-1, node-1 should tell node-3, node-4 etc. about node-2 (gossip protocol). Currently each node only knows about nodes it directly talked to
- [ ] **Internal auth for node-to-node** — currently uses the first API key for internal comms, should have a dedicated internal key

### Query Language
- [ ] **ORDER ASC** for GET queries (currently only DESC is implemented in leaderboard, GET doesn't sort)
- [ ] **COUNT aggregation** — `GET kills COUNT` to count events instead of returning them
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
- [ ] **Metrics endpoint** — expose FlameDB's own stats (queries/sec, storage size, node count) as a special `__internal` metric namespace
- [ ] **Backup/restore** — snapshot Pebble data to S3 or local disk
- [ ] **Data TTL** — auto-expire raw events older than X days (leaderboards kept forever)
- [ ] **Compression** — Pebble supports block compression, should be enabled and configurable
- [ ] **TLS** — encrypt node-to-node and client-to-node traffic
- [ ] **Dashboard UI** — web-based dashboard for graphs and leaderboards (could be a separate project)

### Performance
- [ ] **Write batching** — buffer writes and flush in batches for higher throughput
- [ ] **Read cache** — LRU cache for frequently-read leaderboards
- [ ] **Index on tags** — currently tag filtering is a full scan of the time range, needs a secondary index for large datasets
- [ ] **Benchmark suite** — test with 1000 concurrent writers, measure p99 latency

---

## Known Issues / Tech Debt

- `log.Printf("scanner error: ...")` fires on normal client disconnects (Windows WSARecv error). Should be filtered more cleanly.
- The `Get` method on Leaderboard does a full prefix scan to find an entity — should use a secondary index `lb-entity:metric:entityID → value` for O(1) lookups instead of O(n) scan.
- Node-to-node forwarding re-authenticates on every query (new TCP connection per forward). Should use persistent connection pooling between nodes.
- Config is loaded once at startup, no hot reload.

---

## Development Setup

### Requirements
- Go 1.22+
- Git

### Running locally (Windows + WSL recommended)
```powershell
# build
go build -o flamedb.exe cmd/flamedb/main.go

# node-1 (seed node, no seeds in config)
.\flamedb.exe config.yaml

# node-2 (points to node-1 as seed)
.\flamedb.exe config2.yaml
```

### Testing with nc (from WSL)
```bash
# write
printf 'AUTH flame_abc123\nWRITE kills 10 lb="pixel" region="eu"\n' | nc 192.168.178.199 7777

# leaderboard
printf 'AUTH flame_abc123\nLEADERBOARD kills LIMIT 10 OFFSET 0\n' | nc 192.168.178.199 7777

# cross-node (kills routes to node-1, queried from node-2)
printf 'AUTH flame_abc123\nLEADERBOARD kills LIMIT 10 OFFSET 0\n' | nc 192.168.178.199 7778
```

### Dependencies
```
github.com/cockroachdb/pebble  — storage engine
gopkg.in/yaml.v3               — config parsing
```