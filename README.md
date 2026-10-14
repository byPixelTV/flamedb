> **Next Generation:** A PostgreSQL/TimescaleDB backend with rebuildable Redis rankings is implemented alongside the legacy deployment. Start it with `docker compose -f compose.nextgen.yml up -d --build`. Read the [deployment, migration and measured performance guide](docs/nextgen.md) and [frozen SDK compatibility contract](docs/compatibility.md). Existing SDK calls remain supported; lowercase raw-event aliases repair the frozen Kotlin/TypeScript casing defect. The 200,000 requests/s engineering target has not been demonstrated by local measurements.

For `REQUEST_TIMEOUT` on leaderboard batches in the existing Pebble deployment,
see the [server batch fix, measurements and rollout notes](docs/pebble-batch-timeouts.md).

The [current Pebble versus Next Gen comparison](docs/backend-comparison.md)
measures both backends with equal local workload and resource limits.

> **Legacy upgrade notice:** Pebble mode automatically migrates old databases after creating a checkpoint under `<data_path>.pre-v2-*/db`. Switching to PostgreSQL uses the separate read-only `cmd/import-postgres` tool; existing data directories are not automatically imported. See the [legacy audit](AUDIT_2026-09-18.md).

<div align="center">
  <h1>FlameDB</h1>
  <p><strong>Fast, lightweight event + time-series database with first-class leaderboards.</strong></p>
</div>

<div align="center">
  <a href="https://github.com/byPixelTV/flamedb/actions/workflows/release.yml">
    <img src="https://github.com/byPixelTV/flamedb/actions/workflows/release.yml/badge.svg" alt="build" />
  </a>
  <a href="https://github.com/byPixelTV/flamedb/releases">
    <img src="https://img.shields.io/github/v/release/byPixelTV/flamedb?display_name=tag" alt="release" />
  </a>
  <a href="https://pkg.go.dev/github.com/byPixelTV/flamedb/pkg/go">
    <img src="https://pkg.go.dev/badge/github.com/byPixelTV/flamedb/pkg/go.svg" alt="go reference" />
  </a>
  <a href="https://www.npmjs.com/package/flamedb-typescript-sdk">
    <img src="https://img.shields.io/npm/v/flamedb-typescript-sdk" alt="npm" />
  </a>
</div>

<div align="center">
  <a href="https://go.dev/"><img src="https://github.com/intergrav/devins-badges/blob/v3/assets/cozy/built-with/go_64h.png?raw=true" height="64" alt="built with go" /></a>
  <a href="https://www.typescriptlang.org/"><img src="https://github.com/intergrav/devins-badges/blob/v3/assets/cozy/built-with/typescript_64h.png?raw=true" height="64" alt="built with typescript" /></a>
  <a href="https://kotlinlang.org/"><img src="https://github.com/intergrav/devins-badges/blob/v3/assets/cozy/built-with/kotlin_64h.png?raw=true" height="64" alt="built with kotlin" /></a>
  <a href="https://adoptium.net/temurin/releases/?version=21"><img src="https://github.com/intergrav/devins-badges/blob/v3/assets/cozy/built-with/java21_64h.png?raw=true" height="64" alt="java 21" /></a>
</div>

<div align="center">
  <a href="https://github.com/byPixelTV/flamedb"><img src="https://github.com/intergrav/devins-badges/blob/v3/assets/cozy/available/github_64h.png?raw=true" height="64" alt="github" /></a>
  <a href="https://www.npmjs.com/package/flamedb-typescript-sdk"><img src="https://github.com/intergrav/devins-badges/blob/v3/assets/cozy/available/npm_64h.png?raw=true" height="64" alt="npm" /></a>
  <a href="https://discord.gg/yVp7Qvhj9k"><img src="https://github.com/intergrav/devins-badges/blob/v3/assets/cozy/social/discord-plural_64h.png?raw=true" height="64" alt="discord" /></a>
</div>

<br />

FlameDB serves universal stats, events, analytics and exact leaderboards through its existing TCP protocol. The shared-storage backend keeps raw events and absolute scores in PostgreSQL/TimescaleDB, projects exact rankings into Redis, and permits multiple API nodes over the same durable data. Namespaced metrics and arbitrary entities remain application-neutral.

## Why FlameDB

- **Instant leaderboards** (pre-aggregated on write)
- **Tag filtering + time ranges** for metrics and events
- **Cluster + replication** out of the box
- **Tiny TCP protocol** with a compact query language

## Quickstart

### 1) Run locally

```bash
go run ./cmd/flamedb config.yml
```

### 2) Minimal config

```yaml
auth:
  keys:
    - name: "eramc"
      key: "flame_abc123"
      permissions: ["read", "write"]

storage:
  compression: "zstd" # none|snappy|zstd

server:
  port: 7777
  host: "0.0.0.0"
  advertise_addr: "127.0.0.1:7777"
  node_id: "node-1"
  data_path: "./data-node1"

cluster:
  seeds: []
  replication_factor: 2
  replication_queue_size: 262144
  fanout_queue_size: 262144
```

## Deploy

### Binary (Linux)

```bash
go build -o flamedb ./cmd/flamedb
./flamedb ./config.yml
```

### Docker

```bash
docker build -t flamedb:local .

docker run --rm \
  -p 7777:7777 \
  -v "$(pwd)/config.yml:/root/config.yml" \
  -v "$(pwd)/data-node1:/root/data-node1" \
  flamedb:local ./app /root/config.yml
```

### Docker (GHCR)

```bash
docker run --rm \
  -p 7777:7777 \
  -v "$(pwd)/config.yml:/root/config.yml" \
  -v "$(pwd)/data-node1:/root/data-node1" \
  ghcr.io/bypixeltv/flamedb:latest ./app /root/config.yml
```

### Docker tags

- `:latest` -> release builds
- `:dev` -> master builds
- `:<version>` -> pinned releases or snapshots

### Cluster (multi-node)

Use `config2.yml`, `config3.yml`, `config4.yml` as templates. Set each node's:

- `server.node_id`
- `server.advertise_addr`
- `server.data_path`
- `cluster.seeds` (point to the seed node)

## SDKs

- Go: `go get github.com/byPixelTV/flamedb/pkg/go`
- TypeScript: `npm install flamedb-typescript-sdk`
- Kotlin: `implementation("dev.bypixelc:flamedb-kotlin-sdk:<version>")`

## Protocol (tiny example)

```text
AUTH <api_key>
WRITE kills 1 lb="pixel" tag="region:eu"
LEADERBOARD kills LIMIT 10
```

### Multiple leaderboards for one metric

Add one `lb.<board>="<entity>"` argument per named leaderboard. A single
`WRITE` stores one event and increments each specified leaderboard by its value.
The existing `lb=` argument remains the default leaderboard.

```text
WRITE deaths 1 lb="player-uuid" lb.reason="fall" player="player-uuid" reason="fall"
WRITE deaths 1 lb="other-uuid" lb.reason="fall" player="other-uuid" reason="fall"
LEADERBOARD deaths LIMIT 10
LEADERBOARD deaths BOARD reason LIMIT 10
```

The first leaderboard ranks players; the second ranks reasons (`fall` has score
2). `GROUP_LEADERBOARD deaths BOARD reason ...` also reads the named board.
`SET deaths 5 lb.reason="fall"` sets a named score, and
`DELETE deaths lb.reason="fall"` removes that score. Board names are distinct
from event tags, so include `reason="fall"` if you also need event filtering or
windowed leaderboards. Named boards are all-time aggregates; time ranges use
`LEADERBOARD deaths FROM ... TO ... ENTITY reason` over event tags.

### Time-window leaderboard performance

```text
LEADERBOARD craftattack:clans:joins FROM now-7d ENTITY clan
```

Time-window leaderboards scan primary events only in the requested interval
(`FROM` inclusive, `TO` exclusive). They do not walk the entire historical tag
index or perform a separate primary lookup for every event. Binary event values
are aggregated without building per-event tag maps; small pages use a bounded
candidate heap. Scores are exact, including negative values and deterministic
entity-ID ordering for ties. No result cache or time rounding is used, and this
optimization works with existing data without an additional index migration.
Legacy JSON event values remain supported, with their normal decoding cost.

Query time still scales with events **inside** the selected window, and aggregation
memory scales with distinct entities in that window. `LIMIT` reduces sorting and
response memory; it cannot skip events needed to calculate the scores. Named
`GROUP_LEADERBOARD` members retain selective index scans with a consistent snapshot.

Cardinality bookkeeping allocates maps on demand, reuses small flush buffers and
releases large buffers after bursts. Its cache has an estimated 16 MiB key/entry
budget plus the existing 250,000-entry cap; idle entries expire and map capacity
shrinks. This is not a whole-process RAM limit: Pebble's block cache (256 MiB),
memtables (64 MiB each, write-stop threshold 4), active queries and replication
also consume memory.

Run the reproducible comparisons against the previous query algorithm with:

```bash
go test ./internal/storage -run '^$' -bench 'Benchmark(WindowedLeaderboard|CardinalityChurn)' -benchmem -benchtime=3x -count=3
go test ./internal/storage -run 'Test(CardinalityMemory|WindowedAllocations|WindowedSteadyStateMemory)' -count=1 -v
```

The query benchmark uses 200,000 events, 1,000 clans and both a seven-day window
and full-history scan. Measurements exclude fixture ingestion. Memory checks
measure allocations and live Go heap across repeated queries, not process RSS.
See the [measured results and limitations](PERFORMANCE_2026-10-11.md).

### Namespaced identifiers

Metric names and tag keys accept colons, including trailing colons such as `smp:`.
The server and all SDKs preserve the name exactly; `smp`, `smp:`, and `smp:kills`
are distinct metrics. For example:

```text
WRITE smp: 1 player:id="player-1"
GET smp: WHERE player:id="player-1"
WRITE smp:kills 1 lb="player-1"
LEADERBOARD smp:kills
```

Existing names without colons retain their storage format and need no migration.
Namespaced identifiers require the updated server and SDKs on all participating nodes.

## Contact
If you find bugs/exploits that have to be disclosed privately, please write me an email: contact@bypixel.dev or open a ticket on my Discord server: https://discord.gg/yVp7Qvhj9k
