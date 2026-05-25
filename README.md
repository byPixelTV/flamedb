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

FlameDB is a distributed Go database built for game backends and analytics-heavy workloads. It focuses on speed, simple ops, and cluster-ready defaults.

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

## Contact
If you find bugs/exploits that have to be disclosed privately, please write me an email: contact@bypixel.dev or open a ticket on my Discord server: https://discord.gg/yVp7Qvhj9k   