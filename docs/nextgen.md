# Running the shared-storage engine

The legacy TCP auth handshake, newline framing, query parser and SDK methods are retained. Set `storage.engine` to `postgres` or `timescaledb` to use PostgreSQL as the authoritative store and Redis as a disposable exact leaderboard projection. Omitting the engine keeps the legacy Pebble deployment. Existing Pebble directories do not automatically migrate; use the import command described below before switching application traffic.

Each successful mutation commits PostgreSQL events, tags and absolute scores together before its response. Score changes advance the metric's Redis projection generation in that same transaction; pure event writes/deletes leave that generation unchanged and avoid Redis work. Historical and window queries always read PostgreSQL. The application requires `fsync=on` at startup and sets `synchronous_commit=on` inside every ordinary mutation transaction. Redis failure or erased projection keys cannot lose committed scores. Ranking reads detect stale generations and rebuild from PostgreSQL, with exact PostgreSQL fallback during Redis outages. A successful response promises the configured PostgreSQL durability contract, not a second copy on another API machine.

`SET` changes an absolute all-time score and creates no event. Deleting raw events leaves all-time scores unchanged; deleting `lb="entity"` removes the score. Time ranges use `[FROM,TO)` and same metric/timestamp writes replace the stored event, while each acknowledged leaderboard increment still applies. Windowed leaderboards aggregate tagged event values; use `ENTITY <tag-key>`. Native batches preserve partial success and original item error indices. Legacy mutations carry no application idempotency key: never retry an ambiguous timed-out `WRITE`, `SET`, `DELETE` or batch automatically.

Automatic timestamps use a durable per-metric PostgreSQL allocator under the same transaction lock. It survives restarts and coordinates independent API clocks, including identical wall times. Explicit timestamps retain overwrite semantics and do not move the automatic clock into the future. The allocator avoids existing imported timestamps and explicit positions in its current batch.

Raw-only COPY batches with explicit timestamps use shared per-metric locks, so independent batches and ranking snapshots can overlap. Score changes, automatic timestamp allocation, deletes and rebuilds retain exclusive locks. Event, historical-tag and metadata insert order prevents cyclic row-lock waits among overlapping raw batches. Each pooled connection reuses its COPY staging relations, with rows cleared on commit, avoiding repeated catalog create/drop work; authoritative events and scores still commit synchronously together.

Large multi-metric score batches can execute independent metric groups concurrently, with at most eight workers and never more than the configured connection pool. Input order and accepted event/score changes remain atomic within each metric group. `WRITE_BATCH` promises partial-success reporting, not a cross-metric transaction. Sparse groups retain one COPY transaction because measured transaction setup outweighed their concurrency benefit. All database work finishes before cache attempts share one bounded post-commit budget; an unavailable Redis cannot occupy workers required by later metric groups.

## Single API and local development

The Compose file has persistent PostgreSQL and Redis volumes, healthchecks and image digests matching the tested local databases. Its fixed database credentials and `development-change-me` API key are development examples. Set a secret API key and replace database credentials/connection URLs before exposing it beyond localhost.

```powershell
$env:FLAMEDB_API_KEY = 'choose-a-private-key'
docker compose -f compose.nextgen.yml up -d --build
Invoke-WebRequest http://127.0.0.1:9090/readyz
```

For a native Go API process with container databases only:

```powershell
docker compose -f compose.nextgen.yml up -d postgres redis
go build -o .compat/flamedb-nextgen.exe ./cmd/flamedb
.compat/flamedb-nextgen.exe deploy/nextgen-local.yml
```

`deploy/nextgen.yml` uses decoded environment variables for the API key, PostgreSQL DSN and Redis address/password. TLS uses `server.tls_cert` and `server.tls_key`; use TLS PostgreSQL and Redis connections in remote deployments. Configuration limits bound connections, inflight commands, request/response bytes, batch entries and historical scan size. A read exceeding `storage.max_query_events` returns an explicit error. Tune database connection pools across all API nodes to fit PostgreSQL `max_connections`.

## Multiple APIs and failover

Schema version 2 preserves long protocol identifiers, including metric/board/entity names and tag keys/values. Complete text remains stored; ordinary identities use PostgreSQL unique indexes, while larger identities use canonical internal row IDs under the metric transaction lock. Non-unique digest indexes narrow candidate lookups and always recheck complete strings. Digest collisions cannot merge identities. Row IDs are internal and do not change event timestamps, ordering or wire responses.

For an existing version 1 PostgreSQL prototype, stop its API writers before upgrading. The atomic migration adds internal row IDs and replaces indexes without changing events, absolute scores or historical cardinalities. Large databases can require substantial maintenance time; run `flamedb deploy/nextgen.yml --rebuild` before starting the new APIs. Its schema initialization and rebuild each receive a bounded 30-minute maintenance deadline; ordinary request deadlines remain unchanged. A failed migration rolls back and reports its original error after releasing the transaction. Keep a database backup for deployment rollback planning.

Full rebuild processes metadata in pages of 256 and closes each page before acquiring per-metric transactions. It is independent of public response limits and supports a one-connection PostgreSQL pool.

```powershell
docker compose -f compose.nextgen.yml --profile multi up -d --build
go run ./cmd/loadtest --addrs 127.0.0.1:7777,127.0.0.1:7778,127.0.0.1:7779 --mode mixed --workers 32 --duration 15s
```

The three API processes share the same PostgreSQL database and Redis projection. Score mutations, automatic timestamp allocation and large-identity writes serialize per metric in PostgreSQL, so concurrent increments from separate APIs do not lose updates. Ordinary explicit-timestamp raw batches can overlap under shared metric locks. API processes keep no authoritative data; replace a killed API and reconnect clients to another address. Put them behind a TCP load balancer with readiness checks. A lost connection can represent a committed mutation whose response was lost; clients must reconcile that ambiguity rather than replay the legacy mutation.

A warm all-time ranking reads the durable PostgreSQL generation once and requires the atomic Redis script to return that exact generation, epoch and complete board cardinality. It needs no read transaction/metric lock: an unchanged generation is a coherent snapshot at the SQL read; an advanced generation is rejected. A read starting after a write ACK observes its durable generation. Rebuilds and SQL fallbacks keep their metric locks because those paths perform multiple statements. Real concurrency tests pause an uncommitted writer, check the preceding snapshot, then commit and require the new score; they also reject Redis that advances after the first version read.

For clients that consume `CLUSTER_TOPOLOGY`, retain `server.advertise_addr` as that API's externally reachable `host:port`, especially with wildcard listeners or published container ports. Shared APIs honor this existing configuration field; it does not configure database replication.

Compose supplies one PostgreSQL server and one Redis server. For database HA, point the DSN at an independently operated PostgreSQL failover endpoint with synchronous standbys, fencing, backups and tested promotion. A multi-host pgx DSN with `target_session_attrs=read-write` also discovers the writable primary on reconnection. Asynchronous standby promotion can lose recent commits; API node count and legacy `replication_factor` cannot repair that database guarantee.

`storage.postgres.quorum_replication_factor: 0` (default) explicitly rejects `QUORUM` before mutation. A value of 1 permits the local durable contract. For RF > 1, configure PostgreSQL `synchronous_standby_names = 'ANY n (...)'`, with `n >= floor(RF/2)`; the engine verifies the live setting and at least `n` streaming quorum standbys, then commits that request with `synchronous_commit=remote_apply`. The configured factor cannot weaken an existing legacy cluster majority. The API database role needs visibility into `pg_stat_replication.state/sync_state`, for example `GRANT pg_read_all_stats TO <role>`; masked state rejects safely. If topology changes during commit, its outcome may become unknown and clients must reconcile rather than retry. These settings follow PostgreSQL's [synchronous replication contract](https://www.postgresql.org/docs/17/runtime-config-replication.html). Increasing API replicas alone does not enable quorum.

Redis standalone and Sentinel connection configurations are supported; `deploy/redis-sentinel.example.yml` shows an existing Sentinel deployment. Redis cluster mode accepts multiple addresses; projection keys for one metric share a hash slot. Recover Redis by restarting its service, then issue normal leaderboard/rank reads to rebuild demanded projections. Run `flamedb deploy/nextgen.yml --rebuild` to rebuild every metric under per-metric transaction locks and exit. Redis persistence improves warm restart time, but PostgreSQL remains the recovery source. Do not run `FLUSHALL` against a shared Redis installation.

Normal cache attempts receive at most 250ms and one quarter of the remaining request deadline; PostgreSQL keeps the parent context. A stalled Redis handshake, rebuild pipeline or cache read therefore leaves time for authoritative SQL. Post-commit projection updates have the same bounded budget, shared across a native batch's metrics. An explicit `--rebuild` uses its full caller deadline so large projections can finish.

Run `pwsh -NoProfile -File internal/ha/check.ps1` for isolated, labelled Docker fixtures. It tests a real physical PostgreSQL standby, remote-applied ACK, abrupt primary fencing, standby promotion, reconnection and rejection when a required standby is absent. It runs every mandatory analytics test against a six-node Redis Cluster and Sentinel, then abruptly kills the actual cache master to verify SQL fallback, live client rediscovery and rebuild. It creates a fresh network/replica volume per run and removes only its own labelled resources. Logs remain in `.compat/ha-<run-id>`.

This Docker Desktop VM exhibited repeated wall-clock jumps; default Sentinel entered TILT continuously and correctly withheld promotion while SQL reads/writes stayed valid. Redis documents this [clock safety behavior](https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/). Local client-recovery checks use the isolated fixture option `-SentinelTiltPeriodMilliseconds 1000` and record it in `fixture.json`; normal deployment examples and the script default retain 30,000ms. This verifies engine reconnection after real Sentinel promotion, without establishing default-TILT timing under a stable production clock.

## Import a stopped legacy database

Drain the legacy replication outbox, stop legacy writers and keep a backup of the source directory. Import into an empty destination; the importer reads the Pebble source without changing it, imports events, historical cardinalities and absolute all-time scores in one transaction, and rejects a nonempty destination or pending replication. It does not derive absolute scores from event totals.

```powershell
$env:FLAMEDB_POSTGRES_DSN='postgres://flamedb:flamedb@127.0.0.1:55432/empty_destination?sslmode=disable'
go run ./cmd/import-postgres --source ./data-node1 --timescale --timeout 1h
```

Verify representative historical/tag/leaderboard queries and rebuild Redis before switching application traffic. A lost importer commit response is ambiguous: inspect the destination before rerunning. Do not resume writes on the old database after the cutover; keep the source backup for rollback planning.

Readiness is available at `/ready` and `/readyz`; liveness at `/health` and `/healthz`; bounded server counters at `/metrics` on `server.metrics_address`. A Redis outage allows PostgreSQL fallback. PostgreSQL unavailability makes readiness fail and mutations return errors. Shutdown stops admission, waits for inflight work up to the configured deadline and closes connections.

## Reproducible measurements

```powershell
go build -o .compat/loadtest.exe ./cmd/loadtest
./deploy/benchmark.ps1 -Addresses '127.0.0.1:7777' -Workers 32 -Duration 15s
./deploy/benchmark.ps1 -Addresses '127.0.0.1:7777,127.0.0.1:7778,127.0.0.1:7779' -OutputDirectory '.compat/benchmarks-multi'
```

The script measures single writes, pipelined writes, native batches with and without leaderboard updates, mixed traffic, top/rank/window reads, SUM/AVG/COUNT, history and tag filtering. Read-only workloads seed explicit fixtures before timing and compare every response with the stable fixture result. Ingestion assigns unique nanosecond timestamps and performs independent per-metric count/sum readback after draining. Every run gets an isolated metric prefix. JSON records submitted events, confirmed events, confirmed requests, ambiguous events, errors and p50/p95/p99 request latency. Latencies are upper bounds from a bounded logarithmic histogram; pipelined latency includes queueing until that response is received. Events/sec and requests/sec differ for native batches.

`--durability-contract postgres-sync` labels confirmations under an explicitly synchronous database contract; the default leaves durable-by-contract count zero. Readback verifies observed data, not crash survival. Restart/outage integration tests provide separate durability evidence. The resource script captures Docker CPU/RAM, local process CPU/RAM when `-ApiProcessIds` is supplied, PostgreSQL session waits/database counters and HTTP server inflight/admission/database-pool counters alongside each workload. Pass all three API container names and metrics URLs when measuring multiple APIs. Resource samples include fixture setup and response draining. Metadata records source hashes, loadtest binary hash and running image identities. Queues not exported by the backend are not inferred. Compare measurements only with their hardware, pool limits, fixture size, batch size and operation mix. The requested 200k requests/sec cluster target must be evaluated as requests/sec; batch events/sec is a separate measurement.

For a dedicated profiling run, configure `performance.profile_address: 127.0.0.1:6060` on a native API process and run `./deploy/profile.ps1`. It captures CPU, live heap and cumulative allocated bytes, plus an independently checked write workload. Start a fresh server process for each allocation comparison because the heap allocation sample accumulates from process startup. The profiler is disabled by default. Keep this endpoint local; inspect with `go tool pprof -top <profile-file>`.

`deploy/soak.ps1` runs five minutes of three-API mixed ingestion with a Redis stop/restart, independently verifies every event count, sum and entity score, drains the APIs, abruptly kills the owned PostgreSQL primary and verifies saved expectations after recovery without replay. It then checks two minutes of exact rank reads against immutable fixtures. The script refuses unrelated/replaced Compose containers, records exact image/source identities and captures resource/pool/error counters. Its defaults address the isolated development ports 27777–27779 and metrics 29090–29092. Use it only on the explicitly named development deployment; PostgreSQL is intentionally killed after draining. `--verification-file` and `--verify-file` on the loadtest retain and check expectations for separate recovery/migration verification.

`deploy/benchmark-linux.ps1` runs a Linux load generator inside the local Compose network. It measures rank, top, mixed traffic, raw batch sizes 100/1000/10000 and board batch size 1000, with independent count/sum/all-entity-score checks. Build the client with `GOOS=linux CGO_ENABLED=0 go build -o .compat/loadtest-linux ./cmd/loadtest` in a shell that supports that environment syntax; PowerShell users set and restore those two environment variables separately. The client shares the VM's CPU/memory with databases and APIs. Neither this sweep nor the shorter failure qualification establishes production capacity or long-term reliability.

Release checks run semantic/concurrency tests with `-race`, then measure the unchanged legacy allocation threshold in a production build without race instrumentation, which adds its own allocations. Full release diagnostics remain in `.compat/release-current.jsonl`. Real database cases use independent temporary SQL schemas, so accumulated fixtures cannot alter METRICS or rebuild observations.

### Measured development baseline, 13 October 2026

The rebuilt Linux Compose image was exercised through TCP with one addressed API, then three addressed APIs. Both runs used 32 concurrent clients, 16 metrics, 10,000 configured entities per metric, a 15-second submission period, pipeline 16 for pipelined writes, batch 100 for native batches, and batch 10/pipeline 4 for mixed traffic. Read workloads seeded 1,000 events/entities per metric before timing; history returned at most 100 events and top/window returned ten entries. All-time write boards used entity tags. Each batch rotated across the 16 metrics, creating deliberate contention on their transaction locks. The database accumulated fixtures between runs; this is a development baseline, not a controlled production capacity result.

The Windows client host exposed eight logical processors. Docker Desktop 29.7.2's Linux VM reported eight CPUs and 16,680,857,600 bytes RAM. PostgreSQL had `fsync=on`, `synchronous_commit=on`, `max_connections=100`; each API pool had a limit of 32 connections. TimescaleDB/PostgreSQL 17 and Redis 8 used the digests in Compose. API image identity was `sha256:0c09ccf390ea2f0294a1fc543083eebdec43414cf1e55c7f43018d487eebfa7d`, built with Go 1.27.1 and the previous compiler-image digest `8a5910f31396cd4d89662f56c68b3ae31d374308270a1c3bd96672ee5ed43414`. The current successor uses [stable Go 1.27.2](https://go.dev/dl/) with its newly pinned Docker digest; those earlier measurements also predate the raw-only projection optimization. Source hashes, binary hashes, raw workload reports and resource samples are preserved locally in `.compat/benchmarks-single-final` and `.compat/benchmarks-multi-final`. The earlier two-second smoke runs are separate artifacts and are excluded here.

Rates below are rounded confirmed **events / requests per second**. Read-only rows have zero events and count successful reads as requests. Latency columns contain p50/p99 request milliseconds; a batch is one request and pipeline latency includes its queue.

| Workload | One API events/requests per second | One API p50/p99 ms | Three APIs events/requests per second | Three APIs p50/p99 ms |
|---|---:|---:|---:|---:|
| Single write with board | 536 / 536 | 51.09 / 161.02 | 1149 / 1149 | 22.67 / 99.98 |
| Pipeline write with board | 622 / 622 | 440.87 / 951.19 | 834 / 834 | 322.04 / 844.36 |
| Batch write with board | 1394 / 14 | 2097.15 / 3063.77 | 1739 / 17 | 1744.49 / 3234.25 |
| Batch plain write | 1963 / 20 | 1634.73 / 1802.10 | 3291 / 33 | 920.78 / 1725.70 |
| Mixed | 261 / 33 | 2030.11 / 5680.16 | 425 / 53 | 1359.83 / 3763.78 |
| Top ten | 0 / 1204 | 19.70 / 101.07 | 0 / 2120 | 13.19 / 46.85 |
| Exact rank | 0 / 1316 | 19.48 / 84.07 | 0 / 2931 | 10.17 / 26.10 |
| Window top ten | 0 / 439 | 65.54 / 232.70 | 0 / 842 | 34.59 / 97.84 |
| SUM | 0 / 1615 | 14.86 / 81.39 | 0 / 4111 | 6.96 / 21.25 |
| AVG | 0 / 1433 | 17.87 / 82.27 | 0 / 2995 | 9.23 / 35.35 |
| COUNT | 0 / 1614 | 15.52 / 66.97 | 0 / 3498 | 8.02 / 28.16 |
| History | 0 / 193 | 121.50 / 710.02 | 0 / 963 | 29.72 / 99.98 |
| Tag filtering | 0 / 1920 | 12.10 / 79.64 | 0 / 3643 | 7.51 / 29.09 |

All 26 cases passed, with zero reported errors, zero ambiguous events and exact independent count/sum readback. The one-API matrix confirmed 79,928 measured events and 146,360 reads; the three-API matrix confirmed 117,655 events and 317,038 reads. Separate read fixtures account for 128,000 additional readback events per matrix. Durable-by-contract counts equal confirmed measured writes because synchronous PostgreSQL commits were configured; these counts are not a crash-survival measurement. The requested **200,000 requests/sec target is not demonstrated**: the fastest measured row was 4,111 SUM requests/sec. No linear scaling extrapolation is justified by these runs.

Resource samples exposed up to 31 PostgreSQL lock waiters out of 32 inflight API requests during contended writes. A board-batch sample showed PostgreSQL at 92% of one CPU versus API 16% and Redis 6%. Across the full matrices, including fixtures and drain, sampled PostgreSQL CPU peaked at 632.8% for one addressed API and 855.71% for three; these are Docker's aggregate CPU percentages, not percentages of the entire VM. More API processes improved some reads while all-metric batch writes still shared the same PostgreSQL locks.

Fresh native Windows API profiling runs used 32 clients, 64 metrics, batch 100, 15 seconds submission and a ten-second CPU capture. Plain and board runs independently read back 42,900 and 37,500 confirmed events respectively with zero errors. CPU samples totaled 2.84 and 2.57 seconds; Redis projection had 61.97% cumulative CPU in the plain run, while Redis command processing had 48.64% in the board run. Windows network/runtime calls dominate the flat samples, so these profiles cannot stand in for a Linux CPU profile. Cumulative allocations were 113.72 MiB plain and 162.21 MiB board: parser allocations were 38.26% cumulative in plain writes, while the board batch copy path was 54.97%. Sampled live heaps were 11,679.83 KiB and 14,712.11 KiB. Profiles and `pprof -top` output are in `.compat/profile-final-plain` and `.compat/profile-final-board`. These results identify transaction-lock contention, Redis projection round trips and parsing/copy allocation as measured tuning candidates; they do not prove that removing any one cost would reach the target.

To reproduce the matrix parameters on the normal Compose ports, use the commands above with `-Workers 32 -Duration 15s -Metrics 16 -FixtureEvents 1000`. For the multi run also supply `-DatabaseContainers @('flamedb-nextgen-postgres-1','flamedb-nextgen-redis-1','flamedb-nextgen-api-1','flamedb-nextgen-api-2-1','flamedb-nextgen-api-3-1')` and `-MetricsUrls @('http://127.0.0.1:9090/metrics','http://127.0.0.1:9091/metrics','http://127.0.0.1:9092/metrics')`. The recorded runs used isolated database ports 55433/56380, API ports 27777–27779 and metrics ports 29090–29092.

### Measurements after the final COPY changes

The final Go 1.27.2 implementation skips raw-only Redis updates, permits concurrent explicit-timestamp raw batches under shared locks, and reuses per-connection staging tables. Fresh Windows API runs retained 32 workers, 64 metrics, batch 100 and 15-second submissions. Compared with the immediately preceding raw-only optimization run, plain throughput rose from 3,248 to 21,707 confirmed events/s (32.48 to 217.07 requests/s), and board throughput from 1,915 to 2,896 events/s (19.15 to 28.96 requests/s). Final readback independently matched all 326,900 plain and 46,000 board confirmations, with zero errors or ambiguous writes. Plain p50/p99 was 141/295ms; board p50/p99 was 1,037/1,689ms. These are development observations on a growing database, not statistically controlled capacity estimates or attribution to one isolated change.

Final ten-second native CPU captures sampled 7.33s plain and 3.33s board CPU, reflecting substantially more completed plain work. The complete plain profile has no Redis/projection samples. Live heap samples were 12,135KiB plain and 14,642KiB board; cumulative allocations were 754.58MiB and 208.43MiB, rather than live RAM usage. Parsing accounted for 36.72% cumulative plain allocations, and the COPY path for 54.80% board allocations. Profiles, workload/readback reports, source manifests and binary hashes are retained in `.compat/profile-parallel-{plain,board}`; the preceding runs remain in `.compat/profile-optimized-{plain,board}`.

The rebuilt Linux image `sha256:aca20e4eb63a24d3f3df1216a557f0a919cc747d5b530962402605767682dbfc` then ran through all three API addresses, with the previous matrix's 32 workers, 16 metrics, batch 100 and 15-second submissions:

| Final three-API Linux workload | Confirmed events/s | Confirmed requests/s | p50/p99 request ms | Confirmed and independently read back |
| --- | ---: | ---: | ---: | ---: |
| Raw batch | 29,443 | 294.43 | 101/254 | 443,600 |
| Board batch | 5,728 | 57.28 | 530/901 | 90,000 |

Both reports have zero errors, zero ambiguity and no count/sum mismatches; `.compat/benchmarks-parallel-linux` records source hashes, image identity, environment and reports. The earlier full 26-case matrix remains the baseline; these two final Linux cases cover the changed ingestion paths. The 200,000 **requests/s** engineering target remains unproven and was not reached on this host. Event throughput must not be substituted for that target.

### Lossless schema capture before ingestion tuning

The image used for this capture, `sha256:9dca2d82a9fbef93135f6fee1fc83a95450d6cc66a2327d8d0aacd31cacfd496`, includes validated warm ranking snapshots and the lossless large-identifier schema. Warm ranks use one PostgreSQL version SELECT and two Redis operations (epoch GET plus atomic read); cold rebuilds and SQL fallbacks retain transaction locks. Tests reject self-consistent wrong Redis generations, partial board eviction, a cache that advances after the version read, and uncommitted writer data. Inferred multi-seed Redis Cluster clients also use the bounded core routing path.

The populated local database migrated atomically with all 9,608,764 events retained. Independent saved expectations then verified all 1,657,700 earlier mixed-workload events and every entity score. The first attempt hit the ordinary 30-second startup deadline; maintenance now permits 30 minutes for initialization, and the pool-error path releases its transaction before closing, preserving the original error. Populated plain/Timescale migration, idempotence, collision forcing and a 260-metric rebuild above public limit 1 with a one-connection pool are mandatory tests.

A final 300-second three-API mixed run confirmed and read back 1,188,200 events, with zero errors, ambiguity or score mismatches. Redis was stopped during load, then PostgreSQL was abruptly killed after draining and all three APIs restarted; saved count/sum/all-entity expectations passed again. A separate 120-second immutable rank workload verified all 612,600 responses (5,104 requests/s; p50/p99 5.5/18.7ms). Artifacts are `.compat/soak/81c27fcd2558`, with source/image identities, resource counters and recovery expectations. These shorter failure checks do not establish long-term production reliability.

The Linux client then ran inside the same Docker network, sharing the eight-CPU VM with the three APIs, PostgreSQL and Redis: 32 clients, 16 metrics, 1,000 configured entities and 15-second submissions. Every case passed independent event count/sum verification; board/read fixtures also verified every entity score, and immutable reads matched every response.

| Linux-client workload in this capture | Confirmed events/s | Confirmed requests/s | p50/p99 ms | Confirmed events/reads |
| --- | ---: | ---: | ---: | ---: |
| rank | 0 | 8281.52 | 2.9/15.2 | 0/124267 |
| top | 0 | 7926.97 | 3.3/15.2 | 0/118974 |
| plain-100 | 21207 | 212.07 | 136.9/383.0 | 319400/0 |
| plain-1000 | 33879 | 33.88 | 910.9/1582.5 | 521000/0 |
| plain-10000 | 37066 | 3.71 | 8120.4/12388.5 | 670000/0 |
| board-1000 | 5836 | 5.84 | 5439.3/5931.6 | 119000/0 |
| mixed | 4004 | 49.73 | 765.9/1060.0 | 62800/152 |

The earlier schema-1 Linux client sweep is retained in `.compat/benchmarks-linux-warm` (10,305 rank requests/s, 63,408 raw events/s at batch 10,000), and the preceding atomic-read sweep in `.compat/benchmarks-linux-atomic`. The lossless schema adds physical identity/index work and the database grew between runs; these observations are not isolated causal estimates. Do not quote an earlier image's rate as current production capacity. This capture's fastest measured request rate is 8,282 rank requests/s; 37,066 raw events/s at batch 10,000 represents only 3.71 requests/s.

The requested 200,000 requests/s remains unmet. `deploy/check-throughput.cjs` recomputes the mixed workload's confirmed request rate, requires complete readback/recovery and rejects event-rate substitution, invented rates, unconfirmed submissions and unknown writes. Its actual local mixed result (49.37 requests/s during the failure-injected sustained run) intentionally fails the target gate. Default 30-second Sentinel TILT recovery also remains unproven because of this VM's recurring clock jumps; only local Docker was authorized for qualification.

### Ingestion tuning and follow-up measurements

Ordinary identifiers now bypass the PL/pgSQL large-identity trigger through executor-level `WHEN` predicates. Large identifiers retain complete text, canonical row IDs, metric locks and full-equality collision checks. Native batches classify encoded identity sizes and execute only populated event/tag/score branches, avoiding empty scans and repeated JSON tag expansion. Ordinary tag deduplication names its native unique index directly; large tuples use their canonical row ID. Raw-only batches also skip score staging/catalog work. Synchronous commits and the existing metric serialization contract are retained.

`deploy/profile-ingestion.ps1` measures actual schema/Timescale insert plans inside a private UUID schema, rolls back each measured statement and drops that schema. It checks the development Compose owner and addresses the saved container ID. `-LegacyTriggerDispatch` reconstructs the prior five trigger definitions only in that private schema. With 10,000 staged events, five observations after warmup and existing tag/score fixtures, median event/tag/score execution changed from 356.5/92.9/129.7ms to 302.5/62.5/116.5ms. An empty large-tag branch cost another 19.4ms and is skipped for ordinary live batches. These plans exclude commit and network costs; artifacts are `.compat/profile-ingestion-existing-baseline` and `.compat/profile-ingestion-existing-final`. No profiling schema or public profiling metric remained after cleanup.

The subsequent Linux-client comparison used the same eight-CPU Docker VM, three APIs, 32 workers, 16 metrics, 1,000 entities and 15-second submissions. The before runtime image is recorded as `sha256:9dca2d82a9fbef93135f6fee1fc83a95450d6cc66a2327d8d0aacd31cacfd496`; the after image as `sha256:b996c31851d2d2817299c546a5eeec4f5998a6d9d86e1cebfd086c6055e4832f`. The before run's working-tree hashes already contain the edits; they do not identify its older compiled server. Use the recorded runtime image identities when interpreting the comparison.

| Workload | Before confirmed requests/s | After confirmed requests/s | After confirmed events/s |
| --- | ---: | ---: | ---: |
| rank | 10891.41 | 9842.18 | 0 |
| top | 8444.78 | 9083.09 | 0 |
| plain-100 | 238.41 | 323.47 | 32347 |
| plain-1000 | 40.74 | 49.22 | 49219 |
| plain-10000 | 3.81 | 3.83 | 38292 |
| board-1000 | 6.75 | 7.96 | 7957 |
| mixed | 55.58 | 68.11 | 5474 |

All fourteen before/after cases had zero errors and ambiguous writes and passed independent event count/sum verification; board/read fixtures checked all entity scores and immutable reads. Artifacts are `.compat/benchmarks-linux-ingestion-before` and `.compat/benchmarks-linux-ingestion`. Mixed request throughput increased 22.5% in this pair; ordinary batches of 100 and 1,000 increased 35.7% and 20.8%. The 10,000-item case barely changed, and ranking rates fluctuated despite unchanged read code. These short sequential observations are not an isolated statistical estimate or production capacity qualification. The 200,000 confirmed requests/s target remains unmet.

The ingestion capture's backend and release checks required `TestPostgresIdentityIndexBoundary`, including both sides of the 2,000-byte index boundary, mixed short/large identities and strings that cross the boundary only after lossless encoding. Its backend qualification required 21 observed, unskipped tests.

The ingestion build's 300-second failure-injected run confirmed 1,486,900 events (61.67 requests/s, 4,935.58 events/s). Saved expectations verified every event count/sum and entity score again after the PostgreSQL restart. Artifacts are `.compat/soak/02e72019f368`; the following immutable rank phase and all runnable ingestion gates also passed. Resource observations showed roughly 30 of 32 writers waiting for metric locks while PostgreSQL used about one CPU core, identifying the next concurrency bottleneck. The default Sentinel fixture still reported TILT with repeated clock jumps; `.compat/ingestion-sentinel-current.txt` records that observation.

### Independent metric batching: preceding qualification

The preceding runtime image is `sha256:b00c30debd4c50d20250f22f74a2874636028a6cc978815b19425848484e9bd9`. Multi-metric score chunks with at least 32 items per metric on average use bounded independent metric transactions. An unconditional fan-out experiment improved large board batches but regressed small mixed traffic, so sparse groups retain the existing COPY path. Cache publications are collected only after successful commits and attempted after all database jobs finish; one shared cache budget prevents stalled Redis from delaying successive worker waves. Per-metric locks, sequential float arithmetic, original error indices and accepted event/score atomicity are retained. No cross-metric atomicity is promised.

`TestPostgresIndependentMetricBatches` requires an unblocked metric's committed score to become visible while another metric remains externally locked, without prematurely completing the batch. Its negative control substitutes the original transaction path and must fail that same observation with all 128 writes otherwise accepted. Further cases check interleaved overflow rejection indices, a one-connection pool and cancellation without score changes. The stalled-cache test now uses 1,024 writes across 32 metrics, covering multiple worker waves while retaining its original 600ms request deadline and 500ms response bound. That qualification required 22 unskipped backend tests; frozen/current SDK and TCP checks, whole-project race detection, the unchanged production allocation limit and vet all passed.

That Linux-client sweep used three APIs, 32 workers, 16 metrics, 1,000 configured entities and 15-second submissions. All seven cases had zero errors and ambiguous writes and passed independent count/sum verification; board/read fixtures verified every entity score and every immutable response. The immediately preceding ingestion image is retained locally as `flamedb-nextgen-api:before-metric-batching`.

| Preceding workload | Confirmed requests/s | Confirmed events/s |
| --- | ---: | ---: |
| rank | 9199.51 | 0 |
| top | 7785.65 | 0 |
| plain-100 | 332.70 | 33270 |
| plain-1000 | 47.59 | 47593 |
| plain-10000 | 5.30 | 53035 |
| board-1000 | 14.77 | 14772 |
| mixed | 62.12 | 5003 |

Artifacts are `.compat/benchmarks-linux-metric-batching`. Compared with the ingestion image's immediately preceding 7.96 requests/s, the board-1000 observation increased about 86%; versus the first comparison's 6.75 requests/s it increased about 119%. Mixed traffic measured 62.12 versus the ingestion image's 68.11 requests/s, while reads also varied despite unchanged read code. These short sequential measurements on a growing database justify the large-batch optimization, but are not an isolated capacity estimate or a claim that every workload improved. The discarded unconditional experiment is retained in `.compat/benchmarks-linux-metric-batching-unconditional` for the sparse-group tradeoff.

The final 300-second failure-injected three-API run confirmed and independently read back 1,511,900 events (62.86 requests/s, 5,030.36 events/s), with zero errors, ambiguous writes or entity-score mismatches. After draining, killing the owned PostgreSQL primary and restarting it with all APIs, saved count/sum/all-entity expectations matched again without replay. The following 120-second immutable rank phase verified 619,082 responses (5,158.78 requests/s). Full source/image identities, resource counters, faults and recovery expectations are in `.compat/soak/46e16610814f`. All five metric-batching acceptance gates passed, with none abandoned.

The 200,000 confirmed requests/s target remains unmet: that build sustained 62.86 mixed requests/s, and the negative-control-qualified target checker rejects it. Default Sentinel recovery also remains unqualified; the existing local fixture still reports TILT during VM clock jumps (`.compat/metric-batching-sentinel-current.txt`). Additional overall delivery gaps are the supplied PostgreSQL/Redis HA deployment with automatic promotion and fencing, million-entity ranking capacity tests, and long-term qualification; the current Compose stack is a single PostgreSQL and Redis with three API nodes.

### Small-request coalescing qualification

The preceding coalescing runtime `sha256:bd1e577eeb59f0e20437a8cb0a61bef022242a8b52c7e60a49ce0cff73a0bd81` groups concurrent ordinary single writes and score batches smaller than 256 items. A queue of 32 requests, one carry request, one collecting wave and at most two active waves bounds admission and processing. Waves contain at most `performance.batch_size` items and use a one-millisecond collection timer before waiting for a worker. Tiny waves retain one COPY; sparse multi-metric score waves use four deterministic transaction stripes, while dense groups retain independent metric transactions. Each metric preserves its input order and atomic event/score changes; cross-metric batches retain partial success. SET, DELETE, replica and QUORUM mutations keep their separate paths.

Queued cancellations are removed before dispatch. Every dispatched caller waits for the final outcome; one explicit cancellation does not interrupt live companions, and cancellation of all participants interrupts SQL immediately. A shared wave is bounded by its earliest caller deadline. Shutdown cancels the dispatcher, drains queued outcomes and joins active workers before closing Redis/PostgreSQL. Prometheus exposes `flamedb_ingest_queue_requests`, `flamedb_ingest_active_waves` and `flamedb_ingest_coalesced_requests_total` without metric-name labels.

All 23 mandatory backend observations, frozen/current SDK and TCP comparisons, whole-project race detection, allocation limits and vet passed for this build. The Linux mixed capture measured 169.31 confirmed commands/s and 13,567 events/s versus the preceding metric-batching capture's 62.12 commands/s and 5,003 events/s. All nine cases used the unchanged Linux client, three APIs, 32 workers, 16 metrics and 1,000 configured entities, with independent count/sum/all-entity verification. Captures and source/runtime identities are in `.compat/benchmarks-linux-coalescing`. Experimental one-wave, two-wave and dense-stripe alternatives are retained separately; a short experiment's higher result is never substituted for the final capture.

The 300-second failure-injected coalescing run sustained 138.98 confirmed commands/s and 11,119 events/s, with zero errors or ambiguous outcomes. Every one of 3,339,400 acknowledged events and every entity score matched after the primary was abruptly killed and restarted, without replay. Its following 120-second immutable rank phase verified 811,173 responses. Artifacts are `.compat/soak/356edb83f8a7`; all five coalescing criteria passed. This image remains available locally as `flamedb-nextgen-api:before-projection`.

### Compact Redis projection

Score projections now transmit one Redis sorted-set/hash key pair per board and use numeric key positions in their item arguments. Metric and board hashes are calculated once per distinct board. The atomic Lua script validates all affected boards before any mutation, applies every exact member/hash change, recounts each board once, and publishes the generation last. Cache keys, metadata and ranking representations remain compatible with preceding API instances during a rolling replacement.

A dedicated bounded publisher overlaps already committed projection changes with remaining SQL jobs. One shared Redis budget starts at the first committed publication; the channel has room for every original metric, so a stalled Redis cannot block database workers. The caller still joins all SQL and publication workers, and PostgreSQL commits remain the acknowledgement prerequisite.

`TestRedisProjectionBoardCommandCost` executes the actual Lua source against Redis with a forwarding command counter, then verifies both direct production-script and actual Go-packed projections across default/named boards and positive/negative ties. For 600 changes on three boards, sizes are read six times instead of the old 1,200, membership hashes three times instead of 600, and the production command declares seven explicit keys instead of 1,201. Negative controls reject repeated validation and the actual old Go packing. The independent-metric test additionally requires a committed warm board to appear in Redis while another SQL metric remains externally locked; the old delayed publisher fails that same observation despite accepting all 128 writes.

Linux tests against the existing local Redis Cluster and Sentinel masters passed the real projection-cost, delayed-version, atomic-ranking and independent-metric cases. These checks do not inject master failover or qualify the VM's default Sentinel timings; artifacts are in `.compat/projection-modes`.

The projection qualification runtime is `sha256:eddab388718d8c94ae599c97a3d1498fc139a7f054d142bbe639bed3f6480b22`. The final nine-case Linux capture used the unchanged client and the same three APIs/32 workers/16 metrics/1,000 configured entities, with zero errors, ambiguous writes or count/sum/score mismatches:

| Projection workload | Confirmed commands/s | Confirmed events/s |
| --- | ---: | ---: |
| rank | 16130.36 | 0 |
| top | 13797.83 | 0 |
| plain-1 | 5083.39 | 5083 |
| board-1 | 1911.06 | 1911 |
| plain-100 | 467.65 | 46765 |
| plain-1000 | 73.59 | 73585 |
| plain-10000 | 7.20 | 71961 |
| board-1000 | 26.43 | 26428 |
| mixed | 222.18 | 17793 |

The mixed capture confirmed 269,400 independently verified events. Its 222.18 commands/s is 31.2% above the immediately preceding coalescing capture's 169.31, and 3.58 times the original metric-batching capture's 62.12. Mixed latency bucket upper bounds changed from approximately 617/901ms p50/p99 to 174/254ms in this short comparison. Pure raw ingestion reached 73,585 events/s; board-1000 reached 26,428 events/s. These are sequential measurements on a growing local database, with varying background I/O and VM load; unchanged read/raw paths also varied. They do not isolate every optimization's causal gain or prove production capacity. The intermediate Lua-only capture (194.56 mixed commands/s) remains in `.compat/benchmarks-linux-projection-before-publisher`.

The projection 300-second failure-injected run sustained 202.34 confirmed commands/s and 16,189 events/s, compared with the original build's 62.86 commands/s: approximately 3.22 times the sustained throughput. Its p50/p99 latency bucket bounds were 181/572ms, including Redis loss/recovery. All 4,859,900 acknowledged events and every entity score matched independently before and again after the PostgreSQL primary was killed and restarted, without replay. Errors, ambiguous outcomes and readback mismatches were zero. The following 120-second immutable rank phase verified 1,013,563 responses (8,445.84 commands/s). Runtime/source hashes, resources, faults, saved expectations and recovery results are in `.compat/soak/9d32edec0af8`; the final Linux matrix is `.compat/benchmarks-linux-projection`.

All 24 mandatory backend observations, frozen/current SDK and TCP comparisons, whole-project race detection, the unchanged production allocation threshold and vet passed. The final compact projection and early cache publication also passed the four real Linux tests against both existing Redis Cluster and Sentinel masters. All six projection optimization criteria passed, in addition to the preceding five coalescing criteria; none were abandoned.

The 200,000 confirmed commands/s target remains far unmet. The supplied stack still has one PostgreSQL primary and one Redis, without a furnished PostgreSQL promotion/fencing manager or Redis HA deployment. Million-entity-per-board performance and long-term capacity qualification remain open, as does normal Sentinel failover under the local VM's unstable clock. No default timing was weakened during these optimization checks.

### Automatic ingestion on the local i3

The 2026-10-14 optimization targets SDK-default writes without explicit `ts`. The measured host is an Intel Core i3-12100F with four physical/eight logical cores and 32 GB host RAM; Docker exposes eight CPUs and about 15.54 GiB. PostgreSQL, Redis, three APIs and the Linux load generator share that VM. No Ryzen 9 9900X production capacity has been measured.

Automatic allocation now groups ordinary/large identity classes in three transport phases with at most two SQL statements per phase, instead of three statements per metric. Bounded lower/upper timestamp windows prune distant Timescale chunks. The occupied-position page budget is at most max(1024, 32 times the remaining metric count); fully occupied pages advance and are fetched again. All automatic positions are checked under exclusive metric locks, and clock updates remain in the same durable transaction as events and scores. Explicit timestamps retain overwrite semantics and do not move the clock.

Sorted metric locks, existing versions, scores and clock reads share one READ COMMITTED statement batch. Multiple metric locks use one statement over typed name/mode arrays; ordinality preserves Go's raw-name order, including byte strings whose encoding changes text order. The lock calls are output expressions evaluated after sorting by the independent ordinal, as documented by [PostgreSQL SELECT](https://www.postgresql.org/docs/17/sql-select.html#SQL-SELECT-LIST). A real blocked-transaction test checks that no later metric locks precede the first blocked raw name, inspects shared/exclusive modes, and observes the preceding commit's score, version and clock. Sorting by encoded text fails this test; that negative control is retained in `.compat/timestamp-lock-order-negative.txt`. Subsequent read statements execute after all locks are granted and take fresh snapshots. Temporary staging preparation for explicit or aliased batches happens before acquiring metric locks. Validations still run in original per-metric input order, with rejected multi-board increments leaving no event or companion score.

Automatic raw events and tiny score waves use direct durable COPY. Larger automatic score jobs use typed event/JSONB/tag/absolute-score arrays in one prepared SQL write batch. Deduplication is limited to tag identities and final absolute score rows; every accepted automatic raw event is stored. Explicit timestamps and reused Query pointers retain staging and last-write-wins behavior. Larger automatic raw batches and automatic score batches below 64 events per metric use four bounded metric transaction stripes. This avoids both one transaction locking all metrics and a jump to many small transactions as coalesced requests grow. The 32-events-per-metric boundary with more than eight metrics uses 16 stripes, preserving multiple SQL worker waves and the original shared cache budget; its unchanged race deadline test passes. Larger dense score groups retain independent metric jobs. SQL workers remain bounded by eight and the connection pool.

Redis publication observes its epoch once per request's existing shared cache budget and pipelines at most 32 ready metric projections. Each unchanged atomic Lua script verifies epoch, previous generation and board completeness before modifying data. Only NOSCRIPT responses retry with source; absolute score changes and generation checks also make transport replays harmless. Publication starts after durable commits, overlaps remaining SQL jobs and cannot block their workers. Tests exercise warm pipelines, cold scripts, delayed older generations and cached epochs crossing replacement metadata.

The Compose PostgreSQL defaults now use `max_wal_size=4GB`, `checkpoint_timeout=2min` and `wal_compression=lz4`, with environment overrides for each. `fsync`, `full_page_writes` and synchronous commits stay enabled. The earlier 1 GB WAL budget repeatedly forced checkpoints. An 8 GB/15-minute experiment produced higher short throughput, but failed the existing 120-second readiness bound after a crash: PostgreSQL became ready about 284 seconds after startup. Delayed verification still recovered all 3,259,700 acknowledged events and all entity scores (`.compat/soak-timestamps/a8e55247503b/recovery-delayed.json`). That experiment is not the selected runtime configuration. Larger WAL/checkpoint budgets can increase crash-recovery time and require more disk space; max_wal_size is a soft limit. See the [PostgreSQL 17 WAL documentation](https://www.postgresql.org/docs/17/runtime-config-wal.html). Redis persistence settings were retained. The local API pool is tuned separately from the unmeasured production host.

The selected runtime is `sha256:5d6bca7f2e3ea77ffdcecdb30bc2e57d030d9306c7d32b7fa93a6bd3f7d79588`. The final 30-second Linux comparison uses the identical client binary, 32 connections, 16 metrics, 1,500 configured entities, two tags and automatic timestamps on three APIs. The initial runtime was the preceding projection image. Every case independently matched event count/sum and every expected entity score, with zero errors or ambiguous writes. A concurrent Java application and OBS were running during the preceding comparison; after the user paused them, the entire matrix was repeated. That preceding capture remains in `.compat/benchmarks-timestamps-with-background-load`, and a host CPU sample is in `.compat/timestamp-host-after-pause.json`. These conditions cannot establish isolated CPU scaling or a guarantee for another machine.

| Automatic workload | Before events/s | Selected events/s | Selected confirmed commands/s |
| --- | ---: | ---: | ---: |
| Single leaderboard write | 529.90 | 2302.88 | 2302.88 |
| Raw batch, 1000 events | 14361.81 | 45873.55 | 45.87 |
| Leaderboard batch, 1000 events | 21004.75 | 31052.17 | 31.05 |
| Mixed, 100-event batches and leaderboard reads | 13119.20 | 24915.84 | 311.29 |

Artifacts are `.compat/benchmarks-timestamps-before` and `.compat/benchmarks-timestamps-after`. The comparison includes code, checkpoint/WAL configuration and the selected eight-connection PostgreSQL pool per API, versus the initial 32 per API. A later unchanged-code control with the 8 GB/15-minute PostgreSQL settings measured 20,073 board events/s and 13,348 mixed events/s; its image identity is recorded in `.compat/benchmarks-timestamps-control`. Its `source` inventory describes the checkout at capture, whereas the original image and initial capture identify the historical code. All captures retain actual runtime identities. These sequential observations on a growing database (about 35 GB before the final run) are not isolated causal estimates. The higher 8 GB/15-minute captures remain in `.compat/benchmarks-timestamps-wal-eight` and `.compat/benchmarks-minecraft-wal-eight`. Discarded direct-COPY/dense-group/timer variants and the 2 GB/2-minute and earlier 4 GB/2-minute regressions were also preserved separately instead of substituting their best number for the final result.

The additional 30-second Linux profile has 250 distinct metrics, 1,500 configured entities and the same 32 connections/two tags/automatic timestamps. Each write updates the default leaderboard. All 375,000 configured metric/entity score expectations were checked, including expected absent entries; this is not 1,500 concurrent client connections or a rate-limited player-burst test.

| 250-metric workload | Confirmed events/s | Confirmed commands/s | p99 latency bucket upper bound |
| --- | ---: | ---: | ---: |
| 250-event write batches | 25256.45 | 101.03 | 460ms |
| 1000-event write batches | 30048.76 | 30.05 | 1451ms |
| Mixed, 100-event batches and leaderboard reads | 17124.74 | 213.91 | 305ms |

Artifacts are `.compat/benchmarks-minecraft`, repeated after the user paused the concurrent Java application and OBS. The earlier capture remains in `.compat/benchmarks-minecraft-with-background-load`. The narrower 16-metric board rate must not substitute for this wider workload's rate. 1,500 players producing 100-250 actual events every 1-5 seconds require 30,000-375,000 events/s. The 250-event batches and mixed traffic remain below the lowest demand; the 1000-event case reaches that floor with essentially no headroom in a short run. Batching reduces TCP command count, not the number of stored events. Neither sustained player-burst capacity nor linear CPU scaling nor Ryzen production readiness follows from these measurements.

The subsequent five-minute Windows-host mixed run confirmed 6,248,700 automatic events at 20,820.53 events/s and 260.25 commands/s, including Redis loss between approximately 32 and 50 seconds. Batch size was 100, with 16 metrics, 1,500 configured entities, 32 connections and pipeline depth one. Request latency bucket bounds were 134/225/333ms p50/p95/p99. All confirmations matched count/sum and every expected entity score before and again after the exact primary was killed with SIGKILL; no mutation was replayed. PostgreSQL reached readiness 24.43 seconds after Docker's recorded start time, and all three APIs passed the unchanged 120-second recovery bound. This is one measured recovery, not a production RTO guarantee. The following 120-second immutable rank phase verified 884,006 responses at 7,366.52 commands/s. Errors and ambiguous outcomes were zero throughout. Artifacts, saved expectations, exact source/config/runtime identities and the database recovery log/timing are in `.compat/soak-timestamps/6c9426cb7c3e`.

All 29 mandatory backend observations passed. The project-wide race run passed 236 test cases, including frozen/current SDK and differential TCP checks; the unchanged production allocation test and vet also passed. Five real Linux projection/ranking tests passed against each existing Redis Cluster and Sentinel master, including the actual bounded pipeline and cold-script path (`.compat/timestamp-projection-modes`). These checks do not inject master failover or qualify default Sentinel failover timing. The seven criteria for this local automatic-ingestion optimization are separate from the original 200,000-commands/s target and the deployment/capacity work still open above.
## Compatibility and fault verification

Use a binary built from frozen reference `a7bb892b881e59d207f79ae7258ceecdd4c63ac7` and the new binary:

```powershell
$env:FLAMEDB_INTEGRATION='1'
$env:FLAMEDB_REFERENCE_BINARY=(Resolve-Path .compat/flamedb-reference.exe).Path
$env:FLAMEDB_NEXTGEN_BINARY=(Resolve-Path .compat/flamedb-nextgen.exe).Path
$env:FLAMEDB_TEST_POSTGRES='postgres://flamedb:flamedb@127.0.0.1:55432/flamedb?sslmode=disable'
$env:FLAMEDB_TEST_REDIS='127.0.0.1:56379'
go test ./internal/compat -count=1
./deploy/verify-compat.ps1
```

The differential TCP corpus uses fixed timestamps and JSON numbers without floating-point rounding, preserving every legacy response field and array order. The user approved one additive compatibility repair: raw events include lowercase `timestamp`, `metric`, `value` and `tags` aliases for frozen TypeScript/Kotlin SDKs that require them, alongside the unchanged uppercase legacy fields. Tests require each alias to equal its corresponding legacy value; an absent legacy tag map remains `Tags: null`, while the approved lowercase alias is `tags: {}` for Kotlin's non-null map. Only those four validated aliases are removed before the exact legacy comparison. Unfiltered `METRICS` is restricted to the test prefix because the authoritative test database also contains other test runs. Explicit integration mode fails when required binaries/services are missing. Ordinary unit runs show the integration prerequisite as a skip. Tests cover separate API processes, concurrent shared scores, acknowledged writes after abrupt API termination, absolute SET, event/score DELETE and projection erase/rebuild.

Run outage tests sequentially against throwaway containers only, while no other database tests or benchmarks run:

```powershell
$env:FLAMEDB_FAILURE_INTEGRATION='1'
$env:FLAMEDB_TEST_POSTGRES_CONTAINER='flamedb-nextgen-postgres-1'
$env:FLAMEDB_TEST_REDIS_CONTAINER='flamedb-nextgen-redis-1'
go test ./internal/compat -run TestDatabaseOutagesRejectAndRecover -count=1
```

This separate opt-in stops and restarts the named containers. It requires a `flamedb-nextgen-` container prefix and checks that Redis outage preserves exact results and PostgreSQL outage rejects mutations before recovery. Keep test databases isolated from valuable application data.

### SDK-owned Minecraft ingestion

EraMC's Moonlight adapter now delegates individual `record()` and `recordThen()` calls to the Kotlin SDK's `FlameDB.buffered(...)` factory. The SDK owns batching, journal, sender and recovery; developers do not implement those components. Existing 128-event/100 ms settings, event payloads, historical reads and confirmation callbacks remain in use. The local journal admission budget is separate from database retention. The resolved SDK and adapter pass local TCP burst, legacy-journal, restart and ambiguous-response checks; these checks do not establish database capacity. See [Kotlin SDK usage](../pkg/kotlin/README.md) and [Minecraft workload inputs and remaining qualification](minecraft-workload.md).

The user's subsequent timeout report concerned the existing Pebble server. Its leaderboard batches performed one synchronous commit per event. That path now performs one durable commit per batch, with exact repeated scores, named boards and receipts preserved. This is a separate legacy-backend fix; the PostgreSQL measurements above remain historical observations of their recorded builds. See [Pebble timeout diagnosis, measured improvement and rollout](pebble-batch-timeouts.md).

The [current direct backend comparison](backend-comparison.md) uses the same server image and Linux client, equal total CPU/memory limits, fresh volumes and two rounds of seven common workloads. It records both confirmed event/request rates and independent player SUM verification. Its fresh-database two-filter results are separate from the older optimization measurements and from the unmeasured mature Minecraft workload.
