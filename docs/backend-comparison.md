# Current Pebble versus Next Gen: local comparison

Measured on 2026-10-14 on the local i3-12100F Docker Desktop environment. Values are the mean of two 20-second submission runs, including response drain. Each run has fresh labeled Linux Docker volumes; both server modes use the same compiled image. This compares the repaired Pebble backend with the current PostgreSQL/TimescaleDB + Redis implementation.

| Workload | Unit | Pebble | Next Gen |
| --- | --- | ---: | ---: |
| Single leaderboard writes, 32 connections | events/s | 4,518.4 | 2,431 |
| 128-event batches, one sender | events/s | 4,923.9 | 5,485.7 |
| 128-event batches, 32 connections | events/s | 4,973.1 | 9,715.9 |
| Player SUM, 20 events/player/metric | queries/s | 23,361.9 | 549 |
| Player SUM, 100 events/player/metric | queries/s | 6,240.5 | 116.3 |
| Top 10 leaderboard, 1500 entities | queries/s | 25,434.2 | 6,412 |
| 80/20 batch writes / player SUM | events/s + reads/s | 29,465.1 events/s + 57.4 reads/s | 8,530.4 events/s + 16.6 reads/s |

A batch is one protocol request containing 128 events. Divide the batch event rate by 128 for write requests/s. Mixed total requests/s equals events/s divided by 128 plus read queries/s. Single writes have one event per request. Only successful server responses count; journal admission is not measured.

## Parameters and correctness

Both backends receive a total budget of 4 vCPUs and 4 GiB. Pebble receives that budget in one process. Next Gen splits it between API (1 vCPU, 1 GiB), PostgreSQL (2.5 vCPUs, 2.75 GiB), and Redis (0.5 vCPU, 0.25 GiB). The separate Linux client has 2 vCPUs and 512 MiB. This allocation affects results and is not a claim that it is optimal for every workload. The Docker VM exposes 8 CPUs and about 15.54 GiB. Existing test services remain running; no production service was replaced.

Write-only runs use 250 metrics and an ID range of 1500 players, starting without score entries. They include creation of new metric/entity scores. Read and mixed runs use five metrics with every one of the 1500 players prefilled. The two immutable SUM histories contain 150000 and 750000 events respectively. Queries use `WHERE player=... AND region="eu"`. Mixed tests start with 150000 events and use a read after every four write batches. All writes update the default leaderboard and retain original raw events/tags. Explicit timestamps and pipeline=1 are identical in both modes; the response timeout remains five seconds.

The dataset uses synthetic player IDs, value=1 and region=eu throughout. The usual EraMC player-only SUM filter is not directly measured by this two-filter scenario. No manual ANALYZE is run after fixture loading; PostgreSQL planner statistics and automatic maintenance on fresh databases can influence these short measurements. Do not substitute the results for a mature five-day production dataset.

All 28 measured runs have zero reported errors, zero ambiguous events and exact independent count/sum/entity-score readback. Total measured confirmed writes: 2,833,570 events; reads: 2,487,858. Player SUM runs additionally check every one of the 7500 metric/player results against input and successful-ACK counters after response drain. Immutable timed responses match independently constructed SUM fingerprints; mixed reads have evolving values and receive full independent verification after drain.

Timed target coverage is stored as `player_query_targets_observed` in every player workload report. Some slower SUM and mixed runs do not cycle through all 7500 targets within 20 seconds; every target still receives independent verification after drain.

## Latency and observed memory

Quantiles are upper bounds from the loader's fixed histogram, not exact percentile samples. Memory is sampled Docker CLI memory usage (Linux cache adjustment), summed across backend components; it includes setup/readback and is not RSS or an end-to-end Minecraft memory budget.

| Workload | Pebble p99 range (ms) | Next Gen p99 range (ms) |
| --- | ---: | ---: |
| Single leaderboard writes, 32 connections | 17.7–18.1 | 25–29.4 |
| 128-event batches, one sender | 48.9–76.3 | 60.8–67.7 |
| 128-event batches, 32 connections | 1,881.9–2,388.2 | 610.1–694.8 |
| Player SUM, 20 events/player/metric | 7.7–9.2 | 102.2–195.7 |
| Player SUM, 100 events/player/metric | 21.2–22.2 | 395.6–395.6 |
| Top 10 leaderboard, 1500 entities | 6.4–10.1 | 52.2–53.3 |
| 80/20 batch writes / player SUM | 237.8–298.5 | 725.6–808.6 |

| Equal immutable dataset | Pebble observed peak range (MiB) | Next Gen observed peak range (MiB) |
| --- | ---: | ---: |
| Player SUM, 20 events/player/metric | 309.4–333.1 | 206.9–219.5 |
| Player SUM, 100 events/player/metric | 721.1–736.6 | 423.5–434.1 |

## Architecture and limits

Pebble keeps raw events, tag indexes and ordered score keys in the Go server's embedded database. Next Gen uses durable PostgreSQL transactions and a rebuildable Redis ranking projection, so separate API instances can share authoritative data. Both retain the old TCP/SDK commands and buffered Kotlin transport. Dedicated RANK/SCORE/AROUND/count commands are implemented by the Next Gen engine; the current legacy executor does not implement those new ranking commands. This comparison measures their common TOP/GET/WRITE surfaces.

Next Gen requires operating PostgreSQL and Redis as well as the API. Its deployment still needs furnished production promotion/fencing and HA qualification; additional API replicas alone are not database HA. The benchmark uses one authoritative copy for both backends. Pebble acknowledgements use synchronous WAL commits; PostgreSQL has fsync, full_page_writes and synchronous_commit enabled. Redis is not the sole durable score source. No expiry, aggregation shortcut or unknown-write replay is used.

Two initial Next Gen test-fixture starts failed before a workload began. A retry diagnosed an API start before PostgreSQL listened on TCP: a Unix-socket `pg_isready` check could pass for the temporary init server. The fixture now checks `pg_isready -h 127.0.0.1` before starting the API. Diagnostic logs and successfully completed earlier samples are retained; resumed samples use the unchanged client/image/workload/resource limits. This startup correction does not change database query settings.

These are short closed-loop server measurements, not a sustained open-loop test at 30000–375000 offered events/s. They exclude SDK disk-journal overhead, Minecraft TPS/MSPT, multiple database replicas, million-entity boards and production Ryzen measurements. The 1500 player IDs are not 1500 Minecraft clients. No universal performance winner or production capacity guarantee follows from this matrix.

Artifacts: `.compat/backend-comparison/environment.json`, `results.json`, per-run output/errors, backend logs and fixture failure diagnostics. Reproduce with `go build` for the Linux `cmd/loadtest` binary and `node deploy/compare-backends.cjs run`; validate with `verify` and `controls`, then regenerate this document with `report`. Comparison volumes/containers are removed only after ownership-label checks; original services stay running.
