# Next Gen optimization evidence

Performance parity with the repaired Pebble backend is **not achieved**. The
current candidate passes correctness checks, but the latest matched historical
player SUM measurement reaches about 47% of Pebble's throughput. The complete
seven-case matrix, both player filter forms, and sustained Minecraft/SDK load
have not been qualified for this candidate. Keep the original results in
[backend-comparison.md](backend-comparison.md) as historical evidence.

## Changes under test

Historical SUM/AVG/COUNT reads use a stored, fixed-size metric/tag candidate index.
They still verify the original metric, complete filter and timestamp range, so a
token collision cannot establish equality. An unbounded range no longer causes a
generic prepared plan to intersect that index with a full timestamp scan.

Concurrent reads share database scans, with at most 32 requests per wave, a
100-microsecond collection window, four workers bounded by the connection pool,
and a 128-request queue. No aggregate results are cached. SQL preserves timestamp
order for floating-point accumulation; overflow/cancellation in a shared query
falls back to independent read-only requests. A JSON numeric tag cannot match a
string filter merely because their candidate tokens are equal.

Explicit SDK-sized writes use prepared array upserts instead of temporary-table
COPY exchanges. Raw keys are deduplicated after validating leaderboard increments
in input order: the last raw payload wins, while every accepted increment remains.
Large batches and large protocol identities retain their general fallback paths.
Commits still require PostgreSQL synchronous durability. Redis is rebuildable.

The candidate index adds stored bytes and another GIN index per event. Existing
SQL history needs a table backfill and index construction under migration locks;
large-history migration cost and write amplification are not qualified by these
small fixtures. Normal startup uses `server.request_timeout`. For an existing
Next Gen database, schedule maintenance and run the existing
`./app <nextgen-config> --rebuild` command, whose startup budget is 30 minutes,
before restarting API services. That command also rebuilds Redis projections.
Neither this command nor these optimizations migrate an existing Pebble volume.

## Latest matched measurement

Measured locally on 2026-10-15, on the i3-12100F with Docker Desktop. Two reversed
backend-order rounds, 20 seconds of submissions plus response drain, fresh Linux
named volumes, the same compiled image in both modes, 32 TCP connections and
pipeline=1. Each fixture has five metrics, 1500 player IDs and 20 events per
player/metric: 150000 retained events. Queries contain both `player` and `region`
filters. These player IDs are not running Minecraft clients.

| Backend | Round 1 queries/s | Round 2 queries/s | Mean queries/s | p99 upper bound, ms |
| --- | ---: | ---: | ---: | --- |
| Pebble | 31319.6 | 32170.0 | 31744.8 | 4.10–4.72 |
| Next Gen | 13292.7 | 16331.6 | 14812.2 | 18.26–19.07 |

All four runs report zero errors and zero ambiguous events. Immutable timed
responses match independent SUM fingerprints, and all 7500 metric/player results
and leaderboard entity scores pass independent readback after drain. The verifier
also rejects altered ACK counts, history size, rates and missing rows.

Each backend receives 4 vCPUs and 4 GiB total. Pebble receives them in one process.
This candidate experiment assigns API 2 vCPUs/1 GiB, PostgreSQL 1.75 vCPUs/2.75 GiB,
and Redis 0.25 vCPU/0.25 GiB. The separate client receives 2 vCPUs/512 MiB. PostgreSQL
keeps fsync, full-page writes and synchronous commit enabled. No expiry, history
removal, uncertain-write replay or reduced durability is used.

Image identity:
`sha256:24f4fbb5179ce4c35a5132ae46a6116ba0c20da7407b26e0823139acede33912`
(`nextgen-parity-6`). Source and client fingerprints, parameters, logs and resource
samples are retained in `.compat/nextgen-parity/read-iteration6/`. The image is
pinned under a unique local tag before measurement. Owned containers, networks
and volumes are removed after label checks; prior services remain running.

Recheck the recorded experiment with:

```powershell
$env:FLAMEDB_COMPARISON_DIR='.compat/nextgen-parity/read-iteration6'
$env:FLAMEDB_COMPARISON_CASES='player-sum-20'
node deploy/compare-backends.cjs verify
node deploy/compare-backends.cjs controls
```

The parity checker requires both complete seven-case, two-round filter matrices
with the current source and correct ACK/readback counters. A partial matrix or
CPU-profiled run cannot certify parity. It checks events/s separately from read
queries/s and checks both rates in the mixed workload.

## Correctness and diagnostics

`node deploy/check-nextgen-parity.cjs correctness` builds the current server and
runs Go vet plus race-enabled integration checks against local PostgreSQL/Redis
with per-test SQL schemas. The PostgreSQL mode passes 113 top-level tests,
including frozen-reference TCP compatibility and concurrent crash recovery.
The TimescaleDB mode passes 33 backend tests. Required cases verify ordered float
SUM, tag/time predicates, overwrite/delete freshness, arbitrary identities,
automatic timestamps, independent overflow errors, schema backfill and reuse of
generic plans without leaking a session setting. The recorded source and binary
fingerprints are checked by `verify-correctness`.

The separate SQL-plan probe covers 150000 and 750000 events, one-tag and two-tag
filters, fresh/analyzed/vacuumed data and generic plans. Its four optimized plans
use the generated candidate index and avoid the unbounded timestamp-index scan;
their independently checked totals are 20 or 100. This is diagnostic plan evidence,
not a substitute for concurrent throughput measurements.

The updated Kotlin admission warning exposes the failure kind/status, bounded
reason/cause and journal usage/limit. All SDK tests and the 100000-event,
64-producer buffered stress test pass. One earlier run failed at the unrelated
150-ms warmup GET in a lost-ACK fixture. The fixture now gives that read a wider
budget; its timeout-write case still withholds the ACK beyond the deadline.
Unknown writes remain quarantined and are never automatically replayed. See
[pebble-batch-timeouts.md](pebble-batch-timeouts.md) for the reported 0.99 GB journal
and the operator-selected budget adjustment. The diagnostic SDK changes are local
and still need a release/plugin rollout.

## Experiments and remaining gaps

Read iteration 4 measured Next Gen 12368.8 versus Pebble 28960.8 queries/s, using
the earlier two-worker path and the original API/SQL/Redis CPU split of 1/2.5/0.5.
Iteration 5 tried a 500-microsecond collection window and the 2/1.75/0.25 split:
11601.4 versus 29331.3 queries/s, with Next Gen p99 bounds of 5.03–6.74 ms. It
improved tail latency but did not improve throughput. Iteration 6's four-worker,
100-microsecond path improves throughput but retains a substantial latency and
throughput gap. These are separate experiments, not interchangeable source proofs.

An earlier 750000-event fixture timed out before measurement. Another incomplete
experiment lost its old image identity after retagging the development image;
unique per-run image tags now prevent that failure. A prototype SQL helper failed
Timescale chunk-index creation because of function search-path resolution; the
candidate uses built-in expressions directly. Those failures and partial results
are retained, and none count as a completed performance matrix. CPU profile runs
are explicitly diagnostic and excluded from parity evidence.

The initial complete comparison's single-write, leaderboard and mixed-workload
shortfalls remain unqualified for the current candidate. Its batch-write results
cannot be assumed unchanged after adding the new event index. The actual
player-only filter has plan/correctness coverage but no final matched throughput
qualification. Further work must resolve those gaps and run the complete matrix.
The final measurement must also cover SDK journal overhead, the SDK's real read
connection count, a growing production-like history and Minecraft TPS/MSPT on a
shared host. No production Ryzen throughput or 1500-player capacity promise
follows from this short closed-loop test.
