# Pebble leaderboard batch timeout fix

The 2026-10-14 production report used the existing Pebble server:
`batch failed (REQUEST_TIMEOUT, status=UNKNOWN); no replay`, with 770,952 locally
admitted, 128,177 confirmed and 117,541 uncertain events. These counters do not
establish an event rate or prove that uncertain events were lost. UNKNOWN means
the SDK could not validate the response after sending; some or all of that batch
may already have changed history and scores.

## Server change

The previous `Executor.ExecuteBatch` flushed plain events together but called
`executeMutation` separately for every leaderboard event. Each call committed
with `pebble.Sync`. A normal SDK batch of 128 leaderboard events therefore caused
128 sequential durable WAL syncs on its single sender connection.

`internal/query/pebble_batch.go` now stages raw events, tag indexes, default/named
scores and replica receipts in one indexed Pebble batch and performs one durable
commit. Score locks cover the entire commit. Repeated updates use previous staged
scores in original input order, including floating-point rounding and negative
scores. Intermediate sorted-score keys are removed. A concurrent SET runs before
or after the whole batch. Overflows fail only their item, without creating its
event or changing any of its boards. Duplicate replica receipts, including repeats
within one batch and after restart, do not repeat increments. Repeated explicit
timestamps also remove the previous staged event's tag indexes.

The server still acknowledges only after synchronous commit. No retention policy,
data expiry or automatic replay of ambiguous client mutations was introduced.
This removes a demonstrated ingestion bottleneck; other causes of a live timeout,
such as disk stalls, compaction or replication delays, still require live evidence.

## Local measurements

Both native measurements used the same i3-12100F Windows host, real Pebble WAL
syncs, 125 batches of 128 events per repetition, 250 metric names and an entity ID
range of 1,500. Three repetitions per version independently checked all 16,000
events and 16,000 metric/entity scores. The median of the three repetitions is:

| Measurement | Before | After |
| --- | ---: | ---: |
| Executor batch duration | 151.72 ms | 5.32 ms |
| Confirmed events/s | 843.6 | 24,069 |
| Confirmed batch commands/s | 6.591 | 188.0 |
| Observed WAL syncs/batch | 128 | 1.04 |

The median batch duration improved 28.53 times. The WAL histogram also observes
background work; a separate isolated test verifies exactly one WAL sync for a
128-event batch updating two boards. Replacing the new batch method with its
previous implementation makes that test fail with exactly 128 syncs, and the
fixed source was restored before final verification. WAL bytes/batch also fell
from a median 85,048 to 75,713; every original event was retained.

The built Linux Docker image was separately tested over real TCP with 16,000
events, two boards per event and the unchanged 5,000 ms response deadline.
It confirmed 10,226.96 events/s (79.90 batch commands/s), with p99 batch response
latency 19.24 ms and no errors or ambiguous responses. Complete raw payloads,
counts, sums and both boards matched before and after SIGKILL and restart, without
replaying any writes. This is a short isolated fixture, not a sustained Minecraft
capacity test or a measurement on the production Ryzen host.

Artifacts are in `.compat/pebble-batch-timeout`: before/after native output,
comparison JSON, negative-control output, Docker TCP results and server WAL
recovery logs. `deploy/check-pebble-batch.cjs measurements` validates the measured
inputs/results. Its `tcp` action creates and removes its own labeled test container;
the test database files remain under that artifact directory. Local Go regressions,
race detection and vet for affected packages passed. External PostgreSQL/HA
integration suites are separate from this Pebble fix.

## Using the fix

### Existing GHCR deployment and EraMC

[deploy/pebble.compose.yml](../deploy/pebble.compose.yml) preserves the supplied
single-container deployment, networks, port, command and data mount. Keep using
the existing Compose project and volume; starting it under a different project
name can create a different `flamedb-data` volume. The mounted configuration must
keep `server.data_path: /root/data-node1` (or `./data-node1`, relative to the image's
`/root` working directory). An absent `storage.engine`, or `storage.engine: pebble`,
uses Pebble unless PostgreSQL is explicitly configured. No PostgreSQL/Redis
services or data migration are needed for this fix.

The repository's Build & Release workflow publishes `ghcr.io/bypixeltv/flamedb:dev`
after a push to `master`, following successful binary and SDK build/publish jobs.
A local Git commit alone does not update that registry tag. Wait for the workflow's
Docker job to finish successfully, then run these commands from the directory of
the existing production Compose file:

```sh
docker compose pull flamedb
docker compose up -d --no-deps flamedb
docker compose logs --tail=100 flamedb
```

Take a consistent backup of the existing Pebble data before replacing the service.
Preserve the existing volume; do not use `docker compose down -v`. These commands
update the server image; they do not install an EraMC plugin. Build and deploy
Moonlight separately from the EraMC changes that delegate ingestion to
`FlameDB.buffered(...)`. Its declared Kotlin SDK `0.1.1-dev.44` already provides
that factory, so adopting batching does not depend on a new SDK version from this
server release. Existing `plugins/Moonlight/flamedb-metrics` journals stay in place
and retain their uncertain outcomes. Observe confirmed/uncertain/rejected counters
and batch latency after rollout; a local benchmark does not prove live capacity.

The ordinary Dockerfile now uses the Go 1.27.2 builder already required by this
checkout. The existing Pebble configuration and data mounts remain applicable.
Build the updated server with:

```sh
docker build -f Dockerfile -t flamedb:pebble-batch-fix --build-arg VERSION=pebble-batch-fix .
```

That image was built and checked locally; it has not been deployed to the user's
server or pushed to a registry. Updating the SDK alone does not install this
server-side change. Retain the existing data volume and journal during rollout.

The 117,541 previously uncertain events remain unresolved by this throughput fix.
Keep their audit files intact; do not rename them to pending or blindly resend
them. Their exact payloads/timestamps and authoritative history/scores must be
reconciled before deciding any repair. Raising a timeout or getting a faster
server response does not retroactively classify those outcomes.

## Local journal admission failure

`local admission failed; event NOT acknowledged` occurs before the SDK accepts
the new event. It is separate from a server request timeout. The SDK and the
previous Moonlight configuration defaulted to 268435456 bytes (256 MiB), counting
retained pending and audit files across restarts. A reported 0.99 GB directory
therefore points to an exhausted budget if that default is still configured;
directory size alone does not establish the running configuration or exclude an
I/O failure. For the reported deployment, the user confirmed that the Moonlight
setting is absent, so the previous hardcoded 256 MiB fallback applies.

With sufficient free disk space, explicitly allow a larger backlog in Moonlight's
configuration and restart its writer:

```yaml
flamedb:
  ingestion:
    max-journal-bytes: 4294967296 # 4 GiB, operator-selected backlog capacity
```

This is a plugin setting, not the FlameDB server's `/root/config.yml`. Raising it
does not fix a sustained producer/consumer imbalance or reconcile unknown writes.
Preserve uncertain/partial/recovered files. The updated SDK diagnostic reports
failure kind, execution status, bounded reason/cause, `journal-bytes` and
`max-journal-bytes`; those fields distinguish an admission limit from another
local failure. The diagnostic change still needs an SDK build and plugin rollout.

`admitted=0` alongside positive `confirmed` can happen after restarting with
pending files: admissions count new calls in this process, while confirmations
also include recovered pending events. Rejected calls were not journaled, so
callers must respect the Boolean return value. Restart does not reset retained
bytes or erase audit material.

## Follow-up for the deployed dev.45 single-node server

The reported server already runs `0.1.1-dev.45`, with Zstd, no seeds and
replication factor one. This is the repaired batch implementation; the older
per-event fsync issue is not a sufficient explanation for its remaining live
timeouts. Their exact server-side cause has not been established from the client
warning alone. The current admission rejection has a separate cause: retained
journal bytes exceed Moonlight's implicit 256 MiB budget.

The next Pebble change stages each final leaderboard score once per atomic batch,
while applying every accepted increment and overflow check in input order. It
retains all original raw events and the single synchronous WAL commit. Single
writes also avoid a second lookup of a score already read under its entity lock.
Slow Pebble batches (at least 250 ms) now report entry/ACK counts and timings for
receive, lock wait, local execution, replication and response. No payloads, tag
values, metric names or credentials are included in that diagnostic.

Against the deployed dev.45 source, the local native Windows benchmark uses
Zstd, five boards, 128 events per batch, four Go execution threads and three
identical 125-batch runs per case. For repeated metric/player scores, median
batch time falls from 19.07 ms to 2.50 ms (7.62 times faster); WAL bytes per batch
fall from 102155 to 8082 (92.1% less). For 250 distinct metrics, WAL bytes are
unchanged and median time is 26.35 ms versus 24.08 ms. The repeated-score gain
must not be applied to workloads containing only distinct score keys. Both
cases independently verify all 16000 events and five boards in every run.

An actual buffered Kotlin SDK check against a local four-CPU/four-GiB Linux Docker
Pebble server with Zstd confirms 30000 events for five metrics and 1500 player IDs,
including 7500 definitely-unsent events recovered from an offline journal. Sixty
four producers run alongside two read connections, issuing 3351 statistics reads.
All events and five boards are correct; uncertain/rejected counts are zero and
the acknowledged state survives SIGKILL/restart without resending any write.
Across 235 SDK write batches, measured p99 is 787.56 ms and maximum 1173.01 ms,
below the unchanged five-second command deadline. This is a checked synthetic
burst, not a sustained 1500-player Minecraft capacity test. Local slow-batch logs
attribute the observed delay to local execution; they do not establish the cause
on the production host.

Moonlight's local configuration reader, ingestion model and packaged YAML now use
a 4 GiB fallback/default for this setup. An existing explicitly configured value
still wins. These EraMC edits remain uncommitted. The generic SDK default remains
256 MiB; other users must choose their own capacity. The new Moonlight behavior
requires building/replacing the plugin and restarting its writer; pulling the
server image alone cannot change a plugin's admission budget.

Do **not delete the journal directory**. `.pending` and `.open` contain unsent
events; deleting them loses those events. `.inflight`, `.uncertain`, `.partial`,
`.result.json` and recovery/audit material must be retained for reconciliation.
Unknown writes may already be reflected in leaderboard scores, so blindly
resending them can double-count increments. Confirmed `.done` files are already
cleaned by the SDK; restarting does not purge unknown data. A faster server does
not retroactively resolve those outcomes.

Evidence is retained in `.compat/pebble-journal-rootcause/`. Run
`node deploy/check-pebble-journal.cjs correctness`, `benchmark` and `sdk` for the
required checks. The real SDK test is opt-in (`realPebbleTest`) and requires a
test-owned Docker fixture, so ordinary SDK tests do not need a running database.

## Production evidence after updating to dev.46

The supplied 2026-10-14 23:49:38–23:50:12 UTC logs confirm dev.46 started and
recovered the existing WAL. All displayed slow batches accepted their entries,
with zero failed items. Local processing took 256–4525 ms; receive, server mutation
lock wait, replication and response took zero milliseconds after rounding. This
locates the observed delay inside local execution, but does not yet distinguish
score/event reads, the synchronous commit, entity locks or cardinality processing.
There is no batch exceeding five seconds in this excerpt and no client timeout
correlated with these particular batches. The historical unknown outcomes remain
unresolved. Do not infer a total event rate from logs that omit fast batches.

The follow-up diagnostic adds `entity_lock_ms`, `stage_ms`, `score_read_ms`,
`event_stage_ms`, `commit_ms`, `cardinality_ms`, `legacy_scans` and
`legacy_entries` to the existing slow-batch log. `score_read_ms` and
`event_stage_ms` are portions of `stage_ms`, not additional time. Entity locking,
staging, commit and cardinality are successive portions of `local_ms`. Commit
includes Pebble commit admission/write stalls and the durable WAL sync; a high
`commit_ms` alone does not prove a slow physical disk. Score reads include the
legacy fallback scan used when an entity index is missing. Scan counters measure
that work without logging player IDs, metrics, scores or tags.
Pebble's formerly discarded write-stall events now log `Pebble write stall begin
reason=...` and `Pebble write stall end` when it pauses ingestion for memory-table
or level-zero pressure. Healthy writes do not emit those events.

This change diagnoses the remaining delay; it does not claim to resolve it or
raise the SDK deadline. A test-owned in-memory filesystem explicitly blocks WAL
sync and verifies the mutation remains unacknowledged until release and the delay
is attributed to commit. Failed staging still never commits. Additional tests
check indexed/legacy/missing lookups and the unchanged JSON batch reply, alongside
the ordered score/history, overflow, overwrite, receipt and recovery regressions.
Run `node deploy/check-pebble-local-delay.cjs` for race tests and vet.
