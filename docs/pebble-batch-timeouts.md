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
