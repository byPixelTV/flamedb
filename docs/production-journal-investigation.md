# Real Moonlight journal investigation

The supplied directory contained 410 files and 12603997 bytes: one empty writer
lock and 409 `.uncertain` batches with 41969 records. Every frame passed its length,
CRC32, strict UTF-8 and schema checks. Record IDs were unique. This is the supplied
12 MB excerpt, not the previously reported 0.99 GB production journal.

These files represent unknown production outcomes, not unsent writes. They were
read only. Byte-identical copies were renamed to pending **only inside isolated
test-owned directories** and sent only to fresh local Docker databases. No
production event was replayed or reconciled, and no original file was removed or
renamed. The harness re-hashes every original file after each run.

## Workload and application defect

| Metric | Events | Distinct leaderboard entities |
| --- | ---: | ---: |
| craftattack:blocks-broken | 14554 | 108 |
| craftattack:damage-dealt | 13451 | 92 |
| craftattack:mob-kills | 7961 | 7961 |
| craftattack:blocks-placed | 4102 | 96 |
| craftattack:damage-taken | 1851 | 103 |
| playtime:sessions | 39 | 19 |
| craftattack:deaths | 11 | 8 |

Every record updates the default board. There are no named-board options in this
excerpt; this does not imply the application has only one displayed leaderboard.
The player tag has 8085 distinct UUID values. Most of that cardinality is mobs,
not players: CACore and EcoSmpCore's entity-death listeners attributed mob kills
to the killed mob UUID in the leaderboard and `player` tag. Both now use the
non-null killer UUID instead. These EraMC edits are local and uncommitted. Both
module shadow JARs were built and their packaged listener bytecode was checked.
Old module-local build directories contain stale JARs; the verified artifacts
come from the current root `build/libs` output. No historical correction is
attempted: the supplied mob-kill events do not identify their actual killer.

## Pebble lookup change

An entity absent from `lb-entity` previously triggered a full scan of its board
for compatibility with legacy rows lacking an entity index. Every new mob UUID
therefore repeated that scan. Within a batch, `LockedBatchLookup` now shares one
legacy scan per board. It retains values only for the batch's locked targets;
normal entity-index lookups still take precedence, legacy rows remain supported,
and missing targets stay zero. All target locks cover the scan through the atomic
durable commit. No score cache survives a batch, and original float accumulation,
raw payloads, overflow checks and one synchronous WAL commit are preserved.
Iterator errors fail the mutation instead of treating an incomplete scan as a
missing score.

A regression with 8192 existing entities and 128 new IDs independently counts
1048576 scanned rows before versus 8192 after. It also checks indexed/legacy/missing
scores, named boards and stale legacy sort keys. Existing concurrent SET, receipt,
overflow, overwrite, signed score and reopen regressions pass with race detection
and vet for the affected packages.

## Actual SDK reproduction

Both runs used the real buffered Kotlin SDK, the same byte-identical 409 source
batches, one write connection, two statistics readers, unchanged 5000 ms command
deadline, Zstd, replication factor one, fresh Linux Docker volumes, and a server
limit of four CPUs/four GiB. Source metric names, UUIDs, values, tags, timestamps
and batch boundaries were preserved. The candidate image was built from the
committed dev.47 source plus only the Pebble batch-lookup change, excluding the
uncommitted NextGen work.

| Measurement | Published dev.47 | Optimized local candidate |
| --- | ---: | ---: |
| Confirmed events | 41969 | 41969 |
| Write batches | 409 | 409 |
| Drain time | 29.845 s | 5.979 s |
| Confirmed events/s | 1406.23 | 7018.93 |
| p50 batch time | 51.104 ms | 9.887 ms |
| p99 batch time | 237.174 ms | 23.659 ms |
| Maximum batch time | 290.099 ms | 89.240 ms |
| Concurrent statistics reads | 3666 | 698 |
| Unknown/rejected events | 0 / 0 | 0 / 0 |

The reader loops use the same queries and pacing. The faster drain has a shorter
reader lifetime, so read counts differ. Independent complete readback checks all
41969 raw timestamps/values/tag payloads, seven boards with 8387 metric/entity
scores, and ordered SUM results. Both checks pass after SIGKILL and restart without
sending further writes.

This short local comparison demonstrates a roughly fivefold drain improvement
for the supplied real workload and about tenfold lower p99. Neither run reproduced
the production 4.525-second batches or timeouts. The uncertain excerpt does not
contain the production Pebble history, complete board sizes, filesystem latency
or simultaneous Minecraft load. The lookup and attribution defects are established;
they are not proof of the only production timeout cause or of 1500-player capacity.

Evidence remains under `.compat/production-journal/`: source SHA256 inventory,
baseline/candidate environments and measurements, SDK output, server timing logs,
SIGKILL readback, module artifact hashes and ordinary SDK test output. Test
containers and volumes are removed only after ownership/image/label checks;
source and copied audit files remain available.

Run `node deploy/check-production-journal.cjs inspect <journal-directory>` first.
`replay` uses the pinned published dev.47 image; `candidate` uses
`flamedb:production-journal-fix` unless overridden with `FLAMEDB_JOURNAL_IMAGE`.
`node deploy/check-journal-killer.cjs` verifies module artifacts and ordinary SDK
tests. Original unknown outcomes stay quarantined in production. Server/module
updates fix future handling; they do not restore historical killers or authorize
replaying uncertain events.
