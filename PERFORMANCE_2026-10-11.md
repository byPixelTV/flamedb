# Windowed leaderboards and sustained memory use

Measured locally on 2026-10-11 with Go 1.27.1, Windows/amd64, Intel Core
i3-12100F (8 logical CPUs). These are synthetic storage benchmarks, not
production latency or whole-process RSS measurements.

## Query results

Fixture: 200,000 binary events, one per minute, 1,000 clans, three tags per event,
namespaced metric `craftattack:clans:joins`, flushed SSTables, 32 MiB Pebble block
cache, default compression. Fixture ingestion is excluded. Queries return the
top 10; the seven-day interval contains 10,081 events including its lower bound.
The test-only legacy implementation is frozen from commit `2c3a2af` and both
implementations read the same database. Each cell below is the median of three
runs of three measured queries; caches are warmed by benchmark calibration.

| Query | Previous latency | New latency | Speedup | Previous allocation/query | New allocation/query |
| --- | ---: | ---: | ---: | ---: | ---: |
| Last seven days | 56.72 ms | 1.002 ms | 56.6x | 4,586,717 B | 191,664 B |
| Full 200,000-event history | 751.57 ms | 38.32 ms | 19.6x | 82,975,765 B | 188,714 B |

The seven-day query allocates 95.8% fewer bytes; the full-history query allocates
99.8% fewer. Production results depend on events per window, distinct clans,
storage, compression, cache state and concurrent work. No latency guarantee is
implied for a production dataset that was not available here.

## Memory behavior

- Reproducing the previous 100,000-entry allocation on each cardinality flush
  costs a median 9,450,330 bytes/flush. Reusing small maps with otherwise identical
  database work costs 2,368 bytes/flush in the short benchmark. A separate warmed
  50-flush test with new distinct tags measured 261 bytes/flush.
- With 10,000 events and 100 clans, allocations/query fall from 90,037 to 131.
- Live Go heap after successive groups of 50 queries (250 total): 886,288;
  827,264; 824,408; 824,392; 824,376 bytes after GC. Pebble native/block-cache
  allocations and process RSS are not represented by this Go heap measurement.
- Idle cardinality cache entries now expire via the existing storage worker.
  The cache has both a count cap and an estimated key/entry byte budget, and its
  map shrinks after expiration. Large pending maps are released after bursts.

## Implementation and correctness

The old algorithm scans the entity-first index across all historical timestamps,
performs a random primary lookup for every matching event, and allocates a tag
map for every decoded event. The new algorithm scans primary keys directly in
`[FROM, TO)`, extracts the requested tag and score without per-event allocations
for binary values, and copies entity IDs only on first encounter. Small pages
use a candidate heap instead of sorting every entity. Returned pages own their
backing arrays. Legacy JSON is still decoded through the existing decoder.

There are no new persistent indexes, stale query caches, rounded time windows,
approximate scores or changes to write durability. Group queries retain member
index lookups and now use one snapshot for all members. Corrupt data and storage
errors are reported instead of silently returning partial scores.

Time-window queries still read all events inside the window and retain one
aggregate per distinct non-empty entity ID. They are not precomputed rollups.
Existing Pebble caches, memtables, replication and concurrent query state still
contribute to total server RAM.

Validation: full `go test ./...`, `go vet ./...`, targeted race tests, and the
five acceptance gates in [GATES.md](GATES.md) passed. Coverage includes the exact
reported query, exclusive upper bounds, ties/negative scores, pagination,
legacy JSON, namespaces, stale indexes after overwrites, deletes, corrupt data,
concurrent atomic writes and repeated-query heap use.

```bash
go test ./internal/storage -run '^$' -bench 'Benchmark(WindowedLeaderboard|CardinalityChurn)' -benchmem -benchtime=3x -count=3
go test ./internal/storage -run 'Test(CardinalityMemory|WindowedAllocations|WindowedSteadyStateMemory)' -count=1 -v
go test -race ./internal/storage ./internal/query -run 'Test(Windowed|Cardinality|ClanLeaderboard)' -count=1 -timeout=120s
```

The existing query syntax is unchanged. Rebuild and restart with the updated
server to use these improvements; no additional data migration is required by
this change.
