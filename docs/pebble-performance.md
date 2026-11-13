# Pebble leaderboard lookup performance

When a leaderboard ID has no entity-index entry, FlameDB scans the board for
older rows that predate the index. A batch already shares one scan per board;
the scan no longer decodes complete JSON objects for ordinary rows whose entity
ID cannot match. It still decodes matching rows and alternate encodings, and
uses the ID in the persisted value rather than assuming it matches the key
suffix. The persisted keys, numeric scores and synchronous WAL commit are
unchanged.

`node deploy/check-pebble-only.cjs performance` compares the same benchmark on
commit `d02dc17` and the current code. It uses a real flushed Pebble database
with 32,768 existing IDs, four Go execution threads, and three alternating
measurements on an Intel Core i3-12100F. These are medians for the lookup, without
TCP, network, or WAL writes in the timed part:

| Lookup | Before | After | Allocations before → after |
| --- | ---: | ---: | ---: |
| One missing ID | 17.12 ms | 2.51 ms | 65,555 → 12 |
| 128 missing IDs sharing a scan | 16.72 ms | 3.15 ms | 66,059 → 521 |
| Existing indexed ID | 1.24 µs | 1.21 µs | 4 → 4 |

The full scan remains necessary for old rows when an ID is not indexed. These
figures describe that lookup and should not be read as a server-wide speedup.

`RANK`, `SCORE`, `AROUND` and `LEADERBOARD_COUNT` now run directly on Pebble. For
all-time boards they scan a single consistent view in existing score order;
`AROUND` only keeps the bounded neighborhood needed for the response. Windowed
commands use the existing time-window calculation. Legacy and current SDK
clients are exercised against the Pebble TCP server in the compatibility check.

`node deploy/check-pebble-only.cjs journal` also replayed the same 41,969 records
in 409 copied journal batches against fresh Linux Docker databases. Both servers
used four CPUs, 4 GiB, Zstd, two read connections, one write connection and the
same 5 s SDK deadline. In this run, confirmed write throughput went from 9,284 to
11,456 events/s (+23.4%); batch p99 went from 14.56 to 11.16 ms. Both runs had zero
unknown or rejected events. All raw payloads and 8,387 entity scores across seven
boards matched after `SIGKILL` and restart. The original journal was only read.
This is a short local replay, not a production capacity estimate.
