# Minecraft workload: agreed inputs and remaining qualification

Recorded on 2026-10-14. Inputs and requirements for the application capacity tests still to be implemented and run.

## User requirements

- 1,500 players on one Minecraft server, sharing the Ryzen 9 9900X host with FlameDB.
- Debian host; services run in Docker. Approximately 60 GB RAM and 200 GB storage are available beyond Minecraft's needs. Actual Docker limits and disk characteristics must be observed on that host.
- Each player produces 100–250 metric events every 1–5 seconds, including repeated metric names. Each leaderboard-enabled event updates its score. Five displayed leaderboards are read/refreshed every minute.
- Statistics are queried multiple times per second **per player**. The precise rate and query mix have not yet been specified.
- Preserve FlameDB's existing event/history and `SUM` behavior. No automatic event expiry, deletion or aggregation is introduced for this workload.
- The user reports that the live server has run for five days with little storage usage. Its exact size, event counts and effective write rate have not been measured here; the stress rates below are not observations of that deployment.

## Observed application path

The current EraMC checkout uses `MoonlightFlameDB.record()` and `recordThen()` for event admission. Moonlight now delegates to `FlameDB.buffered(...)` from the Kotlin SDK; its former private journal and sender have been removed. Developers submit individual events. The SDK owns the journal, batch assembly, independent flusher, one sender, safe retry decisions and shutdown; it awaits one `writeBatch()` at a time.

`platform/bukkit/Moonlight/src/main/resources/config.yml` currently sets batch size 128, flush interval 100 ms, one write connection and two read connections. The SDK assigns explicit nanosecond timestamps before journaling and uses them in the eventual batch, preserving the former application's behavior. These are checkout defaults, not an observation of a deployed server's effective configuration.

The SDK's default 256 MiB admission budget is configurable as `flamedb.ingestion.max-journal-bytes` and applies to local pending/audit journal files, not database history. A full budget rejects new local admission rather than silently deleting events. Old Moonlight journal framing and payload fields are compatible: the application fixture checks the exact recovered TCP payload, timestamp, tags, default/named boards and quorum option. Known-unsent data can recover; inflight/ambiguous data is quarantined without automatic replay.

SMP combat callbacks and scoreboards use `getSum()` with a `player` tag filter. The FlameDB backend computes that SUM from matching raw events, ordered by timestamp. Test data must therefore include historical events and per-player tags; leaderboard TOP/RANK throughput alone does not qualify these queries.

The test must distinguish locally admitted journal entries from database-confirmed events. Report pending journal events/bytes/age, confirmed events, errors and ambiguous outcomes. A fast admission rate with a growing unconfirmed journal does not establish sustainable ingestion. Do not automatically replay ambiguous mutations.

## Required test matrix

| Player event pattern | Total offered events/s |
| --- | ---: |
| 100 events every 5 seconds | 30,000 |
| 250 events every 5 seconds | 75,000 |
| 100 events every 1 second | 150,000 |
| 250 events every 1 second | 375,000 |

Run staggered player schedules and synchronized bursts. Include different and repeated metric names. Start with 250 metric names and the plugin's actual tags/entity IDs, and label this assumed cardinality until the real plugin workload is established.

Read-rate test levels of 2, 5 and 10 statistic queries/player/s mean 3,000, 7,500 and 15,000 queries/s. These are proposed stress levels, not a claim about the user's exact rate. Include per-player SUM and the application's additional filters. Measure five leaderboard refreshes each minute; if the application performs separate requests for every viewer, report and test that request multiplication too.

Measure both application-default 128-event batching with explicit timestamps and a single-WRITE control. The end-to-end test must include the actual SDK-managed Kotlin journal/sender and its read queue limits, rather than inferring their capacity from a multiconnection Go loader.

Use prefilled history at multiple sizes and simultaneous writes/reads. Compare isolated database capacity with the complete application sharing the host with Minecraft. Record Minecraft TPS/MSPT, write confirmation delay, statistic latency, queue growth, CPU/memory, disk/WAL growth and database correctness. Offered demand and achieved confirmed throughput must be reported separately; a client that submits only after each response can reduce demand as latency grows.

Independent final event counts/sums and entity scores must match all successful database acknowledgements. Include Redis loss, a database crash after response drain, restore from a backup, and recovery of the application journal without ambiguous replay. Process failover on one Docker host does not qualify failure of the whole host. Existing local SDK/backend/crash qualifications remain useful, but do not constitute this complete application capacity qualification.

## Storage measurement with existing semantics

The rates above describe stress cases with all 1,500 players producing the stated events continuously. They do not establish how many events the live server actually writes over a day. The application contains event-triggered writes and session-related writes; effective counts depend on activity and the deployed configuration. A hypothetical bytes/event estimate does not explain the user's observed five-day footprint.

Measure actual confirmed event counts and disk growth on representative data, including indexes, WAL and backup space. Distinguish the existing Pebble deployment from the PostgreSQL/Timescale backend and local benchmark databases; their physical footprints can differ. Preserve current player-filtered historical SUM and leaderboard semantics. Batch transport reduces command count and keeps individual events. Existing cache TTLs are not a raw-event retention policy, and no new retention decision is required for this integration.

## SDK adoption verification

`deploy/check-sdk-batching-adoption.cjs` checks that Moonlight uses the public SDK factory and contains no private journal, sender or `writeBatch()` calls. Set `FLAMEDB_ERAMC_SOURCE` to the EraMC checkout and run its `ownership`, `sdk` and `application` actions. The SDK and actual Moonlight adapter each exercise 100,000 events from 64 producers over local TCP fixtures, plus admission bounds, concurrent reads, shutdown/recovery and ambiguous responses. The Moonlight build uses its declared `0.1.1-dev.44` SDK dependency. These fixtures verify client behavior; they perform no database work and do not qualify the full workload's sustainable capacity.

Production qualification requires implementing the application-specific tests and running them on the actual Debian/Ryzen deployment alongside Minecraft. Its resource limits, disk characteristics and application capacity remain to be measured.
