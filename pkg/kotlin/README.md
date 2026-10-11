# FlameDB — Kotlin SDK

Coroutine-based Kotlin client for FlameDB. Designed for Minecraft plugin devs
on Folia/Paper/Velocity. Network methods suspend the caller; blocking socket I/O
runs on `Dispatchers.IO`.

## Concurrency, deadlines and timings

The default pool has **two read connections and one write connection**. Connections open lazily; `connect` authenticates one write connection. GET, LEADERBOARD, GROUP_LEADERBOARD, STATS, METRICS and PREVIEW_DELETE use reserved reads; mutations use writes. A blocked write cannot occupy read capacity. Each leased connection owns one complete request/response; concurrent coroutines are no longer serialized on a client-wide mutex.

```kotlin
val db = FlameDB.connect(FlameDBConfig(
    host = "127.0.0.1", port = 7777, apiKey = apiKey,
    timeoutMs = 5000, readConnections = 2, writeConnections = 1,
    maxQueuedReads = 1024, maxQueuedWrites = 1024,
    maxResponseBytes = 8 * 1024 * 1024,
    onCommandTiming = { timing ->
        // queueWaitNanos and requestNanos are nanoseconds.
        // failure contains per-command error/outcome details, including cancellation.
        // Forward to a bounded, non-blocking metrics sink.
    },
))
```

`timeoutMs` is a total deadline per protocol command, including pool wait, lazy connect/auth, socket I/O, response JSON parsing and typed result conversion. An enclosing coroutine timeout/cancellation can stop it earlier. Multi-command helpers such as `deleteByTags` apply it per command. `queueWaitNanos` measures admission; `requestNanos` measures work after admission (including lazy authentication and result conversion). A call that never acquires a connection has zero request time. `success` includes result validation; `failure` contains the structured failure when unsuccessful. Observers run after successful I/O releases its lease; keep them quick and non-blocking, and safe for concurrent callbacks. Observer exceptions do not fail commands. On cancellation, the caller/observer may return before blocking-worker cleanup finishes; that lease is held until the worker actually exits. JVM scheduling, DNS lookup and non-interruptible decoding can delay cleanup beyond the caller's deadline.

Concurrent operations have **no ordering guarantee across connections**. Await a write before issuing a dependent read; do not depend on coroutine launch order. Increasing write capacity can reorder concurrent SET/DELETE/increment operations. Replication consistency is unchanged. For one shared connection, set `readConnections = 0, writeConnections = 1`.

Queued cancellation never sends the command or interrupts another caller's socket. Active cancellation closes the affected socket and holds its lease until the blocking worker actually exits; ambiguous writes are **not retried**. `close()` rejects queued calls and closes active sockets permanently. Pool sizes are fixed, failed sockets are replaced on demand, and no timing history is retained by the SDK.

## Admission and response limits

| Configuration | Default | Meaning |
|---|---|---|
| `readConnections` | `2` | Reserved read leases; zero explicitly shares write connections |
| `writeConnections` | `1` | Exclusive write leases |
| `maxQueuedReads` | `1024` | Maximum reads waiting for a lease |
| `maxQueuedWrites` | `1024` | Maximum writes waiting for a lease |
| `maxResponseBytes` | `8 * 1024 * 1024` | Maximum UTF-8 wire bytes in each response before LF |
| `timeoutMs` | `5000` | Total deadline for one protocol command |

Queue limits exclude active leases and must be non-negative; zero allows an immediately available connection but rejects any wait. Each class has its own admission budget. A full queue fails early with `OVERLOADED` and `NOT_SENT`, without opening a connection or sending the command. Cancellation, timeout and close release waiting reservations in `finally`; an active connection returns only after its worker exits. Admission bounds work retained by this client, not the number of coroutines/batch objects your application creates. Use a bounded producer and reuse a client rather than starting unlimited jobs or constructing clients per request.

`maxResponseBytes` must be positive. The limit applies separately to the initial auth challenge, the auth reply and every command response. It is enforced incrementally on raw bytes before accumulating the complete frame. LF is required and excluded from the count; an optional CR before LF is included in the count and removed before decoding. Exact-limit frames are accepted. UTF-8 is decoded strictly, including characters split across TCP reads; invalid or incomplete UTF-8 and EOF before LF are invalid responses, even when the partial bytes happen to be valid JSON. Responses must be JSON objects.

Oversized, invalid and truncated replies discard only the affected connection. Its next lease reconnects and authenticates; other pool connections remain usable. Slow trickle traffic does not refresh the total deadline. Response buffers grow only within the configured limit and are not retained between frames; a fixed input buffer of at most 8 KiB remains per connected socket. The byte limit is **not** a JVM heap ceiling: decoded strings, JSON trees and result objects use additional memory, as do simultaneously active responses. Choose limits and pagination for your heap budget; very large explicitly configured limits can still exhaust it.

## Structured failures and mutation outcomes

All transport/protocol failures remain `FlameDBException`. The existing `(message)` and `(message, cause)` constructors and catches remain available. New fields are `kind`, `command` (protocol verb), `phase`, `mutating`, `executionStatus` and `outcomeUnknown`. Available causes are preserved. Existing caller argument validation still uses `IllegalArgumentException`; legacy manually constructed `FlameDBException` defaults to `REQUEST_FAILED`/`NOT_SENT`.

| `kind` | Meaning / execution status |
|---|---|
| `OVERLOADED` | No waiting capacity; `NOT_SENT` |
| `POOL_WAIT_TIMEOUT` | Total SDK deadline expired before a lease; `NOT_SENT` |
| `CONNECTION` | TCP/DNS/connect deadline failed; command `NOT_SENT` |
| `AUTHENTICATION` | Auth rejection, unexpected challenge or auth I/O/deadline failure; command `NOT_SENT` |
| `SERVER_REJECTION` | Valid `error` response; connection remains usable; mutations conservatively `UNKNOWN` |
| `INVALID_RESPONSE` | Invalid UTF-8/JSON/object/schema, truncated response or invalid result counters; connection discarded |
| `RESPONSE_TOO_LARGE` | Byte limit exceeded while reading; connection discarded |
| `REQUEST_TIMEOUT` | Deadline expired after admission during request/result handling |
| `REQUEST_FAILED` | Other request I/O failure; connection discarded |
| `REQUEST_CANCELLED` | Details of native coroutine cancellation, delivered through timing/suppressed diagnostics |
| `CLIENT_CLOSED` | Permanent shutdown; queued calls `NOT_SENT`, active calls depend on send/response progress |

`phase` is `POOL_WAIT`, `CONNECT`, `AUTHENTICATE` or `REQUEST`. Invalid/oversized auth frames keep their response-specific `kind`, with `AUTHENTICATE` phase and `NOT_SENT` status.

`executionStatus` is `NOT_SENT` before the first command write attempt; `UNKNOWN` for a mutating request that may have reached the server without a validated result; `RESPONSE_RECEIVED` if a validated result reached the SDK but delivery to the caller lost a deadline/cancellation race; and `NOT_APPLICABLE` for sent reads. `RESPONSE_RECEIVED` does not mean an entire batch succeeded atomically. Because flush failures cannot prove how many bytes reached the server, the SDK marks writes conservatively from the first write attempt. A server rejection also cannot prove that no mutation occurred: quorum and batch failures may follow partial application. The SDK never automatically replays commands, including `UNKNOWN` mutations.

```kotlin
try {
    db.write("kills", 1.0)
} catch (cancelled: kotlinx.coroutines.CancellationException) {
    // Preserve structured concurrency. Native timeout/cancellation types are retained.
    // cancelled.flameDBFailure() can expose a suppressed diagnostic.
    throw cancelled
} catch (failure: FlameDBException) {
    when (failure.kind) {
        FlameDBErrorKind.OVERLOADED, FlameDBErrorKind.POOL_WAIT_TIMEOUT -> {
            // This call was NOT_SENT. Apply bounded application-level backpressure.
        }
        else -> {
            if (failure.outcomeUnknown) {
                // Reconcile the outcome; do not blindly repeat an increment.
            }
        }
    }
}
```

Native cancellation may be shared by a group of coroutines and copied by coroutine stack recovery. Suppressed diagnostics retain at most one SDK detail per cancellation cause, preventing a reused cancellation exception from growing an unbounded history. For the status of **each individual call**, use `onCommandTiming` and `CommandTiming.failure`; `flameDBFailure()` is a diagnostic helper and a shared outer cancellation can represent several operations. A close/cancellation race may report `CLIENT_CLOSED` when shutdown has already closed the pool. `deleteByTags` keeps its existing per-metric `error` strings and adds `MetricDeleteResult.failure` for structured details; coroutine cancellation still propagates.

## Compatibility and verification

Source callers can omit the new options. The new finite defaults intentionally change overload behavior: bursts beyond connection plus queue capacity now fail promptly instead of retaining unlimited waits. Increase queue limits explicitly if a previously tested workload needs more admitted callers; this is a memory/latency tradeoff, not faster database execution. Replies larger than 8 MiB, missing LF, or malformed UTF-8 that were previously accepted/replacement-decoded now fail. Existing missing-leaderboard/GET defaults are retained, while undecodable result fields and invalid batch/delete counters produce structured errors.

The additional data-class fields change generated constructor/default/copy JVM signatures (`FlameDBConfig`, `CommandTiming`, `MetricDeleteResult`); **recompile consuming Kotlin/Java applications**. Catching `FlameDBException` remains compatible, but use structured fields rather than parsing error message wording. No version has been published by this change. Explicit `writeBatch` remains available; an automatic batching queue is outside this scope.

Run `gradlew.bat test --no-daemon --console=plain` for the existing and new local TCP regression suites. `gradlew.bat sdkStressTest --no-daemon --console=plain` separately runs 100,000 mixed requests with 128 workers and a 10,000-caller overload/cancellation probe. The size rejection oracle is tested against a known unsafe `BufferedReader.readLine()` counterexample. See [HARDENING_RESULTS.md](HARDENING_RESULTS.md) for measured results and limits; these fixtures perform no database work.

## Dependency

```kotlin
// build.gradle.kts
dependencies {
    implementation("dev.bypixel:flamedb-kotlin-sdk:0.1.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.1")
}
```

## Usage

```kotlin
import dev.bypixel.flamedb.*
import kotlinx.coroutines.*

fun main() = runBlocking {
    // connect (suspends during TCP handshake + auth)
    val db = FlameDB.connect(
        FlameDBConfig(host = "127.0.0.1", port = 7777, apiKey = "flame_abc123")
    )

    // single write with leaderboard + tags
    db.write("kills", 1.0, WriteOptions(
        leaderboardEntity = "pixel",
        tags = mapOf("player" to "pixel", "region" to "eu"),
    ))

    // quorum write (waits for majority of replicas)
    db.write("money", 100.0, WriteOptions(
        leaderboardEntity = "pixel",
        quorum = true,
    ))

    // batch write — one round-trip for many events
    val result = db.writeBatch(listOf(
        WriteBatchItem("kills",  1.0, WriteOptions(leaderboardEntity = "pixel")),
        WriteBatchItem("deaths", 1.0, WriteOptions(leaderboardEntity = "pixel")),
        WriteBatchItem("tps",   19.8, WriteOptions(tags = mapOf("server" to "survival-1"))),
    ))
    println("batch: accepted=${result.accepted} failed=${result.failed}")

    // absolute leaderboard value
    db.set("money", 5000.0, leaderboardEntity = "pixel")

    // leaderboard (pre-computed, instant)
    val top10 = db.leaderboard("kills", LeaderboardOptions(limit = 10))
    top10.forEach { println("${it.entityId}: ${it.score}") }

    // group leaderboard
    val teams = db.groupLeaderboard("kills", listOf(
        GroupDef("team_red",  listOf("pixel", "notch")),
        GroupDef("team_blue", listOf("dream")),
    ))

    // get raw events with tag filter
    val events = db.get("kills", options = GetOptions(
        where  = mapOf("region" to "eu"),
        limit  = 100,
        order  = SortOrder.DESC,
    ))
    println(events.events)

    // time range + ordering
    val ranged = db.get("kills", options = GetOptions(
        where = mapOf("player" to "test"),
        from  = "2024-01-01",
        to    = "2024-12-31",
        order = SortOrder.ASC,
    ))

    // relative time range
    val lastWeek = db.get("kills", options = GetOptions(
        where = mapOf("player" to "test"),
        from  = "now-7d",
        to    = "now",
    ))

    // aggregates (all-time)
    val sum = db.getSum("kills", options = GetOptions(
        where = mapOf("player" to "test"),
    )).aggregate?.value

    val count = db.getCount("kills", options = GetOptions(
        where = mapOf("player" to "test"),
    )).aggregate?.value

    val avg = db.getAvg("kills", options = GetOptions(
        where = mapOf("player" to "test"),
    )).aggregate?.value

    // time series (GROUP BY)
    val series = db.getSeries(
        "1h",
        "kills",
        aggregate = Aggregate.SUM,
        options = GetOptions(where = mapOf("player" to "test")),
    ).series

    // time series for multiple metrics
    val multiSeries = db.getSeries(
        "1d",
        "kills",
        "deaths",
        aggregate = Aggregate.SUM,
        options = GetOptions(where = mapOf("player" to "test")),
    ).seriesByMetric

    // GROUP BY with COUNT / AVG
    val countSeries = db.getSeries(
        "1h",
        "kills",
        aggregate = Aggregate.COUNT,
        options = GetOptions(where = mapOf("player" to "test")),
    ).series

    val avgSeries = db.getSeries(
        "1h",
        "kills",
        aggregate = Aggregate.AVG,
        options = GetOptions(where = mapOf("player" to "test")),
    ).series

    // multi-metric get
    val multi = db.get("kills", "deaths", options = GetOptions(limit = 50))
    println(multi.metrics)

    // multi-metric aggregate
    val totals = db.getSum(
        "kills",
        "deaths",
        options = GetOptions(where = mapOf("player" to "test")),
    )
    val killsTotal = totals.aggregates?.get("kills")?.value
    val deathsTotal = totals.aggregates?.get("deaths")?.value

    // cardinality stats
    val stats = db.stats("kills", "player", "region")
    stats.tagStats.forEach { println("${it.tagKey}: ${it.cardinality}") }

    // delete a leaderboard entry
    db.delete("kills", leaderboardEntity = "pixel")

    db.close()
}
```

## Minecraft / Folia usage

```kotlin
// in a Paper/Folia plugin, launch from a coroutine scope:
plugin.launch { // or GlobalScope.launch(plugin.minecraftDispatcher)
    val db = FlameDB.connect(cfg)

    // called on kill event:
    db.write("kills", 1.0, WriteOptions(
        leaderboardEntity = player.uniqueId.toString(),
        tags = mapOf("world" to player.world.name),
    ))
}
```

## Thread safety

`FlameDB` uses a `Mutex` internally, so multiple coroutines can share one
instance safely. For high-throughput workloads, prefer `writeBatch()` to
amortize the round-trip cost.

## API

| method | description |
|---|---|
| `FlameDB.connect(cfg)` | Open + authenticate. Suspends on IO. |
| `write(metric, value, opts?)` | WRITE command |
| `writeBatch(items)` | WRITE_BATCH command |
| `set(metric, value, entity?)` | SET absolute leaderboard value |
| `delete(metric, entity?, from?, to?)` | DELETE entry or events |
| `get(vararg metrics, opts?)` | GET raw events or aggregates |
| `getSum/getCount/getAvg(metrics, opts?)` | GET aggregate helpers |
| `getSeries(groupBy, metrics, aggregate?, opts?)` | GET GROUP BY helpers |
| `Aggregate` | Aggregate operators: SUM, COUNT, AVG |
| `leaderboard(metric, opts?)` | LEADERBOARD sorted entries |
| `groupLeaderboard(metric, groups, opts?)` | GROUP_LEADERBOARD |
| `stats(metric, vararg tags)` | STATS cardinality |
| `close()` | Close TCP connection |

## Connection failures

Each pool connection serializes its own commands. If sending or
reading a response fails, it closes that connection and throws `FlameDBException`
with the original cause. The next command reconnects and authenticates before
sending. An explicit `close()` is permanent and disables reconnection.

A failed command is **never replayed automatically**: a write may have committed
before its response was lost. Do not blindly retry increments. A server error
response does not close an otherwise healthy connection.

Catch failures inside recurring update loops so a transient failure does not
terminate the coroutine. Always rethrow coroutine cancellation:

```kotlin
while (isActive) {
    try {
        val top = db.leaderboard("smp:deaths", LeaderboardOptions(limit = 10))
        // Reuse this result for holograms and placeholders.
        // Apply game-state updates on the platform's appropriate scheduler.
    } catch (cancelled: CancellationException) {
        throw cancelled
    } catch (error: Exception) {
        logger.log(java.util.logging.Level.WARNING, "Leaderboard refresh failed", error)
    }
    delay(60_000)
}
```

A top-10 response cannot provide a global rank for players outside those ten.

## Metric discovery and player UUIDs

Requires a server supporting `METRICS` and read permission. Returns sorted, unique names across all known cluster nodes (an unavailable node causes an error, not a partial list). Empty results are `[]`.

```kotlin
val id = player.uniqueId.toString()
val allKeys = db.listMetrics()
val playerKeys = db.listMetrics(mapOf("uuid" to id))
val events = db.get("kills", options = GetOptions(where = mapOf("uuid" to id)))
// Reset the all-time leaderboard entry for this player:
for (metric in allKeys) db.delete(metric, leaderboardEntity = id)
```

Store the UUID explicitly as an event tag, e.g. `WRITE kills 1 lb="<uuid>" uuid="<uuid>"`. The `lb` entity alone does not create a UUID tag. Multiple filters use AND and must match the same event. Filtered discovery includes only metrics with matching stored events; unfiltered discovery also includes leaderboard-only metrics. Discovery scans stored keys and is intended for administrative operations, not per-tick calls.

Deleting `lb="<uuid>"` resets only that all-time leaderboard entry. It does not delete historical events or reset windowed aggregates. Use tag-filtered deletion below to remove one player's historical events.

## Delete events by tag

Requires the updated server. `DELETE kills WHERE player="abc"` deletes only events with that exact tag value. Tag names are arbitrary, multiple conditions use AND, and optional FROM/TO bounds use an inclusive start and exclusive end. All secondary indexes are removed with the events, so GET, aggregates and windowed leaderboards reflect the deletion. Explicitly empty SDK filters are rejected; omit the filter only for the existing unfiltered deletion behavior.

```kotlin
val filter = mapOf("player" to player.uniqueId.toString())

// Delete one metric's matching events:
db.delete("kills", where = filter)

// Delete matching events across all discovered metrics:
for (metric in db.listMetrics(filter)) {
    db.delete(metric, where = filter)
}

// Full player reset when the UUID is also the leaderboard entity ID.
// Use all keys so leaderboard-only entries are included too.
for (metric in db.listMetrics()) {
    db.delete(metric, where = filter, leaderboardEntity = player.uniqueId.toString())
}
```

A tag filter alone does not change all-time leaderboard scores: stored events do not record the `lb` entity, and SET can independently override its score. To reset a known entity, combine the filter with `leaderboardEntity`; this removes that **entire** all-time entry regardless of the event filter/time range, atomically with the matching events for this metric. It does not subtract event values or infer entity IDs from tag values.

The loop across metrics is not an atomic cluster-wide operation. Stop concurrent writes for the player during a complete reset; new events may otherwise arrive during or after it. Errors propagate, and completed metric deletions are not rolled back. The server uses the existing primary routing and replication path; default replication remains asynchronous. Protocol callers can request `QUORUM`.

## Generic bulk deletion and preview

`deleteByTags` (`DeleteByTags` in Go) requires a non-empty tag filter and processes discovered metrics sequentially. Optional time bounds and an explicit leaderboard entity have the same semantics as single-metric deletion. With an explicit leaderboard entity, discovery includes all metrics so leaderboard-only entries are handled too.

```kotlin
val filter = mapOf("account" to accountId)
val preview = db.deleteByTags(filter, dryRun = true)
println("${preview.events} events, ${preview.leaderboardEntries} leaderboard entries")

val deleted = db.deleteByTags(filter)
for (item in deleted.metrics) {
    if (item.error != null) println("${item.metric}: ${item.error}")
    else println("${item.metric}: ${item.result?.events} deleted events")
}
// Optional: remove this explicit all-time entity across all metrics as well.
val reset = db.deleteByTags(filter, leaderboardEntity = accountId)
// Single-metric preview/result:
val one = db.delete("requests", where = filter, dryRun = true)
```

Results contain the dry-run flag, total event/leaderboard-entry counts, and a per-metric list with either a result or an error. In preview mode counts mean **would be deleted**; otherwise they mean **confirmed deleted**. A missing entity and an existing zero-score entity are distinguished. Zero matches succeed with zero counts.

Discovery failures abort the call. Per-metric failures are collected while remaining metrics are attempted. Totals exclude failed/unknown outcomes: a transport or replication error may occur after the primary already applied a deletion. The helper does not retry failed mutations automatically. Go returns a partial report plus an error on context cancellation; Kotlin propagates coroutine cancellation.

Preview and execution are separate calls, not a reserved snapshot or a transaction across metrics. Concurrent writes can change the counts. Inspect `metrics` for errors before treating a preview or deletion as complete. Both require write permission; discovery additionally requires read permission.

Update servers before using these SDK methods. The preview sends `PREVIEW_DELETE <metric> [WHERE ...] [lb="..."] [FROM ...] [TO ...]`, which performs no replication or data changes. Actual `DELETE` now returns `{"delete":{"dry_run":false,"events":42,"leaderboard_entries":1}}`; preview returns the same shape with `dry_run:true`. Missing/inconsistent counters are reported as errors instead of being presented as a successful zero count.
