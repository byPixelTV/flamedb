# FlameDB — Kotlin SDK

Coroutine-based Kotlin client for FlameDB. Designed for Minecraft plugin devs
on Folia/Paper/Velocity. All methods are `suspend` functions — no blocking, no
callbacks.

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

The client serializes concurrent commands on one TCP connection. If sending or
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
