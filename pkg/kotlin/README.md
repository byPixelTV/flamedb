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
