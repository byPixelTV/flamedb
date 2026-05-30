package dev.bypixel.flamedb

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.PrintWriter
import java.net.Socket
import java.time.LocalDate

/**
 * FlameDB client with coroutine-based I/O.
 *
 * All public methods are `suspend` functions safe to call from any coroutine.
 * I/O is dispatched to [Dispatchers.IO]; the internal mutex serializes
 * concurrent callers over the single TCP connection.
 *
 * ```kotlin
 * val db = FlameDB.connect(FlameDBConfig(host = "127.0.0.1", port = 7777, apiKey = "flame_abc123"))
 *
 * // Write — lb= updates the all-time index, player= tag enables windowed queries
 * db.write("kills", 5.0, WriteOptions(leaderboardEntity = "pixel", tags = mapOf("player" to "pixel")))
 *
 * // All-time leaderboard (instant, pre-aggregated)
 * val allTime = db.leaderboard("kills", LeaderboardOptions(limit = 10))
 *
 * // Weekly leaderboard (on-the-fly from raw events)
 * val weekly = db.leaderboard("kills", LeaderboardOptions(from = "now-7d", entityTag = "player", limit = 10))
 *
 * // Group leaderboard with time window
 * val teams = db.groupLeaderboard(
 *     "kills",
 *     listOf(GroupDef("red", listOf("pixel", "notch")), GroupDef("blue", listOf("dream"))),
 *     LeaderboardOptions(from = "now-7d", entityTag = "player"),
 * )
 *
 * db.close()
 * ```
 */
class FlameDB private constructor(private val cfg: FlameDBConfig) : AutoCloseable {

    private val json = Json { ignoreUnknownKeys = true }
    private val mutex = Mutex()

    private lateinit var socket: Socket
    private lateinit var reader: BufferedReader
    private lateinit var writer: PrintWriter

    // ─── Factory ──────────────────────────────────────────────────────────────

    companion object {
        /**
         * Opens a TCP connection to FlameDB and authenticates.
         * Suspends on the IO dispatcher during the handshake.
         */
        suspend fun connect(cfg: FlameDBConfig): FlameDB = withContext(Dispatchers.IO) {
            val client = FlameDB(cfg)
            client.dial()
            client
        }
    }

    // ─── Connection ───────────────────────────────────────────────────────────

    private fun dial() {
        socket = Socket(cfg.host, cfg.port).also {
            it.soTimeout = cfg.timeoutMs
        }
        reader = BufferedReader(InputStreamReader(socket.getInputStream()))
        writer = PrintWriter(socket.getOutputStream(), true)
        performAuth()
    }

    private fun performAuth() {
        val challenge = readLine()
        val parsed = json.parseToJsonElement(challenge).jsonObject
        check(parsed["auth"]?.jsonPrimitive?.content == "required") {
            "FlameDB: unexpected auth challenge: $challenge"
        }

        sendLine("AUTH ${cfg.apiKey}")

        val resp = readLine()
        val authResp = json.parseToJsonElement(resp).jsonObject
        if (authResp["auth"]?.jsonPrimitive?.content != "ok") {
            val err = authResp["error"]?.jsonPrimitive?.content ?: resp
            throw FlameDBException("FlameDB auth failed: $err")
        }
    }

    private fun readLine(): String =
        reader.readLine() ?: throw FlameDBException("FlameDB connection closed")

    private fun sendLine(line: String) = writer.println(line)

    /**
     * Sends [lines] and reads one JSON response. Throws [FlameDBException] on
     * server-side errors.
     */
    private fun rawCommand(lines: List<String>): JsonObject {
        lines.forEach { sendLine(it) }
        val raw = readLine()
        val obj = json.parseToJsonElement(raw).jsonObject
        val err = obj["error"]?.jsonPrimitive?.content
        if (err != null) throw FlameDBException(err)
        return obj
    }

    /** Dispatches to IO and holds the mutex for the duration of the command. */
    private suspend fun command(lines: List<String>): JsonObject =
        withContext(Dispatchers.IO) {
            mutex.withLock { rawCommand(lines) }
        }

    private suspend fun command(line: String): JsonObject = command(listOf(line))

    // ─── Close ────────────────────────────────────────────────────────────────

    override fun close() {
        runCatching { socket.close() }
    }

    // ─── Write ────────────────────────────────────────────────────────────────

    /**
     * Sends a WRITE command.
     *
     * To make an entity eligible for windowed leaderboards, pass the entity ID
     * both as [WriteOptions.leaderboardEntity] (updates all-time index) **and**
     * as a tag (enables time-range queries):
     *
     * ```kotlin
     * db.write("kills", 5.0, WriteOptions(
     *     leaderboardEntity = "pixel",
     *     tags = mapOf("player" to "pixel", "region" to "eu"),
     * ))
     * ```
     */
    suspend fun write(metric: String, value: Double, options: WriteOptions = WriteOptions()) {
        command(buildWrite(metric, value, options))
    }

    /**
     * Sends a WRITE_BATCH command with multiple items in a single round-trip.
     */
    suspend fun writeBatch(items: List<WriteBatchItem>): BatchResult {
        val lines = buildList {
            add("WRITE_BATCH")
            items.forEach { add(buildWrite(it.metric, it.value, it.options)) }
            add("END")
        }
        val resp = command(lines)
        return json.decodeFromString(resp.toString())
    }

    private fun buildWrite(metric: String, value: Double, opts: WriteOptions): String =
        buildString {
            append("WRITE $metric $value")
            opts.leaderboardEntity?.let { append(""" lb="$it"""") }
            opts.tags.forEach { (k, v) -> append(""" $k="$v"""") }
            opts.timestampNs?.let { append(" ts=$it") }
            if (opts.quorum) append(" QUORUM")
        }

    // ─── Set ──────────────────────────────────────────────────────────────────

    /**
     * Sends a SET command (absolute leaderboard value, not an increment).
     */
    suspend fun set(metric: String, value: Double, leaderboardEntity: String? = null) {
        val cmd = buildString {
            append("SET $metric $value")
            leaderboardEntity?.let { append(""" lb="$it"""") }
        }
        command(cmd)
    }

    // ─── Delete ───────────────────────────────────────────────────────────────

    /**
     * Sends a DELETE command. Can delete a leaderboard entry or a time range
     * of raw events.
     */
    suspend fun delete(
        metric: String,
        leaderboardEntity: String? = null,
        from: LocalDate? = null,
        to: LocalDate? = null,
    ) {
        val cmd = buildString {
            append("DELETE $metric")
            leaderboardEntity?.let { append(""" lb="$it"""") }
            from?.let { append(" FROM $it") }
            to?.let { append(" TO $it") }
        }
        command(cmd)
    }

    // ─── Get ──────────────────────────────────────────────────────────────────

    /**
     * Sends a GET command and returns the raw events (or aggregated result).
     *
     * @param metrics One or more metric names. Multiple metrics are fetched
     *   in a single request and returned under [GetResult.metrics].
     */
    suspend fun get(vararg metrics: String, options: GetOptions = GetOptions()): GetResult {
        val cmd = buildString {
            append("GET ${metrics.joinToString(",")}")

            if (options.where.isNotEmpty()) {
                val clauses = options.where.entries.joinToString(" AND ") { (k, v) -> """$k="$v"""" }
                append(" WHERE $clauses")
            }
            options.from?.let { append(" FROM $it") }
            options.to?.let { append(" TO $it") }
            options.limit?.let { append(" LIMIT $it") }
            options.offset?.let { append(" OFFSET $it") }
            options.order?.let { append(" ORDER $it") }
        }
        val resp = command(cmd)
        return json.decodeFromString(resp.toString())
    }

    private fun buildMetricSpec(
        metrics: Array<out String>,
        aggregate: Aggregate? = null,
        groupBy: String? = null,
    ): String {
        require(metrics.isNotEmpty()) { "at least one metric is required" }
        val base = metrics.joinToString(",")
        val aggPart = aggregate?.let { " $it" } ?: ""
        val groupPart = groupBy?.let { " GROUP BY $it" } ?: ""
        return base + aggPart + groupPart
    }

    /** GET with aggregate (SUM/COUNT/AVG). */
    suspend fun getAggregate(
        aggregate: Aggregate,
        vararg metrics: String,
        options: GetOptions = GetOptions(),
    ): GetResult {
        val metricSpec = buildMetricSpec(metrics, aggregate = aggregate)
        return get(metricSpec, options = options)
    }

    /** GET with time buckets (GROUP BY). Aggregate is optional; default is SUM. */
    suspend fun getSeries(
        groupBy: String,
        vararg metrics: String,
        aggregate: Aggregate? = null,
        options: GetOptions = GetOptions(),
    ): GetResult {
        val metricSpec = buildMetricSpec(metrics, aggregate = aggregate, groupBy = groupBy)
        return get(metricSpec, options = options)
    }

    suspend fun getSum(vararg metrics: String, options: GetOptions = GetOptions()): GetResult =
        getAggregate(Aggregate.SUM, *metrics, options = options)

    suspend fun getCount(vararg metrics: String, options: GetOptions = GetOptions()): GetResult =
        getAggregate(Aggregate.COUNT, *metrics, options = options)

    suspend fun getAvg(vararg metrics: String, options: GetOptions = GetOptions()): GetResult =
        getAggregate(Aggregate.AVG, *metrics, options = options)

    // ─── Leaderboard ──────────────────────────────────────────────────────────

    /**
     * Sends a LEADERBOARD command.
     *
     * **All-time** (pre-aggregated, instant — no [LeaderboardOptions.entityTag] needed):
     * ```kotlin
     * db.leaderboard("kills", LeaderboardOptions(limit = 10))
     * ```
     *
     * **Time-windowed** (on-the-fly from raw events — [LeaderboardOptions.entityTag] required):
     * ```kotlin
     * db.leaderboard("kills", LeaderboardOptions(
     *     from = "now-7d",
     *     entityTag = "player",
     *     limit = 10,
     * ))
     * ```
     *
     * @throws FlameDBException if [from] or [to] is set but [entityTag] is null.
     */
    suspend fun leaderboard(
        metric: String,
        options: LeaderboardOptions = LeaderboardOptions(),
    ): List<LeaderboardEntry> {
        val windowed = options.from != null || options.to != null
        if (windowed && options.entityTag == null) {
            throw FlameDBException(
                "LeaderboardOptions.entityTag is required when from or to is set"
            )
        }

        val cmd = buildString {
            append("LEADERBOARD $metric")
            options.from?.let { append(" FROM $it") }
            options.to?.let { append(" TO $it") }
            // ENTITY muss vor LIMIT/OFFSET stehen (Parser-Reihenfolge ist flexibel,
            // aber konsistente Reihenfolge ist leichter zu lesen)
            if (windowed) options.entityTag?.let { append(" ENTITY $it") }
            options.limit?.let { append(" LIMIT $it") }
            options.offset?.let { append(" OFFSET $it") }
        }
        val resp = command(cmd)
        val raw = resp["leaderboard"]?.toString() ?: return emptyList()
        return json.decodeFromString(raw)
    }

    // ─── Group Leaderboard ────────────────────────────────────────────────────

    /**
     * Sends a GROUP_LEADERBOARD command for ad-hoc team/group ranking.
     *
     * **All-time:**
     * ```kotlin
     * db.groupLeaderboard(
     *     "kills",
     *     listOf(GroupDef("red", listOf("pixel", "notch")), GroupDef("blue", listOf("dream"))),
     * )
     * ```
     *
     * **Time-windowed** ([LeaderboardOptions.entityTag] required):
     * ```kotlin
     * db.groupLeaderboard(
     *     "kills",
     *     listOf(GroupDef("red", listOf("pixel", "notch")), GroupDef("blue", listOf("dream"))),
     *     LeaderboardOptions(from = "now-7d", entityTag = "player"),
     * )
     * ```
     *
     * @throws FlameDBException if [from] or [to] is set but [entityTag] is null.
     */
    suspend fun groupLeaderboard(
        metric: String,
        groups: List<GroupDef>,
        options: LeaderboardOptions = LeaderboardOptions(),
    ): List<GroupLeaderboardEntry> {
        val windowed = options.from != null || options.to != null
        if (windowed && options.entityTag == null) {
            throw FlameDBException(
                "LeaderboardOptions.entityTag is required when from or to is set"
            )
        }

        val cmd = buildString {
            append("GROUP_LEADERBOARD $metric")
            options.from?.let { append(" FROM $it") }
            options.to?.let { append(" TO $it") }
            if (windowed) options.entityTag?.let { append(" ENTITY $it") }
            groups.forEach { g ->
                append(""" GROUP "${g.name}:${g.members.joinToString(",")}"""")
            }
            options.limit?.let { append(" LIMIT $it") }
            options.offset?.let { append(" OFFSET $it") }
        }
        val resp = command(cmd)
        val raw = resp["leaderboard"]?.toString() ?: return emptyList()
        return json.decodeFromString(raw)
    }

    // ─── Stats ────────────────────────────────────────────────────────────────

    /**
     * Sends a STATS command.
     * Returns cardinality information for [tags] on [metric], useful for
     * understanding index selection and query performance.
     */
    suspend fun stats(metric: String, vararg tags: String): StatsResult {
        val cmd = "STATS $metric TAGS ${tags.joinToString(" ")}"
        val resp = command(cmd)
        return json.decodeFromString(resp.toString())
    }
}

// ─── Config ───────────────────────────────────────────────────────────────────

/**
 * Configuration for a [FlameDB] client.
 *
 * @property host FlameDB server host.
 * @property port FlameDB server port (default: 7777).
 * @property apiKey API key for authentication.
 * @property timeoutMs Socket read/write timeout in milliseconds (default: 5000).
 */
data class FlameDBConfig(
    val host: String,
    val port: Int = 7777,
    val apiKey: String,
    val timeoutMs: Int = 5_000,
)