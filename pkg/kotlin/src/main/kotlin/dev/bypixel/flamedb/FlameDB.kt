package dev.bypixel.flamedb

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.decodeFromJsonElement
import kotlinx.serialization.encodeToString
import java.time.LocalDate

/**
 * FlameDB client with coroutine-based I/O.
 *
 * All public methods are `suspend` functions safe to call from any coroutine.
 * Reserved read/write pools dispatch blocking socket I/O to Dispatchers.IO.
 * Each leased connection has exclusive response ownership.
 */
class FlameDB private constructor(private val cfg: FlameDBConfig) : AutoCloseable {

    private val json = Json { ignoreUnknownKeys = true }
    private val transport = CommandPool(cfg, json)

    companion object {
        /** Authenticates one connection; other pool connections open on demand. */
        suspend fun connect(cfg: FlameDBConfig): FlameDB {
            val client = FlameDB(cfg)
            try {
                client.transport.connect()
                return client
            } catch (error: Throwable) {
                client.close()
                throw error
            }
        }
    }

    private suspend fun command(lines: List<String>): JsonObject = transport.command(lines)
    private suspend fun command(line: String): JsonObject = command(listOf(line))

    override fun close() = transport.close()

    // ─── Write ────────────────────────────────────────────────────────────────

    /**
     * Sends a WRITE command.
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
        return json.decodeFromJsonElement(resp)
    }

    private fun buildWrite(metric: String, value: Double, opts: WriteOptions): String =
        buildString {
            require(value.isFinite()) { "value must be finite" }
            append("WRITE ${identifier(metric)} $value")
            opts.leaderboardEntity?.let { append(" lb=${json.encodeToString(it)}") }
            opts.leaderboardEntities.forEach { (board, entity) ->
                append(" lb.${identifier(board)}=${json.encodeToString(entity)}")
            }
            opts.tags.forEach { (k, v) -> append(" ${identifier(k)}=${json.encodeToString(v)}") }
            opts.timestampNs?.let { append(" ts=$it") }
            if (opts.quorum) append(" QUORUM")
        }

    // ─── Set ──────────────────────────────────────────────────────────────────

    /**
     * Sends a SET command (absolute leaderboard value, not an increment).
     */
    suspend fun set(metric: String, value: Double, leaderboardEntity: String? = null) {
        val cmd = buildString {
            require(value.isFinite()) { "value must be finite" }
            append("SET ${identifier(metric)} $value")
            leaderboardEntity?.let { append(" lb=${json.encodeToString(it)}") }
        }
        command(cmd)
    }

    // ─── Delete ───────────────────────────────────────────────────────────────

    /**
     * Deletes raw events matching all tags in [where], optionally within a time range.
     * With [leaderboardEntity], also removes that entire all-time leaderboard entry.
     * Without [where], existing leaderboard-only / metric-range deletion is preserved.
     */
    suspend fun delete(
        metric: String,
        leaderboardEntity: String? = null,
        from: LocalDate? = null,
        to: LocalDate? = null,
        where: Map<String, String>? = null,
        dryRun: Boolean = false,
    ): DeleteResult {
        require(where == null || where.isNotEmpty()) { "delete filter must not be empty" }
        val cmd = buildString {
            append(if (dryRun) "PREVIEW_DELETE ${identifier(metric)}" else "DELETE ${identifier(metric)}")
            where?.let {
                val clauses = it.entries.joinToString(" AND ") { (k, v) -> "${identifier(k)}=${json.encodeToString(v)}" }
                append(" WHERE $clauses")
            }
            leaderboardEntity?.let { append(" lb=${json.encodeToString(it)}") }
            from?.let { append(" FROM $it") }
            to?.let { append(" TO $it") }
        }
        val resp = command(cmd)
        val result = json.decodeFromJsonElement<DeleteResult>(resp["delete"] ?: throw FlameDBException("missing delete result (updated server required)"))
        if (result.dryRun != dryRun || result.events < 0 || result.leaderboardEntries < 0) throw FlameDBException("inconsistent delete result")
        return result
    }

    /** Per-metric errors may have applied remotely; totals include confirmed results only. */
    suspend fun deleteByTags(
        where: Map<String, String>,
        leaderboardEntity: String? = null,
        from: LocalDate? = null,
        to: LocalDate? = null,
        dryRun: Boolean = false,
    ): DeleteByTagsResult {
        require(where.isNotEmpty()) { "delete filter must not be empty" }
        where.keys.forEach { identifier(it) }
        val keys = listMetrics(if (leaderboardEntity != null) emptyMap() else where)
        val results = mutableListOf<MetricDeleteResult>()
        for (metric in keys) {
            try {
                results += MetricDeleteResult(metric, delete(metric, leaderboardEntity, from, to, where, dryRun))
            } catch (e: kotlinx.coroutines.CancellationException) {
                throw e
            } catch (e: Exception) {
                results += MetricDeleteResult(metric, error = e.message ?: e.toString())
            }
        }
        return DeleteByTagsResult(dryRun, results.sumOf { it.result?.events ?: 0L },
            results.sumOf { it.result?.leaderboardEntries ?: 0L }, results)
    }

    // ─── Get ──────────────────────────────────────────────────────────────────

    /**
     * Sends a GET command and returns the raw events (or aggregated result).
     */
    suspend fun get(vararg metrics: String, options: GetOptions = GetOptions()): GetResult {
        require(metrics.isNotEmpty()) { "at least one metric is required" }
        return getWithSpec(metrics.joinToString(",") { identifier(it) }, options)
    }

    private suspend fun getWithSpec(metricSpec: String, options: GetOptions): GetResult {
        val cmd = buildString {
            append("GET $metricSpec")

            if (options.where.isNotEmpty()) {
                val clauses = options.where.entries.joinToString(" AND ") { (k, v) -> "${identifier(k)}=${json.encodeToString(v)}" }
                append(" WHERE $clauses")
            }
            options.from?.let { append(" FROM $it") }
            options.to?.let { append(" TO $it") }
            options.limit?.let { append(" LIMIT $it") }
            options.offset?.let { append(" OFFSET $it") }
            options.order?.let { append(" ORDER $it") }
        }
        val resp = command(cmd)
        return json.decodeFromJsonElement(resp)
    }

    private fun buildMetricSpec(
        metrics: Array<out String>,
        aggregate: Aggregate? = null,
        groupBy: String? = null,
    ): String {
        require(metrics.isNotEmpty()) { "at least one metric is required" }
        val base = metrics.joinToString(",") { identifier(it) }
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
        return getWithSpec(metricSpec, options)
    }

    /** GET with time buckets (GROUP BY). Aggregate is optional; default is SUM. */
    suspend fun getSeries(
        groupBy: String,
        vararg metrics: String,
        aggregate: Aggregate? = null,
        options: GetOptions = GetOptions(),
    ): GetResult {
        val metricSpec = buildMetricSpec(metrics, aggregate = aggregate, groupBy = groupBy)
        return getWithSpec(metricSpec, options)
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
            append("LEADERBOARD ${identifier(metric)}")
            options.board?.let { append(" BOARD ${identifier(it)}") }
            options.from?.let { append(" FROM $it") }
            options.to?.let { append(" TO $it") }
            if (windowed) options.entityTag?.let { append(" ENTITY ${identifier(it)}") }
            options.limit?.let { append(" LIMIT $it") }
            options.offset?.let { append(" OFFSET $it") }
        }
        val resp = command(cmd)
        val rawElement = resp["leaderboard"] ?: return emptyList()
        return json.decodeFromJsonElement(rawElement)
    }

    // ─── Group Leaderboard ────────────────────────────────────────────────────

    /**
     * Sends a GROUP_LEADERBOARD command for ad-hoc team/group ranking.
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
            append("GROUP_LEADERBOARD ${identifier(metric)}")
            options.board?.let { append(" BOARD ${identifier(it)}") }
            options.from?.let { append(" FROM $it") }
            options.to?.let { append(" TO $it") }
            if (windowed) options.entityTag?.let { append(" ENTITY ${identifier(it)}") }
            groups.forEach { g ->
                append(" GROUP ${json.encodeToString(g.name + ":" + g.members.joinToString(","))}")
            }
            options.limit?.let { append(" LIMIT $it") }
            options.offset?.let { append(" OFFSET $it") }
        }
        val resp = command(cmd)
        val rawElement = resp["leaderboard"] ?: return emptyList()
        return json.decodeFromJsonElement(rawElement)
    }

    // ─── Stats ────────────────────────────────────────────────────────────────

    /**
     * Sends a STATS command.
     */
    /** Lists cluster-wide metric names, optionally filtered by event tags. */
    suspend fun listMetrics(where: Map<String, String> = emptyMap()): List<String> {
        val clauses = where.entries.joinToString(" AND ") { (k, v) -> "${identifier(k)}=${json.encodeToString(v)}" }
        val resp = command("METRICS" + if (clauses.isEmpty()) "" else " WHERE $clauses")
        return json.decodeFromJsonElement(resp["metric_keys"] ?: throw FlameDBException("missing metric_keys"))
    }

    suspend fun stats(metric: String, vararg tags: String): StatsResult {
        val cmd = "STATS ${identifier(metric)} TAGS ${tags.joinToString(" ") { identifier(it) }}"
        val resp = command(cmd)
        return json.decodeFromJsonElement(resp["stats"] ?: throw FlameDBException("missing stats"))
    }

    private fun identifier(value: String): String {
        require(value.isNotEmpty() && value.none { it.isWhitespace() || it == '\u0000' || it == '\u001f' || it in "=\"," }) { "invalid identifier" }; return value
    }
}

// ─── Config ───────────────────────────────────────────────────────────────────

/**
 * Configuration for a [FlameDB] client.
 */
data class FlameDBConfig(
    val host: String,
    val port: Int = 7777,
    val apiKey: String,
    /** Total command deadline including pool wait, connection/auth and socket I/O. */
    val timeoutMs: Int = 5_000,
    val readConnections: Int = 2,
    val writeConnections: Int = 1,
    /** Called when a command completes or fails. Keep the callback quick/non-blocking. */
    val onCommandTiming: ((CommandTiming) -> Unit)? = null,
)
