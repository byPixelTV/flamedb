package dev.bypixel.flamedb

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

// ─── Result Types ─────────────────────────────────────────────────────────────

@Serializable
data class Event(
    val timestamp: Long,          // unix nanoseconds
    val metric: String,
    val value: Double,
    val tags: Map<String, String> = emptyMap(),
)

/**
 * One entry in a LEADERBOARD response.
 *
 * Field names match the JSON emitted by types.go:
 *   {"entity_id":"pixel","value":42.0}
 */
@Serializable
data class LeaderboardEntry(
    @SerialName("entity_id") val entityId: String,
    @SerialName("value")     val score: Double,
)

/**
 * One entry in a GROUP_LEADERBOARD response.
 * The group name is in [group]; score is the sum of all members.
 */
@Serializable
data class GroupLeaderboardEntry(
    @SerialName("entity_id") val group: String,
    @SerialName("value")     val score: Double,
)

@Serializable
data class SeriesPoint(
    val ts: Long,
    val value: Double,
    val count: Int,
)

@Serializable
data class AggregateResult(
    val type: String,
    val value: Double,
    val count: Int,
)

@Serializable
data class TagStats(
    @SerialName("tag_key") val tagKey: String,
    val cardinality: Int,
)

@Serializable
data class StatsResult(
    val metric: String,
    @SerialName("tag_stats") val tagStats: List<TagStats> = emptyList(),
)

@Serializable
data class GetResult(
    val events: List<Event> = emptyList(),
    val metrics: Map<String, List<Event>> = emptyMap(),
    val aggregate: AggregateResult? = null,
    val aggregates: Map<String, AggregateResult> = emptyMap(),
    val series: List<SeriesPoint> = emptyList(),
    @SerialName("series_by_metric") val seriesByMetric: Map<String, List<SeriesPoint>> = emptyMap(),
)

@Serializable
data class BatchItemError(
    val index: Int,
    val error: String,
)

@Serializable
data class BatchResult(
    val ok: Boolean,
    val accepted: Int,
    val failed: Int,
    val errors: List<BatchItemError> = emptyList(),
)

// ─── Option Types ─────────────────────────────────────────────────────────────

/** Options for a WRITE command. */
data class WriteOptions(
    /** Sets lb= and increments the all-time leaderboard for this entity. */
    val leaderboardEntity: String? = null,
    val tags: Map<String, String> = emptyMap(),
    /** Override timestamp in unix nanoseconds. */
    val timestampNs: Long? = null,
    /** Wait for quorum of replicas before returning ok. */
    val quorum: Boolean = false,
)

/** Options for a GET command. */
data class GetOptions(
    val where: Map<String, String> = emptyMap(),
    /** Start of time range, e.g. "now-7d" or "2026-01-01". */
    val from: String? = null,
    /** End of time range, e.g. "now" or "2026-02-01". */
    val to: String? = null,
    val limit: Int? = null,
    val offset: Int? = null,
    val order: SortOrder? = null,
)

enum class SortOrder { ASC, DESC }

/** Aggregate operators supported by GET. */
enum class Aggregate { SUM, COUNT, AVG }

/**
 * Options for LEADERBOARD and GROUP_LEADERBOARD.
 *
 * When [from] or [to] is set the server computes the leaderboard on-the-fly
 * from raw events. In that case [entityTag] is **required** — it names the
 * tag whose value is used as the entity ID.
 *
 * Example: if you write `WRITE kills 5 lb="pixel" player="pixel"` then
 * [entityTag] should be `"player"`.
 *
 * Without [from]/[to] the pre-aggregated all-time index is used and
 * [entityTag] is ignored.
 */
data class LeaderboardOptions(
    val limit: Int? = null,
    val offset: Int? = null,
    /** Start of time window, e.g. "now-7d" or "2026-01-01". */
    val from: String? = null,
    /** End of time window, e.g. "now" or "2026-02-01". */
    val to: String? = null,
    /**
     * Tag key whose value is the entity ID.
     * Required when [from] or [to] is set, ignored otherwise.
     */
    val entityTag: String? = null,
)

/** A group definition for GROUP_LEADERBOARD. */
data class GroupDef(
    val name: String,
    val members: List<String>,
)

/** One item in a WRITE_BATCH request. */
data class WriteBatchItem(
    val metric: String,
    val value: Double,
    val options: WriteOptions = WriteOptions(),
)

/** Thrown when FlameDB returns an error or the connection is broken. */
class FlameDBException(message: String) : RuntimeException(message)