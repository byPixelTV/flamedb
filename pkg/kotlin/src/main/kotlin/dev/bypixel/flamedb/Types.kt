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

@Serializable
data class LeaderboardEntry(
    @SerialName("EntityID") val entityId: String,
    @SerialName("Value") val score: Double,
)

@Serializable
data class GroupLeaderboardEntry(
    val group: String,
    val score: Double,
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
    /** Sets lb= and increments the leaderboard for this entity. */
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
    /** Inclusive start date (YYYY-MM-DD). */
    val from: String? = null,
    /** Inclusive end date (YYYY-MM-DD). */
    val to: String? = null,
    val limit: Int? = null,
    val offset: Int? = null,
    val order: SortOrder? = null,
)

enum class SortOrder { ASC, DESC }

/** Aggregate operators supported by GET. */
enum class Aggregate { SUM, COUNT, AVG }

/** Options for LEADERBOARD / GROUP_LEADERBOARD. */
data class LeaderboardOptions(
    val limit: Int? = null,
    val offset: Int? = null,
    /** Inclusive start date (YYYY-MM-DD). */
    val from: String? = null,
    /** Inclusive end date (YYYY-MM-DD). */
    val to: String? = null,
)

/** A group definition for GROUP_LEADERBOARD. */
data class GroupDef(
    val name: String,
    val members: List<String>,
)

/** One entry in a WRITE_BATCH. */
data class WriteBatchItem(
    val metric: String,
    val value: Double,
    val options: WriteOptions = WriteOptions(),
)

/** Thrown when FlameDB returns an error field in its JSON response. */
class FlameDBException(message: String) : RuntimeException(message)
