package dev.bypixel.flamedb

import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.JsonDecoder
import kotlinx.serialization.json.double
import kotlinx.serialization.json.jsonPrimitive

// ─── Custom serializer for inconsistent API number formats ──────────────────────────────

object FlexibleDoubleSerializer : KSerializer<Double> {
    override val descriptor: SerialDescriptor =
        PrimitiveSerialDescriptor("FlexibleDouble", PrimitiveKind.DOUBLE)

    override fun deserialize(decoder: Decoder): Double {
        val jsonPrimitive = (decoder as JsonDecoder).decodeJsonElement().jsonPrimitive
        return jsonPrimitive.double
    }

    override fun serialize(encoder: Encoder, value: Double) {
        encoder.encodeDouble(value)
    }
}

// ─── Result Types ─────────────────────────────────────────────────────────────

@Serializable
data class Event(
    val timestamp: Long,          // unix nanoseconds
    val metric: String,
    @Serializable(with = FlexibleDoubleSerializer::class) val value: Double,
    val tags: Map<String, String> = emptyMap(),
)

/**
 * One entry in a LEADERBOARD response.
 */
@Serializable
data class LeaderboardEntry(
    @SerialName("entity_id") val entityId: String,
    @Serializable(with = FlexibleDoubleSerializer::class) @SerialName("value") val score: Double,
)

/**
 * One entry in a GROUP_LEADERBOARD response.
 */
@Serializable
data class GroupLeaderboardEntry(
    @SerialName("entity_id") val group: String,
    @Serializable(with = FlexibleDoubleSerializer::class) @SerialName("value") val score: Double,
)

@Serializable
data class SeriesPoint(
    val ts: Long,
    @Serializable(with = FlexibleDoubleSerializer::class) val value: Double,
    val count: Int,
)

@Serializable
data class AggregateResult(
    val type: String,
    @Serializable(with = FlexibleDoubleSerializer::class) val value: Double,
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

data class WriteOptions(
    val leaderboardEntity: String? = null,
    val tags: Map<String, String> = emptyMap(),
    val timestampNs: Long? = null,
    val quorum: Boolean = false,
    val leaderboardEntities: Map<String, String> = emptyMap(),
)

data class GetOptions(
    val where: Map<String, String> = emptyMap(),
    val from: String? = null,
    val to: String? = null,
    val limit: Int? = null,
    val offset: Int? = null,
    val order: SortOrder? = null,
)

enum class SortOrder { ASC, DESC }

enum class Aggregate { SUM, COUNT, AVG }

data class LeaderboardOptions(
    val limit: Int? = null,
    val offset: Int? = null,
    val from: String? = null,
    val to: String? = null,
    val entityTag: String? = null,
    val board: String? = null,
)

data class GroupDef(
    val name: String,
    val members: List<String>,
)

data class WriteBatchItem(
    val metric: String,
    @Serializable(with = FlexibleDoubleSerializer::class) val value: Double,
    val options: WriteOptions = WriteOptions(),
)

enum class FlameDBErrorKind {
    OVERLOADED, POOL_WAIT_TIMEOUT, CONNECTION, AUTHENTICATION, SERVER_REJECTION,
    INVALID_RESPONSE, RESPONSE_TOO_LARGE, REQUEST_TIMEOUT, REQUEST_FAILED,
    REQUEST_CANCELLED, CLIENT_CLOSED,
}

enum class FlameDBPhase { POOL_WAIT, CONNECT, AUTHENTICATE, REQUEST }

/** RESPONSE_RECEIVED means a validated response reached the SDK, not atomic batch success. */
enum class ExecutionStatus { NOT_SENT, UNKNOWN, RESPONSE_RECEIVED, NOT_APPLICABLE }

class FlameDBException(
    message: String,
    val kind: FlameDBErrorKind,
    val command: String? = null,
    val mutating: Boolean = false,
    val executionStatus: ExecutionStatus = ExecutionStatus.NOT_SENT,
    val phase: FlameDBPhase = FlameDBPhase.REQUEST,
    cause: Throwable? = null,
) : RuntimeException(message, cause) {
    // Preserve both existing JVM constructor signatures and catch compatibility.
    constructor(message: String) : this(message, FlameDBErrorKind.REQUEST_FAILED)
    constructor(message: String, cause: Throwable) : this(message, FlameDBErrorKind.REQUEST_FAILED, cause = cause)

    val outcomeUnknown: Boolean get() = executionStatus == ExecutionStatus.UNKNOWN
}

/** Cancellation retains its native type. Shared cancellation causes carry one diagnostic;
 * use CommandTiming.failure for details of each individual call in a cancelled group. */
fun Throwable.flameDBFailure(): FlameDBException? {
    val seen = java.util.Collections.newSetFromMap(java.util.IdentityHashMap<Throwable, Boolean>())
    val pending = java.util.ArrayDeque<Throwable>()
    pending.add(this)
    while (pending.isNotEmpty()) {
        val error = pending.removeFirst()
        if (!seen.add(error)) continue
        if (error is FlameDBException) return error
        error.suppressed.forEach { pending.add(it) }
        error.cause?.let { pending.add(it) }
    }
    return null
}

/** Counts are a preview when dryRun is true, otherwise confirmed deletions. */
@Serializable
data class DeleteResult(
 @SerialName("dry_run") val dryRun: Boolean,
 val events: Long,
 @SerialName("leaderboard_entries") val leaderboardEntries: Long,
)
data class MetricDeleteResult(val metric: String, val result: DeleteResult? = null, val error: String? = null,
                            val failure: FlameDBException? = null)
data class DeleteByTagsResult(
 val dryRun: Boolean,
 val events: Long,
 val leaderboardEntries: Long,
 val metrics: List<MetricDeleteResult>,
)
