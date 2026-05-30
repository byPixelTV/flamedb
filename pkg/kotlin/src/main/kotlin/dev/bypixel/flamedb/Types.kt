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

// ─── Custom Serializer für unsaubere API-Numbers ──────────────────────────────

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

class FlameDBException(message: String) : RuntimeException(message)