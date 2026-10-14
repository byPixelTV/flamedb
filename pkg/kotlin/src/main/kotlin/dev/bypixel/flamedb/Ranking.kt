package dev.bypixel.flamedb

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json

/** Global one-based ordinal rank in the selected leaderboard's tie order. */
@Serializable
data class RankedEntry(
    @SerialName("entity_id") val entityId: String,
    @SerialName("value") val score: Double,
    val rank: Long,
)

@Serializable
internal data class RankingResult(
    val rank: Long?,
    val score: Double?,
    val count: Long,
    val around: List<RankedEntry>? = null,
) {
    fun validate(requireAround: Boolean) {
        check(count >= 0 && (rank == null) == (score == null)) { "inconsistent ranking result" }
        check(rank == null || rank in 1..count) { "invalid ranking rank" }
        check(score == null || score.isFinite()) { "invalid ranking score" }
        check(!requireAround || around != null) { "missing ranking around entries" }
        around?.forEach { check(it.entityId.isNotEmpty() && it.rank in 1..count && it.score.isFinite()) { "invalid ranking around entry" } }
    }
}

/** Returns the global one-based rank, or null if the entity is absent. */
suspend fun FlameDB.getRank(metric: String, entity: String, options: LeaderboardOptions = LeaderboardOptions()): Long? =
    rankingCommand(rankingLine("RANK", metric, entity, null, options)).rank

/** Returns the current score (including zero), or null for an absent entity. */
suspend fun FlameDB.getScore(metric: String, entity: String, options: LeaderboardOptions = LeaderboardOptions()): Double? =
    rankingCommand(rankingLine("SCORE", metric, entity, null, options)).score

/** Entity plus up to radius neighbors on either side, retaining global ranks.
 * An absent entity returns an empty list. */
suspend fun FlameDB.getAroundEntity(metric: String, entity: String, radius: Int, options: LeaderboardOptions = LeaderboardOptions()): List<RankedEntry> {
    require(radius in 0..100_000) { "invalid ranking radius" }
    return rankingCommand(rankingLine("AROUND", metric, entity, radius, options)).around!!
}

/** Full entity count, including zero scores; pagination never changes the count. */
suspend fun FlameDB.getLeaderboardCount(metric: String, options: LeaderboardOptions = LeaderboardOptions()): Long =
    rankingCommand(rankingLine("LEADERBOARD_COUNT", metric, null, null, options)).count

private fun rankingIdentifier(value: String): String {
    require(value.isNotEmpty() && value.none { it.isWhitespace() || it == '\u0000' || it == '\u001f' || it in "=\"," }) { "invalid ranking identifier" }
    return value
}

private fun rankingLine(command: String, metric: String, entity: String?, radius: Int?, options: LeaderboardOptions): String = buildString {
    append("$command ${rankingIdentifier(metric)}")
    if (entity != null) {
        require(entity.isNotEmpty()) { "invalid ranking entity" }
        append(" ${Json.encodeToString(entity)}")
    }
    radius?.let { append(" $it") }
    val windowed = options.from != null || options.to != null
    require(!windowed || options.entityTag != null) { "entityTag required with from or to" }
    options.board?.let {
        require(!windowed && ':' !in it) { "BOARD cannot be combined with FROM/TO or contain a colon" }
        append(" BOARD ${rankingIdentifier(it)}")
    }
    // JSON quoting accepts RFC3339, dates and relative times while ensuring that
    // arbitrary string options cannot inject a second clause or protocol line.
    options.from?.let { append(" FROM ${Json.encodeToString(it)}") }
    options.to?.let { append(" TO ${Json.encodeToString(it)}") }
    options.entityTag?.let { val tag = rankingIdentifier(it); if (windowed) append(" ENTITY $tag") }
    options.limit?.let { require(it in 0..1_000_000) { "invalid ranking limit" }; append(" LIMIT $it") }
    options.offset?.let { require(it >= 0) { "invalid ranking offset" }; append(" OFFSET $it") }
}
