package consumer

import dev.bypixel.flamedb.*
import kotlinx.coroutines.runBlocking

fun main(args: Array<String>) = runBlocking {
    val metric = args[1]+".kotlin"
    val cfg = FlameDBConfig(args[0].substringBeforeLast(':'),args[0].substringAfterLast(':').toInt(),"compat-key",timeoutMs=10_000)
    FlameDB.connect(cfg).use { db ->
        for (entity in listOf("b","a","zero")) db.write(metric,if (entity=="zero") 0.0 else 5.0,WriteOptions(leaderboardEntity=entity,leaderboardEntities=mapOf("team" to entity),tags=mapOf("player" to entity),timestampNs=1704067200000000001L+entity[0].code))
        check(db.getRank(metric,"b")==2L); check(db.getScore(metric,"zero")==0.0)
        check(db.getRank(metric,"missing")==null); check(db.getScore(metric,"missing")==null)
        check(db.getAroundEntity(metric,"b",1)==listOf(RankedEntry("a",5.0,1),RankedEntry("b",5.0,2),RankedEntry("zero",0.0,3)))
        check(db.getLeaderboardCount(metric,LeaderboardOptions(board="team",limit=1,offset=2))==3L)
        val window = LeaderboardOptions(from="2024-01-01",to="2024-01-02",entityTag="player")
        check(db.getRank(metric,"b",window)==2L); check(db.getLeaderboardCount(metric,window)==3L)
        check(db.getAroundEntity(metric,"missing",0,window).isEmpty())
    }
    println("RANKING_KOTLIN_SDK_OK")
}
