package consumer

import dev.bypixel.flamedb.*
import java.nio.file.Path
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking

fun main(args: Array<String>) = runBlocking {
    val host = args[0].substringBeforeLast(':'); val port = args[0].substringAfterLast(':').toInt()
    val prefix = args[1]; val metric = "$prefix.kotlin"
    val cfg = FlameDBConfig(host,port,"compat-key",timeoutMs=10_000)
    FlameDB.connect(cfg).use { db ->
        db.write(metric,5.0,WriteOptions(leaderboardEntity="alice",leaderboardEntities=mapOf("reason" to "fall"),tags=mapOf("suite" to prefix,"player" to "alice"),timestampNs=1704067200000000001L))
        if (args.getOrNull(3) == "raw-only") {
            check(db.get(metric).events.single().timestamp==1704067200000000001L)
            println("FROZEN_RAW_KOTLIN_SDK_OK"); return@runBlocking
        }
        val br = db.writeBatch(listOf(WriteBatchItem(metric,3.0,WriteOptions(leaderboardEntity="bob",tags=mapOf("suite" to prefix,"player" to "bob"),timestampNs=1704070800000000002L)),WriteBatchItem(metric,-2.0,WriteOptions(leaderboardEntity="alice",tags=mapOf("suite" to prefix,"player" to "alice"),timestampNs=1704153600000000003L))))
        check(br.ok && br.accepted==2 && br.failed==0)
        if (args.getOrNull(3) != "skip-raw") {
            val get = db.get(metric,options=GetOptions(where=mapOf("player" to "alice"),order=SortOrder.ASC))
            check(get.events.size==2 && get.events[0].timestamp==1704067200000000001L && get.events[1].value == -2.0)
            db.write("$metric.other",1.0,WriteOptions(timestampNs=1704067200000000004L))
            check(db.get(metric,"$metric.other").metrics[metric]!!.size==3)
            check(db.get(metric,"$metric.absent").metrics["$metric.absent"]!!.isEmpty())
        }
        check(db.getSum(metric).aggregate!!.value==6.0)
        check(db.getCount(metric).aggregate!!.value==3.0)
        check(db.getAvg(metric).aggregate!!.value==2.0)
        check(db.getAggregate(Aggregate.SUM,metric).aggregate!!.count==3)
        check(db.getSeries("1d",metric).series.size==2)
        check(db.getSum(metric,"$metric.absent").aggregates["$metric.absent"]!!.count==0)
        check(db.getSeries("1d",metric,"$metric.absent").seriesByMetric["$metric.absent"]!!.isEmpty())
        check(db.leaderboard(metric)==listOf(LeaderboardEntry("alice",3.0),LeaderboardEntry("bob",3.0)))
        check(db.leaderboard(metric,LeaderboardOptions(board="reason"))==listOf(LeaderboardEntry("fall",5.0)))
        check(db.leaderboard(metric,LeaderboardOptions(from="2024-01-01",to="2024-01-02",entityTag="player"))==listOf(LeaderboardEntry("alice",5.0),LeaderboardEntry("bob",3.0)))
        check(db.groupLeaderboard(metric,listOf(GroupDef("team",listOf("alice","bob")))).single().score==6.0)
        db.set(metric,0.0,"alice"); check(db.leaderboard(metric,LeaderboardOptions(offset=1)).single().score==0.0)
        check(db.stats(metric,"player").metric==metric)
        check(db.listMetrics(mapOf("suite" to prefix))==listOf(metric))
        check(db.delete(metric,leaderboardEntity="alice",where=mapOf("player" to "alice"),dryRun=true)==DeleteResult(true,2,1))
        db.delete(metric,leaderboardEntity="alice")
        val report = db.deleteByTags(mapOf("suite" to prefix)); check(report.events==3L && report.metrics.size==1)
        check(db.get(metric).events.isEmpty()); check(db.leaderboard(metric).single().score==3.0)
        db.delete(metric,leaderboardEntity="bob")
        db.delete("$metric.other")
    }
    // Exercise the untouched buffered SDK and its local admission -> native batch
    // -> confirmed count -> shutdown path against the actual TCP server.
    val buffered = FlameDB.buffered(cfg,Path.of(args[2]),BufferedWriteConfig(batchSize=2,flushMs=20,shutdownMs=10_000))
    try {
        check(buffered.record("$prefix.buffered",2.0,WriteOptions(leaderboardEntity="alice",tags=mapOf("suite" to prefix))))
        check(buffered.record("$prefix.buffered",3.0,WriteOptions(leaderboardEntity="alice",tags=mapOf("suite" to prefix))))
        repeat(200) { if (buffered.statistics().confirmed < 2) delay(25) }
        check(buffered.statistics().confirmed==2L && buffered.statistics().uncertain==0L)
        check(buffered.getSum("$prefix.buffered").aggregate!!.value==5.0)
        check(buffered.leaderboard("$prefix.buffered").single().score==5.0)
        check(buffered.groupLeaderboard("$prefix.buffered",listOf(GroupDef("team",listOf("alice")))).single().score==5.0)
        check(buffered.directClient().listMetrics(mapOf("suite" to prefix)).contains("$prefix.buffered"))
    } finally { buffered.shutdown() }
    check(buffered.auditFiles().isEmpty())
    println("FROZEN_KOTLIN_SDK_OK")
}
