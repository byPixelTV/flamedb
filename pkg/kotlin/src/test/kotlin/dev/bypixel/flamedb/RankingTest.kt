package dev.bypixel.flamedb

import java.util.concurrent.CopyOnWriteArrayList
import kotlinx.coroutines.runBlocking
import kotlin.test.*

class RankingTest {
    @Test fun wireOptionsNullZeroAndReadClassification() = runBlocking {
        val timings = CopyOnWriteArrayList<CommandTiming>()
        TcpFixture { socket, line, _ ->
            socket.reply(if (line.contains("\"missing\"")) """{"ranking":{"rank":null,"score":null,"count":3,"around":[]}}"""
                else """{"ranking":{"rank":2,"score":0,"count":3,"around":[{"entity_id":"a","value":10,"rank":1},{"entity_id":"🌍 \"quoted\"","value":0,"rank":2},{"entity_id":"z","value":-4,"rank":3}]}}""")
        }.use { server ->
            FlameDB.connect(server.config().copy(onCommandTiming = { timings.add(it) })).use { db ->
                val options = LeaderboardOptions(board="reason", limit=7, offset=2)
                assertEquals(2L, db.getRank("kills", "🌍 \"quoted\"", options))
                assertEquals(0.0, db.getScore("kills", "zero"))
                assertNull(db.getRank("kills", "missing")); assertNull(db.getScore("kills", "missing"))
                val rows = db.getAroundEntity("kills", "zero", 1, LeaderboardOptions(from="now-7d", to="now", entityTag="player:id"))
                assertEquals(listOf(1L,2L,3L),rows.map { it.rank }); assertEquals(0.0,rows[1].score)
                assertEquals(3L,db.getLeaderboardCount("kills",options))
                assertEquals(emptyList(),db.getAroundEntity("kills","missing",0))
                assertEquals(listOf("""RANK kills "🌍 \"quoted\"" BOARD reason LIMIT 7 OFFSET 2""", """SCORE kills "zero"""", """RANK kills "missing"""", """SCORE kills "missing"""",
                    """AROUND kills "zero" 1 FROM "now-7d" TO "now" ENTITY player:id""", "LEADERBOARD_COUNT kills BOARD reason LIMIT 7 OFFSET 2", """AROUND kills "missing" 0"""), server.commands.toList())
                assertTrue(timings.all { it.read && it.success })
            }
        }
    }

    @Test fun malformedRankingKeepsStructuredReadFailures() = runBlocking {
        var response = "{}"
        TcpFixture { socket, _, _ -> socket.reply(response) }.use { server ->
            FlameDB.connect(server.config()).use { db ->
                for (invalid in listOf("{}", """{"ranking":null}""", """{"ranking":{"rank":null,"score":null}}""", """{"ranking":{"rank":0,"score":0,"count":1}}""", """{"ranking":{"rank":null,"score":0,"count":1}}""")) {
                    response = invalid
                    val failure = assertFailsWith<FlameDBException> { db.getRank("m","a") }
                    assertEquals(FlameDBErrorKind.INVALID_RESPONSE,failure.kind); assertFalse(failure.mutating)
                }
                response = """{"ranking":{"rank":null,"score":null,"count":0}}"""
                assertFailsWith<FlameDBException> { db.getAroundEntity("m","a",1) }
                response = """{"error":"permission denied"}"""
                val failure = assertFailsWith<FlameDBException> { db.getScore("m","a") }
                assertEquals(FlameDBErrorKind.SERVER_REJECTION,failure.kind); assertFalse(failure.mutating)
            }
        }
    }

    @Test fun invalidOptionsNeverReachTheWire() = runBlocking {
        TcpFixture { socket, _, _ -> socket.reply("{}") }.use { server ->
            FlameDB.connect(server.config()).use { db ->
                for (options in listOf(LeaderboardOptions(from="now"),LeaderboardOptions(board="reason",from="now",entityTag="p"),LeaderboardOptions(board="bad:board"),LeaderboardOptions(limit=-1)))
                    assertFailsWith<IllegalArgumentException> { db.getRank("m","a",options) }
                assertFailsWith<IllegalArgumentException> { db.getAroundEntity("m","a",-1) }
                assertFailsWith<IllegalArgumentException> { db.getScore("m","") }
                assertFailsWith<IllegalArgumentException> { db.getRank("m 1","a") }
                assertTrue(server.commands.isEmpty())
            }
        }
    }
}
