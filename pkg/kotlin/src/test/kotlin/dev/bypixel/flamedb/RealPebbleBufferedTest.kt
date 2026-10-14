package dev.bypixel.flamedb

import java.net.ServerSocket
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.ConcurrentLinkedQueue
import kotlinx.coroutines.*
import kotlin.test.*

/** Invoked only by realPebbleTest; missing fixture information is an error. */
class RealPebbleBufferedTest {
    private val prefix = requireNotNull(System.getenv("FLAMEDB_PEBBLE_PREFIX"))
    private val journal = Path.of(requireNotNull(System.getenv("FLAMEDB_PEBBLE_JOURNAL")))
    private val writeTimes = ConcurrentLinkedQueue<Double>()
    private val cfg = FlameDBConfig("127.0.0.1", requireNotNull(System.getenv("FLAMEDB_PEBBLE_PORT")).toInt(), "pebble-sdk-check", 5000,
        readConnections = 2, writeConnections = 1, onCommandTiming = {
            if (it.command == "WRITE_BATCH") {
                assertTrue(it.success)
                writeTimes.add((it.queueWaitNanos + it.requestNanos) / 1_000_000.0)
            }
        })
    private val events = 30000
    private val boards = listOf("", "team", "clan", "daily", "weekly")

    private fun metric(i: Int) = "$prefix.${i % 5}"
    private fun player(i: Int) = "player${(i / 5) % 1500}"
    private fun options(i: Int) = WriteOptions(player(i), mapOf("player" to player(i), "region" to "eu"),
        leaderboardEntities = boards.filter { it.isNotEmpty() }.associateWith { player(i) })

    @Test fun pendingRecoveryAndConcurrentReads() = runBlocking {
        assertFalse(Files.exists(journal), "Use a fresh test-owned journal")
        // Produce a definitely-unsent backlog through the real SDK. Keeping the
        // port reserved without accepting AUTH prevents any write from being sent.
        ServerSocket(0).use { offline ->
            val db = FlameDB.buffered(cfg.copy(port = offline.localPort, timeoutMs = 250), journal,
                BufferedWriteConfig(batchSize = 128, flushMs = 100, shutdownMs = 0, maxJournalBytes = 128L * 1024 * 1024))
            try {
                repeat(7500) { assertTrue(db.record(metric(it), 1.0, options(it))) }
            } finally { db.shutdown() }
            assertEquals(0L, db.statistics().confirmed)
            assertEquals(0L, db.statistics().uncertain)
        }
        val db = FlameDB.buffered(cfg, journal,
            BufferedWriteConfig(batchSize = 128, flushMs = 100, shutdownMs = 5000, maxJournalBytes = 128L * 1024 * 1024))
        val reads = AtomicInteger()
        try {
            val readers = List(2) { reader -> launch(Dispatchers.Default) {
                var next = reader
                while (isActive) {
                    val result = db.getSum(metric(next), options = GetOptions(where = mapOf("player" to player(next)))).aggregate!!
                    assertEquals(result.count.toDouble(), result.value)
                    assertTrue(result.count in 0..4)
                    reads.incrementAndGet()
                    next = (next + 2) % 7500
                    delay(2)
                }
            } }
            try {
                coroutineScope {
                    List(64) { producer -> launch(Dispatchers.Default) {
                        for (i in 7500 + producer until events step 64) assertTrue(db.record(metric(i), 1.0, options(i)))
                    } }.joinAll()
                }
                withTimeout(120000) { while (db.statistics().confirmed != events.toLong()) delay(10) }
            } finally { readers.forEach { it.cancel() }; readers.joinAll() }
            assertEquals((events - 7500).toLong(), db.statistics().admitted)
            assertEquals(0L, db.statistics().uncertain)
            assertEquals(0L, db.statistics().rejected)
            assertTrue(reads.get() > 0)
            verify(db)
        } finally { db.shutdown() }
        assertEquals(0L, db.statistics().retainedJournalBytes)
        val timings = writeTimes.sorted()
        assertTrue(timings.isNotEmpty())
        assertTrue(timings.last() < 5000)
        println("PEBBLE_BUFFERED_BATCH_LATENCY batches=${timings.size} p99_ms=${timings[((timings.size - 1) * 0.99).toInt()]} max_ms=${timings.last()}")
        println("PEBBLE_BUFFERED_RECOVERY_VERIFIED events=$events offline=7500 producers=64 readConnections=2 boards=5 reads=${reads.get()} uncertain=0 rejected=0")
    }

    @Test fun acknowledgedStateSurvivesKill() = runBlocking {
        // The harness SIGKILLs/restarts the same Pebble container before invoking
        // this method. No journal records or write commands are replayed here.
        val db = FlameDB.buffered(cfg, journal)
        try {
            verify(db)
            assertEquals(0L, db.statistics().admitted)
            assertEquals(0L, db.statistics().confirmed)
            assertEquals(0L, db.statistics().uncertain)
        } finally { db.shutdown() }
        println("PEBBLE_BUFFERED_KILL_READBACK_VERIFIED events=$events boards=5 replayed=0")
    }

    private suspend fun verify(db: BufferedFlameDB) {
        for (m in 0 until 5) {
            val sum = db.getSum(metric(m)).aggregate!!
            assertEquals(6000, sum.count)
            assertEquals(6000.0, sum.value)
            for (board in boards) {
                val scores = db.leaderboard(metric(m), LeaderboardOptions(limit = 2000, board = board.takeIf { it.isNotEmpty() }))
                assertEquals(1500, scores.size)
                assertEquals((0 until 1500).map { "player$it" }.toSet(), scores.map { it.entityId }.toSet())
                assertTrue(scores.all { it.score == 4.0 })
            }
        }
    }
}
