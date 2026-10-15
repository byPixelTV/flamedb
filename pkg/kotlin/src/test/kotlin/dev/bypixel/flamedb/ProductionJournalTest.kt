package dev.bypixel.flamedb

import java.io.DataInputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.ByteBuffer
import java.nio.charset.CodingErrorAction
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.CRC32
import kotlinx.coroutines.*
import kotlinx.serialization.json.Json
import kotlin.test.*

/** No source journal is opened by BufferedFlameDB. Only a test-owned copy is sent
 * to a fresh local database; this never reconciles production UNKNOWN outcomes. */
class ProductionJournalTest {
    private val input = Path.of(requireNotNull(System.getenv("FLAMEDB_JOURNAL_INPUT")))
    private val journal = Path.of(requireNotNull(System.getenv("FLAMEDB_JOURNAL_COPY")))
    private val measurements = ConcurrentLinkedQueue<CommandTiming>()
    private val cfg = FlameDBConfig("127.0.0.1", requireNotNull(System.getenv("FLAMEDB_JOURNAL_PORT")).toInt(),
        "production-journal-check", timeoutMs = 5000, readConnections = 2, writeConnections = 1,
        onCommandTiming = { measurements.add(it) })
    private val files = Files.list(input).use { paths -> paths.filter { it.toString().endsWith(".uncertain") }.sorted().toList() }
    private val records = files.flatMap { file -> readFrames(file) }

    private fun readFrames(file: Path): List<JournalWrite> {
        val result = ArrayList<JournalWrite>()
        DataInputStream(Files.newInputStream(file).buffered()).use { reader ->
            while (reader.available() > 0) {
                val size = reader.readInt()
                val crc = reader.readInt().toLong() and 0xffffffffL
                require(size in 1..65536)
                val bytes = ByteArray(size); reader.readFully(bytes)
                check(CRC32().apply { update(bytes) }.value == crc)
                val text = Charsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString()
                result += Json.decodeFromString<JournalWrite>(text)
            }
        }
        require(result.size in 1..512)
        return result
    }

    @Test fun replayCopiedUnknownRecords() = runBlocking {
        require(records.isNotEmpty())
        assertFalse(Files.exists(journal), "Use a fresh test-owned journal")
        Files.createDirectories(journal)
        for (file in files) {
            val copy = journal.resolve(file.fileName.toString().removeSuffix(".uncertain") + ".pending")
            Files.copy(file, copy)
            assertTrue(Files.readAllBytes(file).contentEquals(Files.readAllBytes(copy)), "Copy changed a frame")
        }
        val targets = records.distinctBy { it.metric to it.tags["player"] }.take(256)
        val readCount = AtomicInteger()
        val started = System.nanoTime()
        val db = FlameDB.buffered(cfg, journal, BufferedWriteConfig(maxJournalBytes = 64L * 1024 * 1024))
        var elapsed = 0.0
        try {
            val readers = List(2) { reader -> launch(Dispatchers.Default) {
                var i = reader
                while (isActive) {
                    val target = targets[i % targets.size]
                    val filters = target.tags["player"]?.let { mapOf("player" to it) } ?: emptyMap()
                    val aggregate = db.getSum(target.metric, options = GetOptions(where = filters)).aggregate!!
                    assertTrue(aggregate.count >= 0 && aggregate.value.isFinite())
                    readCount.incrementAndGet(); i += 2; delay(2)
                }
            } }
            try {
                withTimeout(300000) {
                    while (db.statistics().confirmed != records.size.toLong()) {
                        assertEquals(0L, db.statistics().uncertain, "Copied replay became ambiguous; retained for inspection")
                        assertEquals(0L, db.statistics().rejected)
                        delay(10)
                    }
                }
                elapsed = (System.nanoTime() - started) / 1e9
            } finally { readers.forEach { it.cancel() }; readers.joinAll() }
            val stats = db.statistics()
            assertEquals(0L, stats.admitted)
            assertEquals(records.size.toLong(), stats.confirmed)
            assertEquals(0L, stats.uncertain)
            assertEquals(0L, stats.rejected)
            assertTrue(readCount.get() > 0)
            verify(db.directClient())
        } finally { db.shutdown() }
        assertEquals(0L, db.statistics().retainedJournalBytes)
        val writes = measurements.filter { it.command == "WRITE_BATCH" }
        assertEquals(files.size, writes.size)
        assertTrue(writes.all { it.success && it.failure == null })
        val times = writes.map { (it.queueWaitNanos + it.requestNanos) / 1e6 }.sorted()
        val p99 = times[((times.size - 1) * 0.99).toInt()]
        println("PRODUCTION_JOURNAL_LATENCY batches=${files.size} events=${records.size} elapsed_s=$elapsed events_per_s=${records.size/elapsed} p50_ms=${times[times.size/2]} p99_ms=$p99 max_ms=${times.last()} reads=${readCount.get()} timeout_ms=5000 uncertain=0 rejected=0")
        println("PRODUCTION_JOURNAL_COPY_REPLAY_VERIFIED events=${records.size} batches=${files.size} metrics=${records.map { it.metric }.toSet().size}")
    }

    @Test fun acknowledgedCopySurvivesKill() = runBlocking {
        FlameDB.connect(cfg).use { client -> verify(client) }
        println("PRODUCTION_JOURNAL_KILL_READBACK_VERIFIED events=${records.size} replayed=0")
    }

    private suspend fun verify(client: FlameDB) {
        val raw = LinkedHashMap<Pair<String,Long>,JournalWrite>()
        val boards = LinkedHashMap<Pair<String,String>,MutableMap<String,Double>>()
        for (entry in records) {
            raw[entry.metric to entry.timestampNs] = entry
            val destinations = entry.leaderboardEntities.toMutableMap()
            entry.leaderboardEntity?.let { destinations[""] = it }
            for ((board,entity) in destinations) {
                val scores = boards.getOrPut(entry.metric to board) { LinkedHashMap() }
                val next = (scores[entity] ?: 0.0) + entry.value
                check(next.isFinite()); scores[entity] = if (next == 0.0) 0.0 else next
            }
        }
        for ((metric,expected) in raw.values.groupBy { it.metric }) {
            val sorted = expected.sortedBy { it.timestampNs }
            val got = client.get(metric, options = GetOptions(limit = 1000000, order = SortOrder.ASC)).events
            assertEquals(sorted.size, got.size, "Raw event count mismatch")
            for ((entry,event) in sorted.zip(got)) {
                assertEquals(entry.timestampNs,event.timestamp)
                assertEquals(entry.value,event.value)
                assertEquals(entry.tags,event.tags)
                assertEquals(entry.metric,event.metric)
            }
            val sum = client.getSum(metric).aggregate!!
            assertEquals(sorted.size,sum.count)
            assertEquals(sorted.fold(0.0) { total,entry -> total+entry.value },sum.value)
        }
        for ((key,expected) in boards) {
            val got = client.leaderboard(key.first,LeaderboardOptions(limit=1000000,board=key.second.takeIf { it.isNotEmpty() }))
            assertEquals(expected.size,got.size,"Leaderboard entity count mismatch")
            assertEquals(expected,got.associate { it.entityId to it.score },"Ordered board increments mismatch")
        }
        println("PRODUCTION_JOURNAL_PAYLOADS_VERIFIED records=${records.size} raw_events=${raw.size} boards=${boards.size} entity_scores=${boards.values.sumOf { it.size }}")
    }
}
