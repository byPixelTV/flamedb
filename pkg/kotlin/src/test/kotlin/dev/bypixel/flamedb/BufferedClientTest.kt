package dev.bypixel.flamedb

import kotlin.test.*
import kotlinx.coroutines.*
import java.io.BufferedWriter
import java.net.ServerSocket
import java.net.Socket
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.concurrent.thread
import kotlin.system.measureTimeMillis

/** Tests the resolved SDK over real loopback TCP, with no Bukkit server dependency. */
class BufferedClientTest {
    private val logger = Logger.getLogger("ingestion-checks").apply { level = Level.OFF }
    private fun report(value: String) { println(value) }
    private class Fixture(port: Int = 0) : AutoCloseable {
        private val listener = ServerSocket(port)
        val sockets = ConcurrentHashMap.newKeySet<Socket>()
        private val executor = Executors.newCachedThreadPool { Thread(it, "metrics-fixture").apply { isDaemon = true } }
        val port = listener.localPort
        val lines = ConcurrentLinkedQueue<String>()
        val batchCalls = AtomicInteger()
        val singleCalls = AtomicInteger()
        val activeWrites = AtomicInteger()
        val maxWrites = AtomicInteger()
        val authAttempts = AtomicInteger()
        @Volatile var authFails = false
        @Volatile var authDelayMs = 0L
        @Volatile var delayMs = 0L
        @Volatile var blockWrites = false
        @Volatile var mode = "OK"
        private val acceptor = thread(isDaemon = true) {
            while (!listener.isClosed) {
                val socket = runCatching { listener.accept() }.getOrNull() ?: break
                sockets.add(socket)
                executor.submit {
                    try {
                        socket.use {
                            socket.tcpNoDelay = true
                            val reader = socket.getInputStream().bufferedReader(Charsets.UTF_8)
                            val writer = socket.getOutputStream().bufferedWriter(Charsets.UTF_8)
                            reply(writer, """{"auth":"required"}""")
                            check(reader.readLine() == "AUTH local-ingestion")
                            authAttempts.incrementAndGet()
                            if (authDelayMs > 0) Thread.sleep(authDelayMs)
                            if (authFails) { reply(writer, """{"error":"bad auth"}"""); return@submit }
                            reply(writer, """{"auth":"ok"}""")
                            while (true) {
                                val command = reader.readLine() ?: break
                                if (command.startsWith("GET ")) {
                                    reply(writer, """{"aggregate":{"type":"sum","value":42,"count":1}}""")
                                    continue
                                }
                                if (command.startsWith("LEADERBOARD ")) {
                                    reply(writer, """{"leaderboard":[{"entity_id":"read","value":7}]}""")
                                    continue
                                }
                                val batch = if (command == "WRITE_BATCH") {
                                    batchCalls.incrementAndGet()
                                    buildList { while (true) { val line = reader.readLine() ?: error("truncated request"); if (line == "END") break; add(line) } }
                                } else {
                                    check(command.startsWith("WRITE ")) { command }
                                    singleCalls.incrementAndGet()
                                    listOf(command)
                                }
                                lines.addAll(batch)
                                maxWrites.accumulateAndGet(activeWrites.incrementAndGet(), ::maxOf)
                                try {
                                    val replyMode = mode
                                    if (replyMode != "OK") mode = "OK"
                                    val responseDelay = delayMs
                                    if (replyMode == "TIMEOUT") delayMs = 0
                                    while (blockWrites && !socket.isClosed) Thread.sleep(5)
                                    if (responseDelay > 0) Thread.sleep(responseDelay)
                                    when (replyMode) {
                                        "DROP" -> return@submit // Command applied, acknowledgement lost.
                                        "TRUNCATED" -> { writer.write("{\"ok\":true"); writer.flush(); return@submit }
                                        "OVERSIZE" -> reply(writer, "x".repeat(4096))
                                        "BAD_ACK" -> reply(writer, """{"ok":true,"accepted":99999,"failed":0}""")
                                        "PARTIAL" -> reply(writer, """{"ok":false,"accepted":${batch.size-1},"failed":1,"errors":[{"index":${batch.size-1},"error":"quorum unavailable"}]}""")
                                        else -> if (command == "WRITE_BATCH") reply(writer, """{"ok":true,"accepted":${batch.size},"failed":0}""") else reply(writer, "{}")
                                    }
                                } finally { activeWrites.decrementAndGet() }
                            }
                        }
                    } catch (_: Exception) {
                        // Timeout/cancellation deliberately closes sockets.
                    } finally { sockets.remove(socket) }
                }
            }
        }
        fun config(timeout: Int = 1000, maxResponse: Int = 8 * 1024 * 1024) = FlameDBConfig(
            "127.0.0.1", port, "local-ingestion", timeout, maxResponseBytes = maxResponse,
        )
        override fun close() {
            blockWrites = false
            listener.close()
            sockets.forEach { runCatching { it.close() } }
            executor.shutdownNow()
            check(executor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS))
            acceptor.join(5000)
        }
        private fun reply(writer: BufferedWriter, value: String) { writer.write(value); writer.newLine(); writer.flush() }
    }

    private fun dir() = Files.createTempDirectory("eramc-flamedb-check-")
    private fun config(batch: Int = 128, shutdown: Long = 1000, confirmations: Int = 4) = BufferedWriteConfig(
        batchSize = batch, flushMs = 20, retryMs = 20, confirmationMs = 100, maxConfirmations = confirmations, shutdownMs = shutdown,
    )
    private suspend fun until(timeout: Long = 10_000, predicate: () -> Boolean) = withTimeout(timeout) {
        while (!predicate()) delay(5)
    }
    private fun count(path: Path, extension: String) = Files.newDirectoryStream(path, "*.$extension").use { it.count() }
    private fun clean(path: Path) { Files.walk(path).use { it.sorted(Comparator.reverseOrder()).forEach(Files::delete) } }

    internal suspend fun burst() {
        val path = dir()
        try {
            Fixture().use { server ->
                server.delayMs = 5
                server.blockWrites = true
                val db = BufferedFlameDB(server.config(60_000), path, config(), logger)
                try {
                    check(db.getSum("warmup").aggregate?.value == 42.0)
                    val n = 100_000
                    val admissionNanos = LongArray(n)
                    System.gc()
                    val runtime = Runtime.getRuntime()
                    val baselineHeap = runtime.totalMemory() - runtime.freeMemory()
                    val elapsed = measureTimeMillis {
                        coroutineScope {
                            List(64) { worker -> launch(Dispatchers.Default) {
                                for (id in worker until n step 64) {
                                    val start = System.nanoTime()
                                    check(db.record("burst:$id", if (id % 2 == 0) 0.25 else -1.0,
                                        WriteOptions(leaderboardEntity = "clan-$id", tags = mapOf("player" to "玩家 $id", "block" to "STONE"),
                                            timestampNs = 1_800_000_000_000_000_000L + id, quorum = id % 7 == 0,
                                            leaderboardEntities = mapOf("team" to "team-$id"))))
                                    admissionNanos[id] = System.nanoTime() - start
                                }
                            } }.joinAll()
                        }
                    }
                    until { server.activeWrites.get() == 1 }
                    check(withTimeout(1000) { db.leaderboard("reads").single().score } == 7.0)
                    check(db.statistics().admitted == n.toLong() && db.statistics().rejected == 0L)
                    check(server.batchCalls.get() == 1) { "burst created parallel SDK writes" }
                    System.gc()
                    val backlogHeapDelta = runtime.totalMemory() - runtime.freeMemory() - baselineHeap
                    check(backlogHeapDelta < 32 * 1024 * 1024) { "journal backlog retained too much heap: $backlogHeapDelta" }
                    val backlogFiles = count(path, "pending")
                    server.blockWrites = false
                    until(30_000) { db.statistics().confirmed == n.toLong() }
                    val expected = (0 until n).map { id ->
                        "WRITE burst:$id ${if (id % 2 == 0) "0.25" else "-1.0"} lb=\"clan-$id\" lb.team=\"team-$id\" player=\"玩家 $id\" block=\"STONE\" ts=${1_800_000_000_000_000_000L + id}${if (id % 7 == 0) " QUORUM" else ""}"
                    }.toSet()
                    check(server.lines.size == n && server.lines.toSet() == expected) { "event/value/tag/timestamp mismatch or duplicate" }
                    check(server.maxWrites.get() == 1 && server.batchCalls.get() <= n / 8)
                    admissionNanos.sort()
                    report("BURST events=$n producers=64 admissionMs=$elapsed p50Us=${admissionNanos[n/2]/1000} p99Us=${admissionNanos[n*99/100]/1000} RPCs=${server.batchCalls.get()} maxConcurrentWrites=${server.maxWrites.get()} preserved=$n backlogFiles=$backlogFiles backlogHeapDeltaBytes=$backlogHeapDelta")
                } finally { server.blockWrites = false; db.shutdown() }
                check(count(path, "pending") == 0 && count(path, "inflight") == 0 && count(path, "open") == 0)
            }
        } finally { clean(path) }
    }

    private suspend fun safeRecovery() {
        val port = ServerSocket(0).use { it.localPort }
        val path = dir()
        try {
            val cfg = FlameDBConfig("127.0.0.1", port, "local-ingestion", 150)
            val before = BufferedFlameDB(cfg, path, config(shutdown = 0), logger)
            repeat(500) { check(before.record("offline:$it", it.toDouble())) }
            delay(150)
            check(before.statistics().confirmed == 0L)
            before.shutdown()
            check(!before.record("closed", 1.0) && before.statistics().rejected == 1L)
            check(count(path, "pending") > 0)
            Fixture(port).use { server ->
                server.authFails = true
                val after = BufferedFlameDB(cfg, path, config(), logger)
                try {
                    until { server.authAttempts.get() >= 2 }
                    check(server.lines.isEmpty())
                    server.authFails = false
                    until { after.statistics().confirmed == 500L }
                    check(server.lines.size == 500 && server.lines.toSet().size == 500)
                    check(after.record("after-recovery", 7.0))
                    until { after.statistics().confirmed == 501L }
                } finally { after.shutdown() }
            }
            report("RECOVERY offline/restart/auth-failure=501 events confirmed exactly once; closed admission rejected")
        } finally { clean(path) }
    }

    private suspend fun ambiguous(mode: String) {
        val path = dir()
        try {
            Fixture().use { server ->
                server.mode = mode
                server.blockWrites = true
                if (mode == "TIMEOUT") server.delayMs = 600
                val db = BufferedFlameDB(server.config(150, 1024), path, config(batch = 8), logger)
                val callbacks = ConcurrentHashMap.newKeySet<Int>()
                var firstBatch = emptyList<String>()
                try {
                    assertEquals(42.0, db.getSum("warmup").aggregate?.value)
                    repeat(8) { id -> check(db.recordThen("$mode:$id", id.toDouble(), onConfirmed = { callbacks.add(id) })) }
                    until { server.lines.isNotEmpty() }
                    firstBatch = server.lines.toList()
                    server.blockWrites = false
                    until { db.statistics().uncertain > 0 && db.statistics().uncertain + db.statistics().confirmed == 8L }
                    check(server.lines.size == 8)
                    val failedIds = if (mode == "PARTIAL") firstBatch.takeLast(1) else firstBatch
                    check(failedIds.none { line -> line.substringAfter("WRITE $mode:").substringBefore(' ').toInt() in callbacks })
                    // A callback deadline does not roll back or resubmit its journaled event.
                    delay(180)
                    server.delayMs = 0
                    check(db.record("healthy:$mode", 1.0))
                    until { server.lines.size == 9 && db.statistics().confirmed >= 1 }
                    check(server.lines.toSet().size == 9)
                } finally { server.blockWrites = false; db.shutdown() }
                val extension = if (mode == "PARTIAL") "partial" else "uncertain"
                check(count(path, extension) == 1 && count(path, "inflight") == 0)
                val audit = db.auditFiles().single()
                val records = db.inspectAudit(audit)
                assertEquals(firstBatch.size, records.size)
                assertEquals(firstBatch.map { it.substringAfter("WRITE ").substringBefore(' ') }, records.map { it.item.metric })
                assertEquals(firstBatch.size, records.map { it.id }.toSet().size)
                assertTrue(records.all { it.item.options.timestampNs != null })
                val after = BufferedFlameDB(server.config(), path, config(), logger)
                try {
                    check(after.record("restart:$mode", 2.0))
                    until { after.statistics().confirmed == 1L }
                    check(server.lines.size == 10 && server.lines.toSet().size == 10)
                } finally { after.shutdown() }
                report("NO_REPLAY mode=$mode original=8 healthy=2 auditFiles=1")
            }
        } finally { clean(path) }
    }

    private suspend fun shutdownAndJournalRecovery() {
        val path = dir()
        try {
            Fixture().use { server ->
                server.blockWrites = true
                val db = BufferedFlameDB(server.config(5000), path, config(batch = 4, shutdown = 30), logger)
                repeat(20) { check(db.record("shutdown:$it", it.toDouble())) }
                until { server.lines.isNotEmpty() }
                val inFlight = server.lines.size
                db.shutdown()
                check(count(path, "uncertain") == 1 && count(path, "pending") > 0)
                server.blockWrites = false
                val after = BufferedFlameDB(server.config(), path, config(batch = 4), logger)
                try {
                    until { after.statistics().confirmed == (20 - inFlight).toLong() }
                    check(server.lines.size == 20 && server.lines.toSet().size == 20)
                } finally { after.shutdown() }
            }
            // Known unsent truncated .open: valid prefix recovers, original retained.
            val journal = WriteJournal(path, 128)
            val entry = JournalWrite("recovery-id", "journal:utf8", 1.5, "clan", mapOf("player" to "玩家"), 1234, false, emptyMap())
            journal.append(entry)
            journal.stopAdmission()
            val pending = journal.oldestPending()!!
            val open = journal.move(pending, "open")
            Files.newOutputStream(open, java.nio.file.StandardOpenOption.APPEND).use { it.write(byteArrayOf(0, 0, 1)) }
            journal.close()
            WriteJournal(path, 128).use { recovered ->
                check(recovered.read(recovered.oldestPending()!!).single() == entry)
                check(count(path, "recovered") == 1)
                val claimed = recovered.move(recovered.oldestPending()!!, "inflight")
                check(Files.exists(claimed))
            }
            WriteJournal(path, 128).use { recovered ->
                check(recovered.oldestPending() == null && count(path, "uncertain") == 2)
            }
            report("SHUTDOWN unsent records recovered; inflight quarantined; truncated UTF-8 journal prefix recovered")
        } finally { clean(path) }
    }

    private suspend fun callbackAndShutdownBound() {
        val path = dir()
        try {
            Fixture().use { server ->
                server.delayMs = 250
                val db = BufferedFlameDB(server.config(1000), path, config(confirmations = 2), logger)
                val callbacks = AtomicInteger()
                try {
                    repeat(500) { check(db.recordThen("confirmation:$it", 1.0, onConfirmed = { callbacks.incrementAndGet() })) }
                    until { db.statistics().confirmed == 500L }
                    check(callbacks.get() == 0 && server.lines.size == 500)
                    server.delayMs = 0
                    check(db.recordThen("confirmation:reuse", 1.0, onConfirmed = { callbacks.incrementAndGet() }))
                    until { callbacks.get() == 1 }
                } finally { db.shutdown() }
            }
            report("CONFIRMATION bounded=2 deadline-expired=500 events retained; capacity reused")
        } finally { clean(path) }
    }

    private suspend fun liveServerRestart() {
        val path = dir()
        var first: Fixture? = Fixture()
        val port = first!!.port
        val db = BufferedFlameDB(first.config(300), path, config(batch = 8), logger)
        try {
            repeat(8) { check(db.record("before-restart:$it", 1.0)) }
            until { db.statistics().confirmed == 8L }
            first.close()
            first = null
            Fixture(port).use { second ->
                repeat(8) { check(db.record("during-restart:$it", 1.0)) }
                // A stale socket can fail before or after sending. Both results are safe:
                // explicit NOT_SENT retries; UNKNOWN is quarantined, never replayed.
                until { db.statistics().confirmed + db.statistics().uncertain == 16L }
                repeat(8) { check(db.record("after-restart:$it", 1.0)) }
                until { db.statistics().confirmed + db.statistics().uncertain == 24L }
                check(db.statistics().confirmed >= 16L)
                check(second.lines.size == second.lines.toSet().size)
                // Retire any read lease that was authenticated before the server restart.
                repeat(2) { try { db.getSum("retire-stale-read") } catch (_: FlameDBException) { } }
                check(withTimeout(1000) { db.getSum("read-after-restart").aggregate?.value } == 42.0)
                report("LIVE_RESTART confirmed=${db.statistics().confirmed} uncertain=${db.statistics().uncertain} duplicateWrites=0 readPoolRecovered=true")
            }
        } finally { first?.close(); db.shutdown(); clean(path) }
    }

    private suspend fun negativeControl() {
        Fixture().use { server ->
            server.delayMs = 10
            val cfg = server.config(200).copy(maxQueuedWrites = 16)
            val db = FlameDB.connect(cfg)
            try {
                val errors = coroutineScope { (0 until 1000).map { id -> async {
                    try { db.write("legacy:$id", 1.0); null } catch (e: FlameDBException) { e.kind }
                } }.awaitAll().filterNotNull() }
                check(errors.size >= 900 && errors.any { it == FlameDBErrorKind.OVERLOADED || it == FlameDBErrorKind.POOL_WAIT_TIMEOUT })
                report("NEGATIVE_CONTROL legacy per-event coroutines: errors=${errors.size}/1000 detected overload as expected")
            } finally { db.close() }
        }
    }

    @Test fun unavailableConnectionAuthAndRestart() = runBlocking { safeRecovery() }
    @Test fun lostAckIsNeverReplayed() = runBlocking { ambiguous("DROP") }
    @Test fun truncatedAckIsNeverReplayed() = runBlocking { ambiguous("TRUNCATED") }
    @Test fun oversizedAckIsNeverReplayed() = runBlocking { ambiguous("OVERSIZE") }
    @Test fun timeoutIsNeverReplayed() = runBlocking { ambiguous("TIMEOUT") }
    @Test fun partialAckIsNeverReplayed() = runBlocking { ambiguous("PARTIAL") }
    @Test fun invalidCountersAreNeverReplayed() = runBlocking { ambiguous("BAD_ACK") }
    @Test fun shutdownAndTruncatedJournalRecoverSafely() = runBlocking { shutdownAndJournalRecovery() }
    @Test fun confirmationCapacityReleasesAfterDeadline() = runBlocking { callbackAndShutdownBound() }
    @Test fun runningClientRecoversAfterServerRestart() = runBlocking { liveServerRestart() }
    @Test fun oldPerEventPatternIsDetectedAsOverload() = runBlocking { negativeControl() }

    @Test fun minimalFactoryPreservesSnapshotsAndDirectWriteSemantics() = runBlocking {
        val path = dir()
        try {
            Fixture().use { server ->
                val db = FlameDB.buffered(server.config(), path)
                try {
                    val tags = mutableMapOf("player" to "玩家 original")
                    val boards = mutableMapOf("team" to "original")
                    assertTrue(db.record("snapshot", 0.25, WriteOptions("clan", tags, leaderboardEntities = boards)))
                    tags["player"] = "mutated"
                    boards["team"] = "mutated"
                    repeat(2000) { assertTrue(db.record("unique-time", 1.0, WriteOptions(tags = mapOf("id" to it.toString())))) }
                    until { db.statistics().confirmed == 2001L }
                    assertTrue(server.lines.first().contains("player=\"玩家 original\""))
                    assertTrue(server.lines.first().contains("lb.team=\"original\""))
                    val timestamps = server.lines.filter { it.startsWith("WRITE unique-time ") }.map { it.substringAfter(" ts=").toLong() }
                    assertEquals(2000, timestamps.toSet().size)
                    assertFalse(db.record("invalid metric", 1.0))
                    assertEquals(ExecutionStatus.NOT_SENT, db.lastFailure?.executionStatus)
                    assertTrue(db.lastFailure?.cause is IllegalArgumentException)
                    // The existing direct API still sends one WRITE and returns its server ACK.
                    db.directClient().write("direct", 7.0)
                    assertEquals(1, server.singleCalls.get())
                    assertEquals(42.0, db.getSum("reads").aggregate?.value)
                } finally { db.close(); db.close() }
                assertFalse(db.record("closed", 1.0))
                assertEquals(FlameDBErrorKind.CLIENT_CLOSED, db.lastFailure?.kind)
                assertEquals(ExecutionStatus.NOT_SENT, db.lastFailure?.executionStatus)
                val reopened = FlameDB.buffered(server.config(), path)
                reopened.shutdown()
                reopened.shutdown()
            }
        } finally { clean(path) }
    }

    @Test fun diskBudgetRejectsEarlyAndReleasesAfterAcknowledgement() = runBlocking {
        val path = dir()
        try {
            Fixture().use { server ->
                server.blockWrites = true
                val db = FlameDB.buffered(server.config(5000), path,
                    config(batch = 4).copy(maxJournalBytes = 4096))
                try {
                    var accepted = 0
                    while (db.record("capacity:$accepted", 1.0)) {
                        accepted++
                        check(accepted < 50) { "journal budget not enforced" }
                    }
                    assertTrue(accepted > 4)
                    assertEquals(FlameDBErrorKind.OVERLOADED, db.lastFailure?.kind)
                    assertEquals(ExecutionStatus.NOT_SENT, db.lastFailure?.executionStatus)
                    assertTrue(db.statistics().retainedJournalBytes <= 4096)
                    assertEquals(42.0, withTimeout(1000) { db.getSum("reads").aggregate?.value })
                    server.blockWrites = false
                    until { db.statistics().confirmed == accepted.toLong() }
                    until { db.statistics().retainedJournalBytes == 0L }
                    assertTrue(db.record("capacity:reused", 5.0))
                    until { db.statistics().confirmed == accepted + 1L }
                    assertEquals(accepted + 1, server.lines.size)
                } finally { server.blockWrites = false; db.shutdown() }
            }
        } finally { clean(path) }
    }

    @Test fun auditFilesConsumeAdmissionBudgetAfterRestartAndNeverReplay() = runBlocking {
        val path = dir()
        try {
            Fixture().use { server ->
                server.mode = "DROP"
                server.blockWrites = true
                val db = FlameDB.buffered(server.config(), path, config(batch = 4))
                repeat(4) { assertTrue(db.record("audit:$it", 1.0)) }
                until { server.lines.isNotEmpty() }
                server.blockWrites = false
                until { db.statistics().uncertain > 0 && db.statistics().uncertain + db.statistics().confirmed == 4L }
                db.shutdown()
                val bytes = db.statistics().retainedJournalBytes
                assertTrue(bytes > 0)
                val after = FlameDB.buffered(server.config(), path, config().copy(maxJournalBytes = bytes))
                try {
                    assertFalse(after.record("full-audit", 1.0))
                    assertEquals(FlameDBErrorKind.OVERLOADED, after.lastFailure?.kind)
                    delay(100)
                    assertEquals(4, server.lines.size)
                    assertEquals(1, count(path, "uncertain"))
                } finally { after.shutdown() }
            }
        } finally { clean(path) }
    }

    @Test fun exclusiveDirectoryAndCancelledShutdownReleaseOwnership() = runBlocking {
        val path = dir()
        try {
            Fixture().use { server ->
                server.blockWrites = true
                val db = FlameDB.buffered(server.config(), path, config(batch = 4, shutdown = 30))
                assertFailsWith<java.nio.channels.OverlappingFileLockException> { FlameDB.buffered(server.config(), path) }
                repeat(20) { assertTrue(db.record("cancel-shutdown:$it", 1.0)) }
                until { server.lines.isNotEmpty() }
                val inFlight = server.lines.size
                assertFailsWith<TimeoutCancellationException> { withTimeout(1) { db.shutdown() } }
                assertFalse(db.record("closed", 1.0))
                assertEquals(1, count(path, "uncertain"))
                server.blockWrites = false
                val next = FlameDB.buffered(server.config(), path, config(batch = 4))
                try { until { next.statistics().confirmed == (20 - inFlight).toLong() } } finally { next.shutdown() }
                assertEquals(20, server.lines.size)
                assertEquals(20, server.lines.toSet().size)
            }
        } finally { clean(path) }
    }

    @Test fun partialJournalFlushDoesNotWaitForBlockedSocket() = runBlocking {
        val path = dir()
        try {
            Fixture().use { server ->
                server.blockWrites = true
                val db = FlameDB.buffered(server.config(5000), path, config(batch = 4))
                try {
                    repeat(5) { assertTrue(db.record("flush:$it", 1.0)) }
                    until { server.lines.isNotEmpty() }
                    assertTrue(server.lines.size in 1..4)
                    until(1000) { count(path, "open") == 0 && count(path, "pending") >= 1 }
                    assertEquals(1, server.activeWrites.get())
                    assertEquals(0L, db.statistics().confirmed)
                    server.blockWrites = false
                    until { db.statistics().confirmed == 5L }
                    assertEquals(5, server.lines.size)
                } finally { server.blockWrites = false; db.shutdown() }
            }
        } finally { clean(path) }
    }

    @Test fun invalidUtf8InAValidCrcJournalFrameIsQuarantined(): Unit = runBlocking {
        val path = dir()
        try {
            WriteJournal(path, 128).use { journal ->
                journal.append(JournalWrite("id", "utf8", 1.0, null, mapOf("player" to "玩家"), 1234, false, emptyMap()))
                journal.stopAdmission()
                val open = journal.move(journal.oldestPending()!!, "open")
                val bytes = Files.readAllBytes(open)
                val encoded = "玩家".toByteArray(Charsets.UTF_8)
                val start = (8 until bytes.size - encoded.size).first { offset ->
                    encoded.indices.all { bytes[offset + it] == encoded[it] }
                }
                bytes[start] = 0xff.toByte()
                val crc = java.util.zip.CRC32().apply { update(bytes, 8, bytes.size - 8) }.value.toInt()
                java.nio.ByteBuffer.wrap(bytes).putInt(4, crc)
                Files.write(open, bytes)
            }
            WriteJournal(path, 128).use { recovered ->
                assertNull(recovered.oldestPending())
                assertEquals(1, count(path, "damaged"))
                assertFailsWith<java.nio.charset.CharacterCodingException> { recovered.inspectAudit(recovered.auditFiles(1).single()) }
            }
        } finally { clean(path) }
    }

    @Test fun failedInitialAuthDoesNotCreateUnboundedReaderMutexWait() = runBlocking {
        val path = dir()
        try {
            Fixture().use { server ->
                server.authFails = true
                server.authDelayMs = 250
                val db = FlameDB.buffered(server.config(150), path, config(shutdown = 0))
                try {
                    val started = System.nanoTime()
                    val results = withTimeout(3000) { coroutineScope { List(100) { async {
                        try { db.getSum("read"); null } catch (error: FlameDBException) { error }
                    } }.awaitAll() } }
                    assertTrue(results.all { it != null })
                    assertTrue((System.nanoTime() - started) / 1_000_000 < 3000)
                    server.authFails = false
                    server.authDelayMs = 0
                    assertEquals(42.0, db.getSum("recovered").aggregate?.value)
                } finally { db.shutdown() }
            }
        } finally { clean(path) }
    }

    @Test fun invalidConfigurationFailsBeforeClaimingDirectory(): Unit {
        val path = dir()
        try {
            val cfg = FlameDBConfig("localhost", apiKey = "test")
            assertFailsWith<IllegalArgumentException> { FlameDB.buffered(cfg.copy(apiKey = "bad\nkey"), path) }
            assertFailsWith<IllegalArgumentException> { FlameDB.buffered(cfg.copy(port = 65536), path) }
            assertFailsWith<IllegalArgumentException> { FlameDB.buffered(cfg.copy(readConnections = 0), path) }
            assertFailsWith<IllegalArgumentException> { FlameDB.buffered(cfg, path, BufferedWriteConfig(maxJournalBytes = 0)) }
            assertFalse(Files.exists(path.resolve("writer.lock")))
        } finally { clean(path) }
    }

    @Test fun coldReadQueueLimitAndCancellationReleaseCapacity() = runBlocking {
        val path = dir()
        try {
            Fixture().use { server ->
                server.authDelayMs = 250
                val db = FlameDB.buffered(server.config(3000).copy(maxQueuedReads = 1), path)
                try {
                    val active = async(start = CoroutineStart.UNDISPATCHED) { db.getSum("active") }
                    val queued = async(start = CoroutineStart.UNDISPATCHED) { db.getSum("queued") }
                    val full = assertFailsWith<FlameDBException> { db.getSum("full") }
                    assertEquals(FlameDBErrorKind.OVERLOADED, full.kind)
                    assertEquals(ExecutionStatus.NOT_SENT, full.executionStatus)
                    queued.cancelAndJoin()
                    val replacement = async(start = CoroutineStart.UNDISPATCHED) { db.getSum("replacement") }
                    assertEquals(42.0, active.await().aggregate?.value)
                    assertEquals(42.0, replacement.await().aggregate?.value)
                } finally { db.shutdown() }
            }
        } finally { clean(path) }
    }

    @Test fun initialPublicReadCannotLeaveOpenClientAfterShutdown() = runBlocking {
        val path = dir()
        try {
            Fixture().use { server ->
                server.authDelayMs = 250
                val db = FlameDB.buffered(server.config(1000), path, config(shutdown = 0))
                val read = async(start = CoroutineStart.UNDISPATCHED) {
                    try { db.getSum("closing"); null } catch (error: FlameDBException) { error }
                }
                until { server.authAttempts.get() > 0 }
                db.shutdown()
                assertEquals(FlameDBErrorKind.CLIENT_CLOSED, read.await()?.kind)
                until { server.sockets.isEmpty() }
                assertFalse(db.record("closed", 1.0))
                assertEquals(0, server.batchCalls.get())
            }
        } finally { clean(path) }
    }
}

class BufferedClientStressTest {
    @Test fun sdkManagedBurstPreservesAllEventsWithBoundedHeapAndOneWriter() = runBlocking {
        BufferedClientTest().burst()
        println("BUFFERED_SDK_STRESS_VERIFIED events=100000 producers=64")
    }
}
