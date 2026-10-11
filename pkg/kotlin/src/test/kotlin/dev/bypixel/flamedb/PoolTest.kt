package dev.bypixel.flamedb

import java.io.BufferedWriter
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.*
import kotlin.concurrent.thread
import kotlin.test.*

class PoolTest {
    private class Server(private val respond: (String, BufferedWriter) -> Unit) : AutoCloseable {
        val listener = ServerSocket(0)
        val commands = CopyOnWriteArrayList<String>()
        val sockets = ConcurrentHashMap.newKeySet<Socket>()
        private val workers = Executors.newCachedThreadPool { r -> Thread(r).apply { isDaemon = true } }
        private val acceptor = thread(isDaemon = true) {
            while (!listener.isClosed) {
                val socket = runCatching { listener.accept() }.getOrNull() ?: break
                sockets.add(socket)
                workers.submit {
                    socket.use {
                        runCatching {
                            val reader = socket.getInputStream().bufferedReader()
                            val writer = socket.getOutputStream().bufferedWriter()
                            reply(writer, """{"auth":"required"}""")
                            check(reader.readLine() == "AUTH test")
                            reply(writer, """{"auth":"ok"}""")
                            while (true) {
                                var line = reader.readLine() ?: break
                                if (line == "WRITE_BATCH") {
                                    val frame = mutableListOf(line)
                                    do { line = reader.readLine() ?: break; frame.add(line) } while (line != "END")
                                    line = frame.joinToString("|")
                                }
                                commands.add(line)
                                respond(line, writer)
                            }
                        }
                    }
                    sockets.remove(socket)
                }
            }
        }
        fun config(timeout: Int = 2000, timing: ((CommandTiming) -> Unit)? = null) =
            FlameDBConfig("127.0.0.1", listener.localPort, "test", timeoutMs = timeout, onCommandTiming = timing)
        override fun close() {
            listener.close()
            sockets.forEach { runCatching { it.close() } }
            workers.shutdownNow()
            acceptor.join(1000)
        }
    }

    @Test fun readsHaveReservedCapacityWhileWritesAreBlocked() = runBlocking {
        val writeStarted = CountDownLatch(1)
        val releaseWrite = CountDownLatch(1)
        val timings = CopyOnWriteArrayList<CommandTiming>()
        Server { line, writer ->
            when {
                line.startsWith("WRITE_BATCH") -> { writeStarted.countDown(); releaseWrite.await(3, TimeUnit.SECONDS); reply(writer, """{"ok":true,"accepted":1,"failed":0}""") }
                line.startsWith("LEADERBOARD") -> reply(writer, """{"leaderboard":[{"entity_id":"${line.split(' ')[1]}","value":42}]}""")
                else -> reply(writer, "{}")
            }
        }.use { server ->
            FlameDB.connect(server.config(timing = { timings.add(it) })).use { db ->
                val write = async { db.writeBatch(listOf(WriteBatchItem("blocked", 1.0))) }
                try {
                    await(writeStarted)
                    val rows = withTimeout(1000) {
                        (0 until 40).map { i -> async { db.leaderboard("clan:$i") } }.awaitAll()
                    }
                    rows.forEachIndexed { i, entries -> assertEquals("clan:$i", entries.single().entityId) }
                    assertFalse(write.isCompleted, "write was not blocked during reads")
                    assertEquals(3, server.sockets.size, "default pool must use at most 2 read + 1 write sockets")
                } finally { releaseWrite.countDown() }
                assertEquals(1, write.await().accepted)
                assertEquals(41, timings.size)
                assertTrue(timings.all { it.success && it.queueWaitNanos >= 0 && it.requestNanos > 0 })
            }
        }
    }

    @Test fun totalDeadlineIncludesPoolWait() = runBlocking {
        val firstStarted = CountDownLatch(1)
        val releaseSecond = CountDownLatch(1)
        val timings = CopyOnWriteArrayList<CommandTiming>()
        Server { line, writer ->
            if (line.contains("first")) { firstStarted.countDown(); Thread.sleep(100); reply(writer, "{}") }
            else { releaseSecond.await(2, TimeUnit.SECONDS); reply(writer, "{}") }
        }.use { server ->
            FlameDB.connect(server.config(300) { timings.add(it) }).use { db ->
                val first = async { db.write("first", 1.0) }
                try {
                    await(firstStarted)
                    assertFailsWith<FlameDBException> { db.write("second", 1.0) }
                    first.await()
                    val timeout = timings.single { !it.success }
                    assertTrue(timeout.queueWaitNanos >= 50_000_000, "$timeout")
                    assertTrue(timeout.queueWaitNanos + timeout.requestNanos < 380_000_000, "$timeout")
                    assertTrue(timeout.requestNanos < 280_000_000, "$timeout")
                } finally { releaseSecond.countDown() }
            }
        }
    }

    @Test fun cancelledQueuedCommandIsNeverSentAndDoesNotCloseActiveSocket() = runBlocking {
        val firstStarted = CountDownLatch(1)
        val releaseFirst = CountDownLatch(1)
        val timings = CopyOnWriteArrayList<CommandTiming>()
        Server { line, writer ->
            if (line.contains("first")) { firstStarted.countDown(); releaseFirst.await(3, TimeUnit.SECONDS) }
            reply(writer, "{}")
        }.use { server ->
            FlameDB.connect(server.config(timing = { timings.add(it) })).use { db ->
                val first = async { db.write("first", 1.0) }
                try {
                    await(firstStarted)
                    assertFailsWith<TimeoutCancellationException> { withTimeout(50) { db.write("queued", 1.0) } }
                    assertFalse(first.isCompleted)
                    assertEquals(0L, timings.single().requestNanos)
                } finally { releaseFirst.countDown() }
                first.await()
                db.write("next", 1.0)
                assertEquals(listOf("first", "next"), server.commands.map { it.split(' ')[1] })
                assertEquals(1, server.sockets.size)
            }
        }
    }

    @Test fun activeCancellationClosesSocketAndNextCallReconnectsWithoutReplay() = runBlocking {
        val started = CountDownLatch(1)
        val release = CountDownLatch(1)
        Server { line, writer ->
            if (line.contains("blocked")) { started.countDown(); release.await(3, TimeUnit.SECONDS) }
            reply(writer, "{}")
        }.use { server ->
            FlameDB.connect(server.config()).use { db ->
                val blocked = async { db.write("blocked", 1.0) }
                await(started)
                blocked.cancelAndJoin()
                withTimeout(1000) { db.write("next", 1.0) }
                release.countDown()
                assertEquals(listOf("blocked", "next"), server.commands.map { it.split(' ')[1] })
            }
        }
    }

    @Test fun closeWakesActiveAndQueuedCalls() = runBlocking {
        val started = CountDownLatch(1)
        val release = CountDownLatch(1)
        Server { _, writer -> started.countDown(); release.await(3, TimeUnit.SECONDS); reply(writer, "{}") }.use { server ->
            val db = FlameDB.connect(server.config(5000))
            val first = async { runCatching { db.write("first", 1.0) } }
            await(started)
            val second = async(start = CoroutineStart.UNDISPATCHED) { runCatching { db.write("queued", 1.0) } }
            db.close()
            try {
                withTimeout(1000) { assertTrue(first.await().isFailure); assertTrue(second.await().isFailure) }
                assertTrue(server.commands.none { it.contains("queued") })
            } finally { release.countDown(); db.close() }
        }
    }

    @Test fun telemetryErrorsDoNotFailCommands() = runBlocking {
        Server { _, writer -> reply(writer, "{}") }.use { server ->
            FlameDB.connect(server.config(timing = { error("observer failed") })).use { it.write("m", 1.0) }
        }
    }

    @Test fun handshakeUsesOneDeadlineForChallengeAndAuthentication() = runBlocking {
        ServerSocket(0).use { listener ->
            val release = CountDownLatch(1)
            val worker = thread(isDaemon = true) {
                listener.accept().use { socket ->
                    runCatching {
                        Thread.sleep(100)
                        val reader = socket.getInputStream().bufferedReader()
                        val writer = socket.getOutputStream().bufferedWriter()
                        reply(writer, """{"auth":"required"}""")
                        check(reader.readLine() == "AUTH test")
                        release.await(2, TimeUnit.SECONDS)
                        reply(writer, """{"auth":"ok"}""")
                    }
                }
            }
            try {
                val started = System.nanoTime()
                assertFailsWith<FlameDBException> {
                    FlameDB.connect(FlameDBConfig("127.0.0.1", listener.localPort, "test", timeoutMs = 300)).close()
                }
                assertTrue(System.nanoTime() - started < 380_000_000, "auth got a fresh socket timeout after the greeting")
            } finally { release.countDown(); listener.close(); worker.join(1000) }
        }
    }

    companion object {
        private fun reply(writer: BufferedWriter, value: String) { writer.write(value); writer.write("\n"); writer.flush() }
        private suspend fun await(latch: CountDownLatch) {
            assertTrue(withContext(Dispatchers.IO) { latch.await(3, TimeUnit.SECONDS) }, "test server did not receive the command")
        }
    }
}
