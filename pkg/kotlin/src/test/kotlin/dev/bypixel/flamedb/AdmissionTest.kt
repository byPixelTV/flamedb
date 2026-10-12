package dev.bypixel.flamedb

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.*
import kotlinx.serialization.json.Json
import kotlin.test.*

class AdmissionTest {
    @Test fun closeAlsoWakesLeasesWaitingForAnIoDispatcherThread() = runBlocking {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val dispatcher = Dispatchers.IO.limitedParallelism(1)
        TcpFixture { socket, _, _ -> socket.reply("{}") }.use { server ->
            val pool = CommandPool(server.config(5000), Json, dispatcher)
            pool.connect()
            val blocker = async(dispatcher) { entered.countDown(); release.await(5, TimeUnit.SECONDS) }
            entered.awaitReceived()
            try {
                val call = async(start = CoroutineStart.UNDISPATCHED) { runCatching { pool.command(listOf("WRITE neverSent 1")) } }
                assertFalse(call.isCompleted)
                pool.close()
                // The worker still cannot run. Shutdown must not wait for the
                // dispatcher or the five-second command deadline to wake callers.
                val failure = withTimeout(1000) { assertIs<FlameDBException>(call.await().exceptionOrNull()) }
                assertEquals(FlameDBErrorKind.CLIENT_CLOSED, failure.kind)
                assertEquals(ExecutionStatus.NOT_SENT, failure.executionStatus)
                assertTrue(server.commands.isEmpty())
            } finally { release.countDown(); blocker.await(); pool.close() }
        }
    }

    @Test fun boundedReadQueueDoesNotConsumeWriteCapacity() = runBlocking {
        val started = CountDownLatch(2)
        val release = CountDownLatch(1)
        TcpFixture { socket, line, _ ->
            if (line.startsWith("LEADERBOARD")) {
                if (line.contains("held")) { started.countDown(); check(release.await(5, TimeUnit.SECONDS)) }
                socket.reply("""{"leaderboard":[{"entity_id":"${line.split(' ')[1]}","value":1}]}""")
            } else socket.reply("{}")
        }.use { server ->
            FlameDB.connect(server.config().copy(maxQueuedReads = 3)).use { db ->
                val active = (0 until 2).map { async { db.leaderboard("held:$it") } }
                try {
                    started.awaitReceived()
                    val calls = (0 until 10).map { i -> async(start = CoroutineStart.UNDISPATCHED) {
                        runCatching { db.leaderboard("queued:$i") }
                    } }
                    assertEquals(3, calls.count { !it.isCompleted })
                    calls.filter { it.isCompleted }.forEach {
                        val error = assertIs<FlameDBException>(it.await().exceptionOrNull())
                        assertEquals(FlameDBErrorKind.OVERLOADED, error.kind)
                        assertFalse(error.mutating)
                        assertEquals(ExecutionStatus.NOT_SENT, error.executionStatus)
                    }
                    withTimeout(1000) { db.write("fastWrite", 1.0) }
                    release.countDown()
                    active.awaitAll()
                    val accepted = calls.awaitAll().filter { it.isSuccess }.map { it.getOrThrow().single().entityId }
                    assertEquals(listOf("queued:0", "queued:1", "queued:2"), accepted)
                } finally { release.countDown() }
            }
        }
    }

    @Test fun boundedWriteQueueRejectsOverflowAndKeepsReadsAvailable() = runBlocking {
        val started = CountDownLatch(1)
        val release = CountDownLatch(1)
        TcpFixture { socket, line, _ ->
            if (line.contains("held")) { started.countDown(); check(release.await(10, TimeUnit.SECONDS)) }
            socket.reply(if (line.startsWith("LEADERBOARD")) """{"leaderboard":[]}""" else "{}")
        }.use { server ->
            FlameDB.connect(server.config(10_000).copy(maxQueuedWrites = 8, maxQueuedReads = 1)).use { db ->
                val active = async { db.write("held", 1.0) }
                try {
                    started.awaitReceived()
                    val calls = (0 until 1000).map { i -> async(start = CoroutineStart.UNDISPATCHED) { runCatching { db.write("queued:$i", 1.0) } } }
                    assertEquals(8, calls.count { !it.isCompleted })
                    val rejected = calls.filter { it.isCompleted }.map { it.await().exceptionOrNull() }
                    assertEquals(992, rejected.size)
                    rejected.forEach { error ->
                        val failure = assertIs<FlameDBException>(error)
                        assertEquals(FlameDBErrorKind.OVERLOADED, failure.kind)
                        assertEquals(ExecutionStatus.NOT_SENT, failure.executionStatus)
                        assertTrue(failure.mutating)
                    }
                    withTimeout(1000) { assertTrue(db.leaderboard("fast").isEmpty()) }
                    assertFalse(active.isCompleted)
                    assertEquals(1, server.commands.count { it.startsWith("WRITE") })
                    release.countDown()
                    active.await()
                    assertEquals(8, calls.awaitAll().count { it.isSuccess })
                    db.write("after", 1.0)
                    assertEquals(10, server.commands.count { it.startsWith("WRITE") })
                } finally { release.countDown() }
            }
        }
    }

    @Test fun queuedCancellationReturnsCapacityAndPreservesNativeTimeoutType() = runBlocking {
        val started = CountDownLatch(1)
        val release = CountDownLatch(1)
        TcpFixture { socket, line, _ ->
            if (line.contains("held")) { started.countDown(); check(release.await(5, TimeUnit.SECONDS)) }
            socket.reply("{}")
        }.use { server ->
            FlameDB.connect(server.config().copy(maxQueuedWrites = 1)).use { db ->
                val active = async { db.write("held", 1.0) }
                try {
                    started.awaitReceived()
                    repeat(20) {
                        val error = assertFailsWith<TimeoutCancellationException> { withTimeout(10) { db.write("cancelled", 1.0) } }
                        val failure = assertNotNull(error.flameDBFailure())
                        assertEquals(FlameDBErrorKind.REQUEST_CANCELLED, failure.kind)
                        assertEquals(FlameDBPhase.POOL_WAIT, failure.phase)
                        assertEquals(ExecutionStatus.NOT_SENT, failure.executionStatus)
                    }
                    val next = async(start = CoroutineStart.UNDISPATCHED) { db.write("next", 1.0) }
                    assertFalse(next.isCompleted, "queue capacity leaked after cancellation")
                    release.countDown()
                    active.await(); next.await()
                    assertEquals(listOf("held", "next"), server.commands.map { it.split(' ')[1] })
                } finally { release.countDown() }
            }
        }
    }

    @Test fun poolWaitDeadlineReturnsWaitingCapacityWhileWorkerIsStillCleaningUp() = runBlocking {
        val decoding = CountDownLatch(1)
        val release = CountDownLatch(1)
        TcpFixture { socket, _, _ -> socket.reply("{}") }.use { server ->
            // The oracle blocks decoding and tests request/pool deadlines. Give
            // the unrelated initial TCP/auth setup room under a loaded JVM.
            val pool = CommandPool(server.config(500).copy(maxQueuedWrites = 1), Json)
            pool.connect()
            try {
                val active = async { runCatching { pool.command(listOf("WRITE held 1")) {
                    decoding.countDown(); check(release.await(5, TimeUnit.SECONDS)); it
                } } }
                decoding.awaitReceived()
                // Cancellation cannot interrupt arbitrary CPU/decoder code; its
                // lease must remain held until that worker actually exits.
                val queued = async(start = CoroutineStart.UNDISPATCHED) { runCatching { pool.command(listOf("WRITE neverSent 1")) } }
                val activeError = assertIs<FlameDBException>(active.await().exceptionOrNull())
                assertEquals(FlameDBErrorKind.REQUEST_TIMEOUT, activeError.kind)
                val queuedError = assertIs<FlameDBException>(queued.await().exceptionOrNull())
                assertEquals(FlameDBErrorKind.POOL_WAIT_TIMEOUT, queuedError.kind)
                assertEquals(ExecutionStatus.NOT_SENT, queuedError.executionStatus)
                val replacement = async(start = CoroutineStart.UNDISPATCHED) { runCatching { pool.command(listOf("WRITE replacement 1")) } }
                assertFalse(replacement.isCompleted, "timed-out waiter retained queue capacity")
                release.countDown()
                assertTrue(replacement.await().isSuccess)
                assertEquals(listOf("held", "replacement"), server.commands.map { it.split(' ')[1] })
            } finally { release.countDown(); pool.close() }
        }
    }

    @Test fun closeWakesBothQueuesAndActiveCallsWithoutSendingWaitingWrites() = runBlocking {
        val started = CountDownLatch(3)
        val release = CountDownLatch(1)
        TcpFixture { socket, _, _ -> started.countDown(); release.await(10, TimeUnit.SECONDS); socket.reply("{}") }.use { server ->
            val db = FlameDB.connect(server.config(10_000).copy(maxQueuedReads = 2, maxQueuedWrites = 2))
            val active = listOf(async { runCatching { db.write("active", 1.0) } },
                async { runCatching { db.get("read1") } }, async { runCatching { db.get("read2") } })
            try {
                started.awaitReceived()
                val queued = (0 until 2).flatMap { i -> listOf(
                    async(start = CoroutineStart.UNDISPATCHED) { runCatching { db.write("queued:$i", 1.0) } },
                    async(start = CoroutineStart.UNDISPATCHED) { runCatching { db.get("queued:$i") } }) }
                db.close()
                withTimeout(1000) {
                    active.awaitAll().forEachIndexed { index, result ->
                        val failure = assertIs<FlameDBException>(result.exceptionOrNull())
                        assertEquals(FlameDBErrorKind.CLIENT_CLOSED, failure.kind)
                        assertEquals(if (index == 0) ExecutionStatus.UNKNOWN else ExecutionStatus.NOT_APPLICABLE, failure.executionStatus)
                    }
                    queued.awaitAll().forEach {
                        val failure = assertIs<FlameDBException>(it.exceptionOrNull())
                        assertEquals(FlameDBErrorKind.CLIENT_CLOSED, failure.kind)
                        assertEquals(ExecutionStatus.NOT_SENT, failure.executionStatus)
                    }
                }
                assertEquals(3, server.commands.size)
                val closed = assertFailsWith<FlameDBException> { db.write("later", 1.0) }
                assertEquals(ExecutionStatus.NOT_SENT, closed.executionStatus)
            } finally { release.countDown(); db.close() }
        }
    }

    @Test fun zeroQueueLimitsAllowFreeConnectionsAndRejectAnyWait() = runBlocking {
        val started = CountDownLatch(1)
        val release = CountDownLatch(1)
        TcpFixture { socket, line, _ ->
            if (line.contains("held")) { started.countDown(); release.await(5, TimeUnit.SECONDS) }
            socket.reply("{}")
        }.use { server ->
            FlameDB.connect(server.config().copy(maxQueuedWrites = 0)).use { db ->
                val active = async { db.write("held", 1.0) }
                try {
                    started.awaitReceived()
                    assertEquals(FlameDBErrorKind.OVERLOADED, assertFailsWith<FlameDBException> { db.write("overflow", 1.0) }.kind)
                } finally { release.countDown() }
                active.await(); db.write("after", 1.0)
            }
        }
    }
}
