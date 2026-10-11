package dev.bypixel.flamedb

import java.net.ServerSocket
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.*
import kotlin.test.*

class FailureTest {
    @Test fun reusedNativeCancellationCauseDoesNotAccumulateAnUnboundedSdkHistory() = runBlocking {
        val failures = java.util.concurrent.CopyOnWriteArrayList<FlameDBException>()
        TcpFixture { socket, _, _ -> socket.reply("{}") }.use { server ->
            FlameDB.connect(server.config().copy(onCommandTiming = { it.failure?.let(failures::add) })).use { db ->
                val source = CancellationException("shared cancellation source")
                val cancelledParent = Job().apply { cancel(source) }
                repeat(100) {
                    launch(cancelledParent, start = CoroutineStart.UNDISPATCHED) { db.write("neverSent", 1.0) }.join()
                }
                assertEquals(100, failures.size)
                assertTrue(failures.all { it.kind == FlameDBErrorKind.REQUEST_CANCELLED && it.executionStatus == ExecutionStatus.NOT_SENT })
                assertTrue(source.suppressed.count { it is FlameDBException } <= 1)
                assertTrue(server.commands.isEmpty())
                db.write("after", 1.0)
                assertEquals(listOf("WRITE after 1.0"), server.commands.toList())
            }
        }
    }

    @Test fun bulkDeleteKeepsLegacyErrorTextAndStructuredPerMetricDetails() = runBlocking {
        TcpFixture { socket, line, _ -> socket.reply(if (line == "METRICS WHERE account=\"a\"")
            """{"metric_keys":["m"]}""" else """{"error":"quorum unavailable"}""") }.use { server ->
            FlameDB.connect(server.config()).use { db ->
                val item = db.deleteByTags(mapOf("account" to "a")).metrics.single()
                assertEquals("quorum unavailable", item.error)
                assertNull(item.result)
                assertEquals(FlameDBErrorKind.SERVER_REJECTION, assertNotNull(item.failure).kind)
                assertTrue(item.failure!!.outcomeUnknown)
            }
        }
    }

    @Test fun serverRejectionPreservesSocketAndConservativelyMarksMutationUnknown() = runBlocking {
        TcpFixture { socket, line, _ ->
            socket.reply(if (line.contains("reject")) """{"error":"quorum unavailable"}""" else "{}")
        }.use { server ->
            FlameDB.connect(server.config()).use { db ->
                val write = assertFailsWith<FlameDBException> { db.write("reject", 1.0) }
                assertEquals(FlameDBErrorKind.SERVER_REJECTION, write.kind)
                assertEquals("quorum unavailable", write.message)
                assertEquals("WRITE", write.command)
                assertTrue(write.outcomeUnknown)
                db.write("next", 1.0)
                assertEquals(1, server.connections.get(), "server rejection should preserve a healthy socket")
                val read = assertFailsWith<FlameDBException> { db.get("reject") }
                assertEquals(FlameDBErrorKind.SERVER_REJECTION, read.kind)
                assertFalse(read.mutating)
                assertEquals(ExecutionStatus.NOT_APPLICABLE, read.executionStatus)
            }
        }
    }

    @Test fun failedReconnectAuthenticationDoesNotSendOrReplayTheMutation() = runBlocking {
        TcpFixture(authentication = { if (it == 2) "{\"error\":\"invalid key\"}\n".toByteArray() else "{\"auth\":\"ok\"}\n".toByteArray() }) { socket, line, _ ->
            if (line.contains("lost")) socket.close() else socket.reply("{}")
        }.use { server ->
            FlameDB.connect(server.config()).use { db ->
                assertTrue(assertFailsWith<FlameDBException> { db.write("lost", 1.0) }.outcomeUnknown)
                val auth = assertFailsWith<FlameDBException> { db.write("notSent", 1.0) }
                assertEquals(FlameDBErrorKind.AUTHENTICATION, auth.kind)
                assertEquals(FlameDBPhase.AUTHENTICATE, auth.phase)
                assertEquals(ExecutionStatus.NOT_SENT, auth.executionStatus)
                assertTrue(auth.mutating)
                db.write("next", 1.0)
                assertEquals(listOf("lost", "next"), server.commands.map { it.split(' ')[1] })
                assertEquals(3, server.connections.get())
            }
        }
    }

    @Test fun actualServerRestartRequiresFreshAuthAndNeverReplaysALostWrite() = runBlocking {
        val first = TcpFixture { socket, _, _ -> socket.reply("{}") }
        val port = first.listener.localPort
        val db = FlameDB.connect(first.config())
        try {
            db.write("before", 1.0)
            first.close()
            TcpFixture(port = port) { socket, _, _ -> socket.reply("{}") }.use { restarted ->
                val lost = assertFailsWith<FlameDBException> { db.write("uncertain", 1.0) }
                assertTrue(lost.outcomeUnknown)
                db.write("after", 1.0)
                assertEquals(listOf("WRITE after 1.0"), restarted.commands.toList())
                assertEquals(1, restarted.connections.get())
            }
        } finally { db.close(); first.close() }
    }

    @Test fun connectionRefusalPreservesCauseAndReportsNotSent() = runBlocking {
        val unusedPort = ServerSocket(0).use { it.localPort }
        val error = assertFailsWith<FlameDBException> { FlameDB.connect(FlameDBConfig("127.0.0.1", unusedPort, "test")) }
        assertEquals(FlameDBErrorKind.CONNECTION, error.kind)
        assertEquals(ExecutionStatus.NOT_SENT, error.executionStatus)
        assertNotNull(error.cause)
        assertEquals(FlameDBPhase.CONNECT, error.phase)
    }

    @Test fun activeExternalCancellationHasUnknownOutcomeAndRecoversWithoutReplay() = runBlocking {
        val started = CountDownLatch(1)
        val release = CountDownLatch(1)
        TcpFixture { socket, line, _ ->
            if (line.contains("held")) { started.countDown(); release.await(5, TimeUnit.SECONDS) }
            socket.reply("{}")
        }.use { server ->
            FlameDB.connect(server.config()).use { db ->
                try {
                    val call = async { runCatching { withTimeout(100) { db.write("held", 1.0) } } }
                    started.awaitReceived()
                    val cancelled = assertIs<TimeoutCancellationException>(call.await().exceptionOrNull())
                    val failure = assertNotNull(cancelled.flameDBFailure())
                    assertEquals(FlameDBErrorKind.REQUEST_CANCELLED, failure.kind)
                    assertTrue(failure.outcomeUnknown)
                    assertEquals(FlameDBPhase.REQUEST, failure.phase)
                    db.write("next", 1.0)
                    assertEquals(listOf("held", "next"), server.commands.map { it.split(' ')[1] })
                } finally { release.countDown() }
            }
        }
    }
}
