package dev.bypixel.flamedb

import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.*
import kotlin.concurrent.thread
import kotlin.test.*

class ResponseTest {
    @Test fun oversizedUnterminatedReplyIsRejectedBeforeTheServerFinishesIt() = runBlocking {
        val release = CountDownLatch(1)
        TcpFixture { socket, line, _ ->
            if (line.contains("oversize")) {
                socket.send(ByteArray(65) { 'x'.code.toByte() }) // No LF, and never send the remainder.
                release.await(5, TimeUnit.SECONDS)
            } else socket.reply("{}")
        }.use { server ->
            FlameDB.connect(server.config().copy(maxResponseBytes = 64)).use { db ->
                try {
                    val error = withTimeout(1000) { assertFailsWith<FlameDBException> { db.write("oversize", 1.0) } }
                    assertEquals(FlameDBErrorKind.RESPONSE_TOO_LARGE, error.kind)
                    assertTrue(error.outcomeUnknown)
                    assertEquals(FlameDBPhase.REQUEST, error.phase)
                    assertNotNull(error.cause)
                    db.write("next", 1.0)
                    assertEquals(2, server.connections.get())
                    assertEquals(listOf("oversize", "next"), server.commands.map { it.split(' ')[1] })
                } finally { release.countDown() }
            }
        }
    }

    @Test fun sizeLimitAppliesToBothChallengeAndAuthenticationResponse() = runBlocking {
        for (challengeTooLarge in listOf(true, false)) {
            val tooLarge = ("{" + " ".repeat(65) + "}\n").toByteArray()
            TcpFixture(
                challenge = { if (challengeTooLarge && it == 1) tooLarge else "{\"auth\":\"required\"}\n".toByteArray() },
                authentication = { if (!challengeTooLarge && it == 1) tooLarge else "{\"auth\":\"ok\"}\n".toByteArray() },
            ) { socket, _, _ -> socket.reply("{}") }.use { server ->
                val cfg = server.config().copy(maxResponseBytes = 64)
                val error = assertFailsWith<FlameDBException> { FlameDB.connect(cfg).close() }
                assertEquals(FlameDBErrorKind.RESPONSE_TOO_LARGE, error.kind)
                assertEquals(FlameDBPhase.AUTHENTICATE, error.phase)
                assertEquals(ExecutionStatus.NOT_SENT, error.executionStatus)
                assertFalse(error.mutating)
                FlameDB.connect(cfg).use { it.write("after", 1.0) }
                assertEquals(listOf("WRITE after 1.0"), server.commands.toList())
            }
        }
    }

    @Test fun utf8ByteLimitAndSplitMultibyteCharactersAreHandledExactly() = runBlocking {
        val value = "Hello 🌍🔥ä"
        val payload = """{"metric_keys":["$value"]}""".toByteArray(Charsets.UTF_8)
        assertTrue(payload.size > value.length)
        TcpFixture { socket, line, _ ->
            if (line.contains("large")) socket.reply("""{"metric_keys":["$value!"]}""")
            else {
                // Split every UTF-8 character over socket writes; use CRLF too.
                for (byte in payload + "\r\n".toByteArray()) { socket.send(byteArrayOf(byte)); Thread.sleep(1) }
            }
        }.use { server ->
            FlameDB.connect(server.config().copy(maxResponseBytes = payload.size + 1)).use { db ->
                assertEquals(listOf(value), db.listMetrics())
                // One additional ASCII byte still fits without CR; two do not.
                assertEquals(listOf(value + "!"), db.listMetrics(mapOf("mode" to "large")))
            }
        }
        TcpFixture { socket, _, _ -> socket.reply("""{"metric_keys":["$value"]}""") }.use { server ->
            FlameDB.connect(server.config().copy(maxResponseBytes = payload.size - 1)).use { db ->
                assertEquals(FlameDBErrorKind.RESPONSE_TOO_LARGE, assertFailsWith<FlameDBException> { db.listMetrics() }.kind)
            }
        }
    }

    @Test fun truncatedMalformedUtf8AndInvalidJsonDiscardOnlyTheirConnection() = runBlocking {
        val invalid = listOf(
            "{}".toByteArray(), // Complete JSON, missing mandatory LF.
            "{\"x\":".toByteArray(),
            byteArrayOf(123, 34, 120, 34, 58, 34, 0xc3.toByte(), 0x28, 34, 125, 10),
            byteArrayOf(123, 34, 120, 34, 58, 34, 0xf0.toByte(), 10),
            "not JSON\n".toByteArray(),
            "[]\n".toByteArray(),
            "{\"error\":42}\n".toByteArray(),
        )
        for ((index, payload) in invalid.withIndex()) {
            TcpFixture { socket, line, _ ->
                if (line.contains("broken")) { socket.send(payload); socket.shutdownOutput() }
                else socket.reply("{}")
            }.use { server ->
                FlameDB.connect(server.config()).use { db ->
                    val error = assertFailsWith<FlameDBException> { db.write("broken:$index", 1.0) }
                    assertEquals(FlameDBErrorKind.INVALID_RESPONSE, error.kind, "case $index")
                    assertTrue(error.outcomeUnknown)
                    assertNotNull(error.cause)
                    db.write("next", 1.0)
                    assertEquals(2, server.connections.get())
                    assertEquals(2, server.commands.size, "failed mutation must never be replayed")
                }
            }
        }
    }

    @Test fun slowTrickleCannotResetTheTotalDeadline() = runBlocking {
        val stopped = CountDownLatch(1)
        TcpFixture { socket, line, _ ->
            if (line.contains("slow")) {
                try { repeat(20) { socket.send(byteArrayOf(' '.code.toByte())); Thread.sleep(30) } }
                finally { stopped.countDown() }
            } else socket.reply("{}")
        }.use { server ->
            FlameDB.connect(server.config(200)).use { db ->
                val started = System.nanoTime()
                val error = assertFailsWith<FlameDBException> { db.write("slow", 1.0) }
                val elapsedMs = (System.nanoTime() - started) / 1_000_000
                assertEquals(FlameDBErrorKind.REQUEST_TIMEOUT, error.kind)
                assertTrue(error.outcomeUnknown)
                assertTrue(elapsedMs < 350, "trickle extended total deadline to ${elapsedMs}ms")
                db.write("next", 1.0)
                assertEquals(2, server.commands.size)
                stopped.awaitReceived()
            }
        }
    }

    @Test fun invalidTypedResultsAreStructuredAndDoNotPoisonThePool() = runBlocking {
        TcpFixture { socket, line, _ ->
            socket.reply(when {
                line.startsWith("WRITE_BATCH") -> """{"ok":true,"accepted":"broken","failed":0}"""
                line.startsWith("STATS") -> """{"stats":{"metric":{},"tag_stats":[]}}"""
                else -> "{}"
            })
        }.use { server ->
            FlameDB.connect(server.config()).use { db ->
                val batch = assertFailsWith<FlameDBException> { db.writeBatch(listOf(WriteBatchItem("m", 1.0))) }
                assertEquals(FlameDBErrorKind.INVALID_RESPONSE, batch.kind)
                assertTrue(batch.outcomeUnknown)
                assertNotNull(batch.cause?.cause)
                val read = assertFailsWith<FlameDBException> { db.stats("m", "x") }
                assertEquals(FlameDBErrorKind.INVALID_RESPONSE, read.kind)
                assertEquals(ExecutionStatus.NOT_APPLICABLE, read.executionStatus)
                assertFalse(read.mutating)
                db.write("next", 1.0)
                assertEquals(1, server.commands.count { it.startsWith("WRITE_BATCH") })
            }
        }
    }

    @Test fun knownUnsafeReadLineCounterexampleFailsTheSameLimitOracle() {
        val limit = 64
        val payload = ("{\"x\":\"" + "🌍".repeat(100) + "\"}\n").toByteArray()
        fun probe(unsafe: Boolean): Boolean = ServerSocket(0).use { server ->
            val worker = thread(isDaemon = true) { server.accept().use { it.send(payload) } }
            val rejected = Socket("127.0.0.1", server.localPort).use { socket ->
                socket.soTimeout = 1000
                try {
                    if (unsafe) socket.getInputStream().bufferedReader(Charsets.UTF_8).readLine()
                    else ResponseReader(socket.getInputStream(), limit).readLine { }
                    false
                } catch (error: ProtocolResponseException) { error.kind == FlameDBErrorKind.RESPONSE_TOO_LARGE }
            }
            worker.join(1000)
            rejected
        }
        fun rejectionOracle(unsafe: Boolean) { assertTrue(probe(unsafe), "oversize frame was accepted") }
        // Reproduce the old reader's failure, then run the identical oracle on
        // the bounded reader. This catches a vacuous/always-passing size test.
        assertFailsWith<AssertionError> { rejectionOracle(unsafe = true) }
        rejectionOracle(unsafe = false)
    }
}
