package dev.bypixel.flamedb

import java.net.ServerSocket
import java.net.Socket
import java.net.SocketTimeoutException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.runBlocking
import kotlin.test.*

class RecoveryTest {
    @Test fun reconnectsAfterLostReplyWithoutReplayingWrite() = checkRecovery(false)
    @Test fun reconnectsAfterTimeoutWithoutConsumingLateReply() = checkRecovery(true)

    private fun checkRecovery(lateReply: Boolean) = runBlocking {
        ServerSocket(0).use { server ->
            server.soTimeout = 5000
            val worker = Executors.newSingleThreadExecutor { task -> Thread(task).apply { isDaemon = true } }
            val timedOut = CountDownLatch(1)
            try {
                val handled = worker.submit<List<String>> {
                    val commands = mutableListOf<String>()
                    server.accept().use { socket ->
                        val (reader, writer) = authenticate(socket)
                        commands += reader.readLine()
                        if (lateReply) {
                            check(timedOut.await(5, TimeUnit.SECONDS))
                            runCatching { writer.write("{}\n"); writer.flush() }
                        }
                        // The first write may have been committed, but its ACK is lost.
                    }
                    server.accept().use { socket ->
                        val (reader, writer) = authenticate(socket)
                        commands += reader.readLine()
                        writer.write("{}\n"); writer.flush()
                        // Server errors do not invalidate a healthy connection.
                        commands += reader.readLine()
                        writer.write("{\"error\":\"test rejection\"}\n"); writer.flush()
                        commands += reader.readLine()
                        writer.write("{}\n"); writer.flush()
                        assertNull(reader.readLine())
                    }
                    commands
                }
                val db = FlameDB.connect(FlameDBConfig("127.0.0.1", server.localPort, "test", timeoutMs = 300))
                db.use {
                    val failure = assertFailsWith<FlameDBException> { db.write("smp:first", 1.0) }
                    assertNotNull(failure.cause)
                    assertTrue(failure.message!!.contains("not retried"))
                    if (lateReply) {
                        // Coroutine stack-trace recovery may copy the exception.
                        assertTrue(generateSequence(failure as Throwable) { it.cause }.any { it is SocketTimeoutException })
                        timedOut.countDown()
                    }
                    db.write("smp:second", 2.0)
                    val rejected = assertFailsWith<FlameDBException> { db.write("smp:rejected", 3.0) }
                    assertEquals("test rejection", rejected.message)
                    db.write("smp:third", 4.0)
                }
                assertEquals("FlameDB client is closed", assertFailsWith<FlameDBException> {
                    db.write("smp:closed", 5.0)
                }.message)
                val commands = handled.get(5, TimeUnit.SECONDS)
                assertEquals(listOf("smp:first", "smp:second", "smp:rejected", "smp:third"), commands.map { it.split(' ')[1] })
            } finally {
                worker.shutdownNow()
            }
        }
    }

    private fun authenticate(socket: Socket): Pair<java.io.BufferedReader, java.io.BufferedWriter> {
        socket.soTimeout = 5000
        val reader = socket.getInputStream().bufferedReader(Charsets.UTF_8)
        val writer = socket.getOutputStream().bufferedWriter(Charsets.UTF_8)
        writer.write("{\"auth\":\"required\"}\n"); writer.flush()
        assertEquals("AUTH test", reader.readLine())
        writer.write("{\"auth\":\"ok\"}\n"); writer.flush()
        return reader to writer
    }
}
