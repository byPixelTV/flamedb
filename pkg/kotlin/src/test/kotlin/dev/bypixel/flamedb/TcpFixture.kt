package dev.bypixel.flamedb

import java.io.IOException
import java.net.ServerSocket
import java.net.Socket
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlin.concurrent.thread
import kotlin.test.assertTrue

internal class TcpFixture(
    port: Int = 0,
    private val recordCommands: Boolean = true,
    private val challenge: (Int) -> ByteArray = { "{\"auth\":\"required\"}\n".toByteArray() },
    private val authentication: (Int) -> ByteArray = { "{\"auth\":\"ok\"}\n".toByteArray() },
    private val respond: (Socket, String, Int) -> Unit,
) : AutoCloseable {
    val listener = ServerSocket().apply { reuseAddress = true; bind(InetSocketAddress("127.0.0.1", port)) }
    val commands = CopyOnWriteArrayList<String>()
    val sockets = ConcurrentHashMap.newKeySet<Socket>()
    val connections = AtomicInteger()
    private val failures = ConcurrentLinkedQueue<Throwable>()
    private val workers = Executors.newCachedThreadPool { r -> Thread(r, "sdk-test-server").apply { isDaemon = true } }
    private val acceptor = thread(isDaemon = true, name = "sdk-test-accept") {
        while (!listener.isClosed) {
            val socket = try { listener.accept() } catch (_: IOException) { break }
            val generation = connections.incrementAndGet()
            sockets.add(socket)
            workers.submit {
                try {
                    socket.use {
                        socket.tcpNoDelay = true
                        socket.soTimeout = 30_000
                        socket.send(challenge(generation))
                        val reader = socket.getInputStream().bufferedReader(Charsets.UTF_8)
                        val authLine = reader.readLine() ?: return@submit
                        check(authLine == "AUTH test")
                        socket.send(authentication(generation))
                        while (true) {
                            var line = reader.readLine() ?: break
                            if (line == "WRITE_BATCH") {
                                val frame = mutableListOf(line)
                                do { line = reader.readLine() ?: throw IOException("incomplete test batch"); frame.add(line) } while (line != "END")
                                line = frame.joinToString("|")
                            }
                            if (recordCommands) commands.add(line)
                            respond(socket, line, generation)
                        }
                    }
                } catch (_: IOException) {
                    // Deliberate connection resets/oversize replies are test inputs.
                } catch (_: InterruptedException) {
                    // Shutdown releases fixtures whose response is held by a latch.
                } catch (error: Throwable) { failures.add(error) }
                finally { sockets.remove(socket) }
            }
        }
    }

    fun config(timeout: Int = 3000) = FlameDBConfig("127.0.0.1", listener.localPort, "test", timeoutMs = timeout)

    override fun close() {
        listener.close()
        sockets.forEach { runCatching { it.close() } }
        workers.shutdownNow()
        acceptor.join(1000)
        workers.awaitTermination(2, TimeUnit.SECONDS)
        assertTrue(failures.isEmpty(), "test server failed: $failures")
    }
}

internal fun Socket.send(bytes: ByteArray) { getOutputStream().write(bytes); getOutputStream().flush() }
internal fun Socket.reply(json: String) = send((json + "\n").toByteArray(Charsets.UTF_8))
internal suspend fun CountDownLatch.awaitReceived() {
    assertTrue(withContext(Dispatchers.IO) { await(5, TimeUnit.SECONDS) }, "test server did not receive command")
}
