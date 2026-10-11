package dev.bypixel.flamedb

import java.io.BufferedReader
import java.io.BufferedWriter
import java.net.InetSocketAddress
import java.net.Socket
import java.net.SocketTimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

data class CommandTiming(
    val command: String,
    val read: Boolean,
    val queueWaitNanos: Long,
    /** Includes lazy connect/auth, socket I/O and JSON parsing. */
    val requestNanos: Long,
    val success: Boolean,
)

internal class CommandPool(private val cfg: FlameDBConfig, private val json: Json) : AutoCloseable {
    private val readCommands = setOf("GET", "LEADERBOARD", "GROUP_LEADERBOARD", "STATS", "METRICS", "PREVIEW_DELETE")
    @Volatile private var closed = false
    private val reads: Channel<Connection>
    private val writes: Channel<Connection>
    private val connections: List<Connection>

    init {
        require(cfg.timeoutMs > 0) { "timeoutMs must be positive" }
        require(cfg.readConnections in 0..1024 && cfg.writeConnections in 1..1024) { "invalid pool sizes" }
        require(cfg.apiKey.none { it == '\n' || it == '\r' }) { "invalid API key" }
        val readSlots = List(cfg.readConnections) { Connection() }
        val writeSlots = List(cfg.writeConnections) { Connection() }
        connections = readSlots + writeSlots
        writes = channel(writeSlots)
        reads = if (readSlots.isEmpty()) writes else channel(readSlots)
    }

    private fun channel(slots: List<Connection>): Channel<Connection> {
        lateinit var pool: Channel<Connection>
        // A receive can be cancelled after delivery but before resuming its
        // caller. Return that undelivered lease rather than shrinking the pool.
        pool = Channel(slots.size, onUndeliveredElement = { if (!closed) pool.trySend(it) })
        slots.forEach { pool.trySend(it) }
        return pool
    }

    suspend fun connect() {
        val deadline = System.nanoTime() + cfg.timeoutMs * 1_000_000L
        val connected = try {
            withTimeoutOrNull(cfg.timeoutMs.toLong()) {
                val connection = writes.receive()
                blocking(connection, writes) { cancelled -> connection.dial(cancelled, deadline) }
                true
            }
        } catch (error: SocketTimeoutException) {
            throw FlameDBException("FlameDB connect timeout", error)
        }
        if (connected == null) throw FlameDBException("FlameDB connect timeout", SocketTimeoutException())
    }

    suspend fun command(lines: List<String>): JsonObject {
        lines.forEach { require(it.none { c -> c in "\r\n\u0000\u001f" }) { "invalid command framing" } }
        val name = lines.first().substringBefore(' ')
        val read = name in readCommands
        val pool = if (read) reads else writes
        val started = System.nanoTime()
        var acquired = 0L
        var success = false
        try {
            val result = withTimeoutOrNull(cfg.timeoutMs.toLong()) {
                if (closed) throw FlameDBException("FlameDB client is closed")
                val connection = try { pool.receive() } catch (error: CancellationException) {
                    if (closed) throw FlameDBException("FlameDB client is closed", error)
                    throw error
                }
                acquired = System.nanoTime()
                // The blocking worker returns the lease only after socket I/O
                // actually exits, even if its coroutine has already cancelled.
                blocking(connection, pool) { cancelled ->
                    connection.command(lines, cancelled, started + cfg.timeoutMs * 1_000_000L)
                }
            } ?: throw FlameDBException(
                if (acquired == 0L) "FlameDB pool wait timeout; command was not sent"
                else "FlameDB command timeout; outcome may be unknown; not retried",
                SocketTimeoutException("total command deadline exceeded"),
            )
            success = true
            return result
        } finally {
            val finished = System.nanoTime()
            runCatching { cfg.onCommandTiming?.invoke(CommandTiming(name, read,
                (if (acquired == 0L) finished else acquired) - started,
                if (acquired == 0L) 0 else finished - acquired, success)) }
        }
    }

    private suspend fun <T> blocking(connection: Connection, pool: Channel<Connection>, block: (AtomicBoolean) -> T): T =
        suspendCancellableCoroutine { continuation ->
            val cancelled = AtomicBoolean(false)
            val operationLock = Any()
            var finished = false
            continuation.invokeOnCancellation {
                synchronized(operationLock) {
                    cancelled.set(true)
                    if (!finished) connection.invalidate()
                }
            }
            try {
                Dispatchers.IO.dispatch(continuation.context, Runnable {
                    val result = try {
                        if (cancelled.get()) throw CancellationException("request cancelled before send")
                        Result.success(block(cancelled))
                    } catch (error: Throwable) {
                        Result.failure(error)
                    }
                    synchronized(operationLock) {
                        finished = true
                        pool.trySend(connection)
                    }
                    continuation.resumeWith(result)
                })
            } catch (error: Throwable) {
                synchronized(operationLock) {
                    finished = true
                    pool.trySend(connection)
                }
                continuation.resumeWith(Result.failure(error))
            }
        }

    override fun close() {
        closed = true
        reads.cancel()
        writes.cancel()
        connections.forEach { it.close() }
    }

    private inner class Connection {
        private val lifecycle = Any()
        private var permanentlyClosed = false
        private var socket: Socket? = null
        private lateinit var reader: BufferedReader
        private lateinit var writer: BufferedWriter

        fun invalidate() = synchronized(lifecycle) {
            socket?.let { runCatching { it.close() } }
            socket = null
        }

        fun close() = synchronized(lifecycle) {
            permanentlyClosed = true
            invalidate()
        }

        private fun remaining(deadline: Long): Int {
            val nanos = deadline - System.nanoTime()
            if (nanos <= 0) throw SocketTimeoutException("total command deadline exceeded")
            return ((nanos + 999_999) / 1_000_000).coerceAtMost(Int.MAX_VALUE.toLong()).toInt()
        }

        fun dial(cancelled: AtomicBoolean, deadline: Long) {
            val current = synchronized(lifecycle) {
                if (permanentlyClosed || closed) throw FlameDBException("FlameDB client is closed")
                if (cancelled.get()) throw CancellationException("request cancelled")
                socket ?: Socket().also { socket = it }
            }
            if (current.isConnected) return
            try {
                current.tcpNoDelay = true
                current.connect(InetSocketAddress(cfg.host, cfg.port), remaining(deadline))
                reader = current.getInputStream().bufferedReader(Charsets.UTF_8)
                writer = current.getOutputStream().bufferedWriter(Charsets.UTF_8)
                fun receive(): JsonObject {
                    current.soTimeout = remaining(deadline)
                    return json.parseToJsonElement(reader.readLine() ?: throw FlameDBException("FlameDB connection closed")).jsonObject
                }
                check(receive()["auth"]?.jsonPrimitive?.content == "required") { "FlameDB: unexpected auth challenge" }
                writer.write("AUTH ${cfg.apiKey}\n")
                writer.flush()
                val auth = receive()
                if (auth["auth"]?.jsonPrimitive?.content != "ok") throw FlameDBException("FlameDB auth failed: ${auth["error"]}")
            } catch (error: Exception) {
                invalidate()
                throw error
            }
        }

        fun command(lines: List<String>, cancelled: AtomicBoolean, deadline: Long): JsonObject {
            try { dial(cancelled, deadline) } catch (error: CancellationException) { throw error }
            catch (error: Exception) { throw FlameDBException("FlameDB reconnect failed; command was not sent", error) }
            val current = synchronized(lifecycle) { socket } ?: throw CancellationException("request cancelled")
            val response = try {
                if (cancelled.get()) throw CancellationException("request cancelled before send")
                current.soTimeout = remaining(deadline)
                for (line in lines) { writer.write(line); writer.write("\n") }
                writer.flush()
                json.parseToJsonElement(reader.readLine() ?: throw FlameDBException("FlameDB connection closed")).jsonObject
            } catch (error: Exception) {
                invalidate()
                throw FlameDBException("FlameDB command failed; outcome may be unknown. The command was not retried; the next call will reconnect.", error)
            }
            response["error"]?.jsonPrimitive?.content?.let { throw FlameDBException(it) }
            remaining(deadline)
            return response
        }
    }
}
