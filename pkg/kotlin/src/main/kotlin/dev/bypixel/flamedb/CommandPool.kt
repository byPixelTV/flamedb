package dev.bypixel.flamedb

import java.io.BufferedWriter
import java.net.InetSocketAddress
import java.net.Socket
import java.net.SocketTimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonObject

data class CommandTiming(
    val command: String,
    val read: Boolean,
    val queueWaitNanos: Long,
    /** Includes lazy connect/auth, socket I/O, JSON parsing and typed decoding. */
    val requestNanos: Long,
    val success: Boolean,
    /** Per-command details, also available when an outer coroutine timeout hides a recovered exception. */
    val failure: FlameDBException? = null,
)

internal class CommandPool(private val cfg: FlameDBConfig, private val json: Json,
                           private val ioDispatcher: CoroutineDispatcher = Dispatchers.IO) : AutoCloseable {
    private val readCommands = setOf("GET", "LEADERBOARD", "GROUP_LEADERBOARD", "STATS", "METRICS", "PREVIEW_DELETE")
    @Volatile private var closed = false
    private val lifetime = Job()
    private val reads: Admission
    private val writes: Admission
    private val connections: List<Connection>

    init {
        require(cfg.timeoutMs > 0) { "timeoutMs must be positive" }
        require(cfg.readConnections in 0..1024 && cfg.writeConnections in 1..1024) { "invalid pool sizes" }
        require(cfg.maxQueuedReads >= 0 && cfg.maxQueuedWrites >= 0) { "queue limits must be non-negative" }
        require(cfg.maxResponseBytes > 0) { "maxResponseBytes must be positive" }
        require(cfg.apiKey.none { it in "\n\r\u0000\u001f" }) { "invalid API key" }
        val readSlots = List(cfg.readConnections) { Connection() }
        val writeSlots = List(cfg.writeConnections) { Connection() }
        connections = readSlots + writeSlots
        writes = Admission(channel(writeSlots), cfg.maxQueuedWrites)
        reads = Admission(if (readSlots.isEmpty()) writes.available else channel(readSlots), cfg.maxQueuedReads)
    }

    private fun channel(slots: List<Connection>): Channel<Connection> {
        lateinit var pool: Channel<Connection>
        // Receive can be cancelled after delivery but before caller resumption.
        pool = Channel(slots.size, onUndeliveredElement = { if (!closed) pool.trySend(it) })
        slots.forEach { pool.trySend(it) }
        return pool
    }

    private inner class Admission(val available: Channel<Connection>, private val limit: Int) {
        private val waiting = AtomicInteger()

        suspend fun acquire(state: RequestState): Connection {
            if (closed) throw state.failure(FlameDBErrorKind.CLIENT_CLOSED, "FlameDB client is closed")
            available.tryReceive().getOrNull()?.let { return it }
            while (true) {
                val count = waiting.get()
                if (count >= limit) {
                    available.tryReceive().getOrNull()?.let { return it }
                    if (closed) throw state.failure(FlameDBErrorKind.CLIENT_CLOSED, "FlameDB client is closed")
                    throw state.failure(FlameDBErrorKind.OVERLOADED,
                        "FlameDB ${if (state.read) "read" else "write"} queue full (limit=$limit); command was not sent")
                }
                if (waiting.compareAndSet(count, count + 1)) break
            }
            try {
                if (closed) throw state.failure(FlameDBErrorKind.CLIENT_CLOSED, "FlameDB client is closed")
                return available.receive()
            } finally { waiting.decrementAndGet() }
        }
    }

    private class RequestState(val name: String, val read: Boolean) {
        @Volatile var phase = FlameDBPhase.POOL_WAIT
        val sent = AtomicBoolean(false)
        val received = AtomicBoolean(false)
        val workerStarted = AtomicBoolean(false)
        fun failure(kind: FlameDBErrorKind, message: String, cause: Throwable? = null): FlameDBException =
            FlameDBException(message, kind, name, !read,
                when {
                    !sent.get() -> ExecutionStatus.NOT_SENT
                    read -> ExecutionStatus.NOT_APPLICABLE
                    received.get() -> ExecutionStatus.RESPONSE_RECEIVED
                    else -> ExecutionStatus.UNKNOWN
                }, phase, cause)
    }

    private fun failure(state: RequestState, error: Exception): FlameDBException {
        if (error is FlameDBException) return error
        val kind = when {
            closed -> FlameDBErrorKind.CLIENT_CLOSED
            error is ProtocolResponseException -> error.kind
            state.phase == FlameDBPhase.CONNECT -> FlameDBErrorKind.CONNECTION
            state.phase == FlameDBPhase.AUTHENTICATE -> FlameDBErrorKind.AUTHENTICATION
            error is SocketTimeoutException -> FlameDBErrorKind.REQUEST_TIMEOUT
            else -> FlameDBErrorKind.REQUEST_FAILED
        }
        val message = when (kind) {
            FlameDBErrorKind.CLIENT_CLOSED -> "FlameDB client is closed"
            FlameDBErrorKind.CONNECTION -> "FlameDB connect/reconnect failed; command was not sent"
            FlameDBErrorKind.AUTHENTICATION -> "FlameDB auth failed; command was not sent"
            else -> "FlameDB ${kind.name.lowercase()}: ${error.message}; not retried"
        }
        return state.failure(kind, message, error)
    }

    private fun attachCancellation(error: CancellationException, detail: FlameDBException) {
        // Coroutine stack recovery creates copies. withTimeout can later
        // rethrow the original, so attach to cancellation causes as well.
        val seen = java.util.Collections.newSetFromMap(java.util.IdentityHashMap<Throwable, Boolean>())
        var current: Throwable? = error
        while (current is CancellationException && seen.add(current)) {
            synchronized(current) {
                // A caller can reuse a cancellation cause across many jobs.
                // Retain one diagnostic there, rather than an unbounded history.
                if (current.suppressed.none { it is FlameDBException }) current.addSuppressed(detail)
            }
            current = current.cause
        }
    }

    suspend fun connect() {
        val state = RequestState("AUTH", true)
        val deadline = System.nanoTime() + cfg.timeoutMs * 1_000_000L
        try {
            val connected = withTimeoutOrNull(cfg.timeoutMs.toLong()) {
                val connection = writes.acquire(state)
                state.phase = FlameDBPhase.CONNECT
                blocking(connection, writes.available, state, deadline) { cancelled -> connection.dial(cancelled, state, deadline) }
                true
            }
            if (connected == null) throw state.failure(
                if (!state.workerStarted.get()) FlameDBErrorKind.REQUEST_TIMEOUT
                else if (state.phase == FlameDBPhase.AUTHENTICATE) FlameDBErrorKind.AUTHENTICATION else FlameDBErrorKind.CONNECTION,
                if (!state.workerStarted.get()) "FlameDB IO dispatch timeout; handshake was not started"
                else "FlameDB connect timeout; command was not sent", SocketTimeoutException("total connect deadline exceeded"))
        } catch (error: CancellationException) {
            attachCancellation(error, state.failure(FlameDBErrorKind.REQUEST_CANCELLED, "FlameDB connect cancelled"))
            throw error
        } catch (error: Exception) { throw failure(state, error) }
    }

    suspend fun command(lines: List<String>): JsonObject = command(lines) { it }

    suspend fun <T> command(lines: List<String>, decode: (JsonObject) -> T): T {
        require(lines.isNotEmpty()) { "empty command" }
        lines.forEach { require(it.none { c -> c in "\r\n\u0000\u001f" }) { "invalid command framing" } }
        val name = lines.first().substringBefore(' ')
        val state = RequestState(name, name in readCommands)
        val pool = if (state.read) reads else writes
        val started = System.nanoTime()
        val deadline = started + cfg.timeoutMs * 1_000_000L
        var acquired = 0L
        var success = false
        var reportedFailure: FlameDBException? = null
        try {
            // Wrap generic results: a decoder returning null must not look like a timeout.
            val result = withTimeoutOrNull(cfg.timeoutMs.toLong()) {
                val connection = pool.acquire(state)
                acquired = System.nanoTime()
                state.phase = FlameDBPhase.CONNECT
                Pair(true, blocking(connection, pool.available, state, deadline) { cancelled ->
                    connection.command(lines, cancelled, state, deadline, decode)
                })
            } ?: throw state.failure(
                if (acquired != 0L && !state.workerStarted.get()) FlameDBErrorKind.REQUEST_TIMEOUT else when (state.phase) {
                    FlameDBPhase.POOL_WAIT -> FlameDBErrorKind.POOL_WAIT_TIMEOUT
                    FlameDBPhase.CONNECT -> FlameDBErrorKind.CONNECTION
                    FlameDBPhase.AUTHENTICATE -> FlameDBErrorKind.AUTHENTICATION
                    FlameDBPhase.REQUEST -> FlameDBErrorKind.REQUEST_TIMEOUT
                }, if (acquired == 0L) "FlameDB pool wait timeout; command was not sent"
                else if (!state.workerStarted.get()) "FlameDB IO dispatch timeout; command was not sent"
                else "FlameDB command timeout; not retried", SocketTimeoutException("total command deadline exceeded"))
            success = true
            return result.second
        } catch (error: CancellationException) {
            if (closed) {
                reportedFailure = state.failure(FlameDBErrorKind.CLIENT_CLOSED, "FlameDB client is closed", error)
                throw reportedFailure
            }
            reportedFailure = state.failure(FlameDBErrorKind.REQUEST_CANCELLED, "FlameDB command cancelled; not retried")
            attachCancellation(error, reportedFailure)
            throw error
        } catch (error: Exception) {
            reportedFailure = failure(state, error)
            throw reportedFailure
        }
        finally {
            val finished = System.nanoTime()
            runCatching { cfg.onCommandTiming?.invoke(CommandTiming(name, state.read,
                (if (acquired == 0L) finished else acquired) - started,
                if (acquired == 0L) 0 else finished - acquired, success, reportedFailure)) }
        }
    }

    private suspend fun <T> blocking(connection: Connection, pool: Channel<Connection>, state: RequestState,
                                    deadline: Long, block: (AtomicBoolean) -> T): T =
        suspendCancellableCoroutine { continuation ->
            val cancelled = AtomicBoolean(false)
            val operationLock = Any()
            var finished = false
            continuation.invokeOnCancellation {
                synchronized(operationLock) {
                    cancelled.set(true)
                    // A queued worker has not touched the socket. Preserve a
                    // healthy connection; keep its lease until the task drains.
                    if (!finished && state.workerStarted.get()) connection.invalidate()
                }
            }
            // Socket close alone cannot wake a worker still awaiting an IO
            // dispatcher thread. Cancel its continuation on client shutdown too.
            val closeHandle = lifetime.invokeOnCompletion {
                continuation.cancel(CancellationException("FlameDB client is closed"))
            }
            try {
                ioDispatcher.dispatch(continuation.context, Runnable {
                    val result = try {
                        synchronized(operationLock) {
                            if (cancelled.get()) throw CancellationException("request cancelled before send")
                            // Coroutine timers can themselves be delayed by a
                            // busy dispatcher. Check the actual deadline before
                            // any connect/auth/socket work, not just cancellation.
                            if (System.nanoTime() >= deadline) throw state.failure(FlameDBErrorKind.REQUEST_TIMEOUT,
                                "FlameDB IO dispatch timeout; command was not sent",
                                SocketTimeoutException("total command deadline exceeded before IO started"))
                            state.workerStarted.set(true)
                        }
                        Result.success(block(cancelled))
                    } catch (error: Throwable) { Result.failure(error) }
                    closeHandle.dispose()
                    synchronized(operationLock) { finished = true; pool.trySend(connection) }
                    continuation.resumeWith(result)
                })
            } catch (error: Throwable) {
                closeHandle.dispose()
                synchronized(operationLock) { finished = true; pool.trySend(connection) }
                continuation.resumeWith(Result.failure(error))
            }
        }

    override fun close() {
        closed = true
        lifetime.cancel()
        reads.available.cancel()
        writes.available.cancel()
        connections.forEach { it.close() }
    }

    private class Transport(val socket: Socket, val reader: ResponseReader, val writer: BufferedWriter)

    private inner class Connection {
        private val lifecycle = Any()
        private var permanentlyClosed = false
        private var socket: Socket? = null
        private var transport: Transport? = null

        fun invalidate() = synchronized(lifecycle) {
            socket?.let { runCatching { it.close() } }
            socket = null
            transport = null
        }

        fun close() = synchronized(lifecycle) { permanentlyClosed = true; invalidate() }

        private fun remaining(deadline: Long): Int {
            val nanos = deadline - System.nanoTime()
            if (nanos <= 0) throw SocketTimeoutException("total command deadline exceeded")
            return ((nanos + 999_999) / 1_000_000).coerceAtMost(Int.MAX_VALUE.toLong()).toInt()
        }

        private fun receive(current: Transport, deadline: Long): JsonObject {
            val line = current.reader.readLine { current.socket.soTimeout = remaining(deadline) }
            return try { json.parseToJsonElement(line).jsonObject }
            catch (error: Exception) {
                throw ProtocolResponseException(FlameDBErrorKind.INVALID_RESPONSE, "invalid FlameDB JSON object", error)
            }
        }

        fun dial(cancelled: AtomicBoolean, state: RequestState, deadline: Long): Transport {
            state.phase = FlameDBPhase.CONNECT
            val current = synchronized(lifecycle) {
                if (permanentlyClosed || closed) throw state.failure(FlameDBErrorKind.CLIENT_CLOSED, "FlameDB client is closed")
                if (cancelled.get()) throw CancellationException("request cancelled")
                transport?.let { return it }
                Socket().also { socket = it }
            }
            try {
                current.tcpNoDelay = true
                current.connect(InetSocketAddress(cfg.host, cfg.port), remaining(deadline))
                val opened = Transport(current, ResponseReader(current.getInputStream(), cfg.maxResponseBytes),
                    current.getOutputStream().bufferedWriter(Charsets.UTF_8))
                state.phase = FlameDBPhase.AUTHENTICATE
                if ((receive(opened, deadline)["auth"] as? JsonPrimitive)?.content != "required") {
                    throw state.failure(FlameDBErrorKind.AUTHENTICATION, "FlameDB: unexpected auth challenge")
                }
                opened.writer.write("AUTH ${cfg.apiKey}\n")
                opened.writer.flush()
                val auth = receive(opened, deadline)
                if ((auth["auth"] as? JsonPrimitive)?.content != "ok") {
                    throw state.failure(FlameDBErrorKind.AUTHENTICATION, "FlameDB auth failed: ${auth["error"]}")
                }
                remaining(deadline)
                synchronized(lifecycle) {
                    if (socket !== current || permanentlyClosed || cancelled.get()) throw CancellationException("request cancelled during auth")
                    transport = opened
                }
                return opened
            } catch (error: Exception) { invalidate(); throw error }
        }

        fun <T> command(lines: List<String>, cancelled: AtomicBoolean, state: RequestState, deadline: Long,
                        decode: (JsonObject) -> T): T {
            try {
                val current = dial(cancelled, state, deadline)
                state.phase = FlameDBPhase.REQUEST
                if (cancelled.get()) throw CancellationException("request cancelled before send")
                current.socket.soTimeout = remaining(deadline)
                // A failing write/flush cannot prove how many bytes the server saw.
                state.sent.set(true)
                for (line in lines) { current.writer.write(line); current.writer.write("\n") }
                current.writer.flush()
                val response = receive(current, deadline)
                remaining(deadline)
                response["error"]?.let { error ->
                    if (error !is JsonPrimitive || !error.isString) throw ProtocolResponseException(
                        FlameDBErrorKind.INVALID_RESPONSE, "FlameDB error field must be a string")
                    // Errors can follow partial writes or failed quorum.
                    throw state.failure(FlameDBErrorKind.SERVER_REJECTION, error.content)
                }
                val result = try { decode(response) } catch (error: CancellationException) { throw error }
                catch (error: Exception) {
                    throw ProtocolResponseException(FlameDBErrorKind.INVALID_RESPONSE, "invalid FlameDB result: ${error.message}", error)
                }
                state.received.set(true)
                remaining(deadline)
                return result
            } catch (error: Exception) {
                // dial() discards its own failed/partial handshakes. A request
                // that failed before its first write leaves a ready socket intact.
                if (state.sent.get() && (error !is FlameDBException || error.kind != FlameDBErrorKind.SERVER_REJECTION)) invalidate()
                throw error
            }
        }
    }
}
