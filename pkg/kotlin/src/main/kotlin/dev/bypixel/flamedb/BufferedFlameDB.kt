package dev.bypixel.flamedb

import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.io.*
import java.nio.channels.FileChannel
import java.nio.file.*
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.Logger
import java.util.zip.CRC32
import java.nio.ByteBuffer

data class BufferedWriteConfig(
    val batchSize: Int = 128,
    val flushMs: Long = 100,
    val retryMs: Long = 1000,
    val confirmationMs: Long = 5000,
    val maxConfirmations: Int = 128,
    val shutdownMs: Long = 5000,
    val maxJournalBytes: Long = 256L * 1024 * 1024,
) {
    init {
        require(batchSize in 1..512 && flushMs in 1..60_000 && retryMs in 1..60_000)
        require(confirmationMs in 1..3_600_000 && maxConfirmations in 1..4096 && shutdownMs in 0..3_600_000)
        require(maxJournalBytes > 0)
    }
}

data class BufferedWriteStatistics(
    val admitted: Long,
    val confirmed: Long,
    val uncertain: Long,
    val rejected: Long,
    val retainedJournalBytes: Long,
)

/** One original event in a retained batch. Its ID belongs to the local journal,
 * not to a server-side deduplication protocol. */
data class BufferedWriteRecord(val id: String, val item: WriteBatchItem)

@Serializable
internal data class JournalWrite(
    val id: String,
    val metric: String,
    val value: Double,
    val leaderboardEntity: String?,
    val tags: Map<String, String>,
    val timestampNs: Long,
    val quorum: Boolean,
    val leaderboardEntities: Map<String, String>,
) {
    fun item() = WriteBatchItem(metric, value, WriteOptions(leaderboardEntity, tags, timestampNs, quorum, leaderboardEntities))
}

/** Bounded batches in RAM; unsent backlog resides on disk. One writer owns the SDK.
 * record() acknowledges journal admission, never remote execution. */
class BufferedFlameDB internal constructor(
    private val config: FlameDBConfig,
    directory: Path,
    private val ingestion: BufferedWriteConfig = BufferedWriteConfig(),
    private val logger: Logger = Logger.getLogger("dev.bypixel.flamedb.buffered"),
) : AutoCloseable {
    init {
        require(config.timeoutMs > 0 && config.readConnections in 1..1024 && config.writeConnections in 1..1024)
        require(config.maxQueuedReads >= 0 && config.maxQueuedWrites >= 0 && config.maxResponseBytes > 0)
        require(config.apiKey.none { it in "\n\r\u0000\u001f" }) { "invalid API key" }
        require(config.port in 1..65535) { "invalid port" }
    }
    private val journal = WriteJournal(directory, ingestion.batchSize, ingestion.maxJournalBytes)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val connectLock = Mutex()
    private val initializationWaiters = AtomicInteger()
    @Volatile private var client: FlameDB? = null
    @Volatile private var stopping = false
    private val callbacks = ConcurrentHashMap<String, CompletableDeferred<Boolean>>()
    private val callbackSlots = kotlinx.coroutines.sync.Semaphore(ingestion.maxConfirmations)
    private val timestamp = AtomicLong()
    @Volatile var lastFailure: FlameDBException? = null
        private set
    private val nextWarning = AtomicLong(Long.MIN_VALUE)
    private val admitted = AtomicLong()
    private val confirmed = AtomicLong()
    private val uncertain = AtomicLong()
    private val rejected = AtomicLong()
    private val shutdownLock = Mutex()
    private var shutdownComplete = false
    private val sender = scope.launch { sendLoop() }
    private val flusher = scope.launch {
        try {
            while (currentCoroutineContext().isActive) {
                journal.flushIfDue(ingestion.flushMs)
                delay(minOf(ingestion.flushMs, 100))
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            lastFailure = FlameDBException("buffered journal flush failed", FlameDBErrorKind.REQUEST_FAILED, cause = e)
            runCatching { journal.stopAdmission() }
            sender.cancel()
            logger.severe("FlameDB journal flusher stopped (${e.javaClass.simpleName}); journal retained at ${journal.directory}")
        }
    }
    init {
        if (journal.auditFilesAtStartup > 0) logger.warning("FlameDB journal contains ${journal.auditFilesAtStartup} audit files; manual reconciliation required at $directory; no automatic replay")
    }

    private fun warn(reason: String) = warn { reason }

    private fun warn(reason: () -> String) {
        val now = System.nanoTime()
        val previous = nextWarning.get()
        if (now >= previous && nextWarning.compareAndSet(previous, now + 10_000_000_000L)) {
            logger.warning("FlameDB buffered writes: ${reason()}; admitted=${admitted.get()}, confirmed=${confirmed.get()}, uncertain=${uncertain.get()}, rejected=${rejected.get()}; journal=${journal.directory}")
        }
    }

    private suspend fun sdk(allowDuringShutdown: Boolean = false): FlameDB {
        fun checkOpen() {
            if (stopping && !allowDuringShutdown) throw FlameDBException("buffered client is closed",
                FlameDBErrorKind.CLIENT_CLOSED, phase = FlameDBPhase.POOL_WAIT)
        }
        checkOpen()
        client?.let { return it }
        val ownsLock = connectLock.tryLock()
        var initializing = ownsLock
        var reserved = false
        if (!ownsLock && !allowDuringShutdown) {
            while (true) {
                val waiting = initializationWaiters.get()
                if (waiting >= config.maxQueuedReads) throw FlameDBException("buffered initialization read queue full",
                    FlameDBErrorKind.OVERLOADED, phase = FlameDBPhase.POOL_WAIT)
                if (initializationWaiters.compareAndSet(waiting, waiting + 1)) { reserved = true; break }
            }
        }
        suspend fun open(): FlameDB {
            checkOpen()
            return client ?: FlameDB.connect(config).also { opened ->
                try { checkOpen() } catch (e: Exception) { opened.close(); throw e }
                client = opened
            }
        }
        // Bound both time and retained waiters during cold/failing initialization.
        try {
            return withTimeout(config.timeoutMs.toLong()) {
                if (ownsLock) open() else connectLock.withLock { initializing = true; open() }
            }
        } catch (error: TimeoutCancellationException) {
            currentCoroutineContext().ensureActive() // Preserve an enclosing caller's cancellation.
            val diagnostic = error.flameDBFailure()
            val phase = if (!initializing) FlameDBPhase.POOL_WAIT else diagnostic?.phase ?: FlameDBPhase.CONNECT
            val kind = when (phase) {
                FlameDBPhase.POOL_WAIT -> FlameDBErrorKind.POOL_WAIT_TIMEOUT
                FlameDBPhase.AUTHENTICATE -> FlameDBErrorKind.AUTHENTICATION
                else -> FlameDBErrorKind.CONNECTION
            }
            throw FlameDBException("buffered client initialization deadline exceeded", kind, phase = phase, cause = error)
        } finally {
            if (ownsLock) connectLock.unlock()
            if (reserved) initializationWaiters.decrementAndGet()
        }
    }

    suspend fun getSum(vararg metrics: String, options: GetOptions = GetOptions()): GetResult =
        sdk().getSum(*metrics, options = options)
    suspend fun leaderboard(metric: String, options: LeaderboardOptions = LeaderboardOptions()): List<LeaderboardEntry> =
        sdk().leaderboard(metric, options)
    suspend fun groupLeaderboard(metric: String, groups: List<GroupDef>, options: LeaderboardOptions = LeaderboardOptions()): List<GroupLeaderboardEntry> =
        sdk().groupLeaderboard(metric, groups, options)

    /** Per-instance counters; confirmations can include recovered events from an older instance. */
    fun statistics() = BufferedWriteStatistics(admitted.get(), confirmed.get(), uncertain.get(), rejected.get(), journal.retainedBytes())

    /** Bounded, read-only audit inspection, including after shutdown.
     * These helpers perform local file I/O; inspect a stopped client for a stable view. */
    fun auditFiles(limit: Int = 100): List<Path> = journal.auditFiles(limit)

    fun inspectAudit(file: Path): List<BufferedWriteRecord> =
        journal.inspectAudit(file).map { BufferedWriteRecord(it.id, it.item()) }

    /** Access the owned direct client for additional SDK operations. Do not close it separately.
     * An admitted buffered record may still be pending; this does not flush buffered writes. */
    suspend fun directClient(): FlameDB = sdk()

    fun record(metric: String, value: Double, options: WriteOptions = WriteOptions()): Boolean =
        append(metric, value, options, null)

    /** The event is admitted synchronously even when Confirmation capacity is full.
     * Confirmation waits are bounded; expiring/cancelling a confirmation callback never resubmits a write. */
    fun recordThen(metric: String, value: Double, options: WriteOptions = WriteOptions(), onConfirmed: suspend () -> Unit): Boolean {
        val deferred = if (callbackSlots.tryAcquire()) CompletableDeferred<Boolean>() else null
        if (deferred == null) warn("Confirmation limit reached; event still journaled")
        if (!append(metric, value, options, deferred)) {
            if (deferred != null) callbackSlots.release()
            return false
        }
        if (deferred != null) scope.launch {
            try {
                if (withTimeoutOrNull(ingestion.confirmationMs) { deferred.await() } == true) onConfirmed()
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                warn("confirmed metric confirmation callback failed (${e.flameDBFailure()?.kind ?: e.javaClass.simpleName})")
            }
        }.invokeOnCompletion {
            // Also runs if shutdown cancels the job before its body ever starts.
            callbacks.entries.removeIf { it.value === deferred }
            callbackSlots.release()
        }
        return true
    }

    private fun append(metric: String, value: Double, options: WriteOptions, confirmation: CompletableDeferred<Boolean>?): Boolean {
        var id: String? = null
        try {
            require(value.isFinite()) { "write value must be finite" }
            require(validIdentifier(metric)) { "invalid metric identifier" }
            require((options.tags.keys + options.leaderboardEntities.keys).all { key ->
                validIdentifier(key)
            }) { "invalid tag/board identifier" }
            val now = Instant.now().let { it.epochSecond * 1_000_000_000L + it.nano }
            val ts = options.timestampNs ?: timestamp.updateAndGet { maxOf(now, it + 1) }
            val entry = JournalWrite(UUID.randomUUID().toString(), metric, value, options.leaderboardEntity,
                options.tags.toMap(), ts, options.quorum, options.leaderboardEntities.toMap())
            id = entry.id
            if (confirmation != null) callbacks[entry.id] = confirmation
            journal.append(entry)
            admitted.incrementAndGet()
            return true
        } catch (e: Exception) {
            id?.let { callbacks.remove(it) }
            val failure = e.flameDBFailure() ?: FlameDBException("buffered write admission failed", FlameDBErrorKind.REQUEST_FAILED,
                command = "WRITE", mutating = true, executionStatus = ExecutionStatus.NOT_SENT, cause = e)
            lastFailure = failure
            rejected.incrementAndGet()
            warn {
                val detail = (failure.message ?: e.javaClass.simpleName)
                .replace(Regex("[\\p{Cntrl}]"), " ").take(160)
                val cause = failure.cause?.let { error ->
                "; cause=${error.javaClass.simpleName}: ${(error.message ?: "unknown").replace(Regex("[\\p{Cntrl}]"), " ").take(160)}"
                } ?: ""
                "local admission failed (kind=${failure.kind}, status=${failure.executionStatus}, reason=$detail$cause); " +
                    "journal-bytes=${journal.retainedBytes()}, max-journal-bytes=${ingestion.maxJournalBytes}; event NOT acknowledged"
            }
            return false
        }
    }

    private fun validIdentifier(value: String) = value.isNotEmpty() && value.none {
        it.isWhitespace() || it == '\u0000' || it == '\u001f' || it in "=\","
    }

    private suspend fun sendLoop() {
        var backoff = ingestion.retryMs
        try {
            while (currentCoroutineContext().isActive) {
                journal.flushIfDue(ingestion.flushMs)
                val pending = journal.oldestPending()
                if (pending == null) {
                    if (stopping) break
                    delay(minOf(ingestion.flushMs, 100))
                    continue
                }
                // Authentication/initial connection failure cannot have executed this batch.
                val connection = try { sdk(allowDuringShutdown = true) } catch (e: CancellationException) { throw e } catch (e: Exception) {
                    lastFailure = e.flameDBFailure()
                    warn("connection/auth unavailable (${e.flameDBFailure()?.kind ?: e.javaClass.simpleName}); unsent batch retained")
                    delay(backoff)
                    backoff = minOf(30_000, backoff * 2)
                    continue
                }
                val items = try { journal.read(pending) } catch (e: Exception) {
                    journal.move(pending, "damaged")
                    warn("damaged journal batch retained for manual recovery")
                    continue
                }
                val inflight = journal.move(pending, "inflight")
                try {
                    val result = connection.writeBatch(items.map { it.item() })
                    val errors = result.errors.map { it.index }.toSet()
                    val valid = result.accepted + result.failed == items.size &&
                        result.errors.size == result.failed && errors.size == result.failed &&
                        errors.all { it in items.indices } && result.ok == (result.failed == 0)
                    if (!valid) {
                        lastFailure = FlameDBException("inconsistent buffered batch acknowledgement", FlameDBErrorKind.INVALID_RESPONSE,
                            command = "WRITE_BATCH", mutating = true, executionStatus = ExecutionStatus.UNKNOWN)
                        journal.move(inflight, "uncertain")
                        uncertain.addAndGet(items.size.toLong())
                        items.forEach { callbacks.remove(it.id)?.complete(false) }
                        warn("inconsistent batch acknowledgement; no replay")
                    } else if (result.failed != 0) {
                        lastFailure = FlameDBException("buffered batch has ${result.failed} failed items; inspect the retained result",
                            FlameDBErrorKind.SERVER_REJECTION, command = "WRITE_BATCH", mutating = true, executionStatus = ExecutionStatus.UNKNOWN)
                        journal.saveResult(inflight, result)
                        journal.move(inflight, "partial")
                        items.forEachIndexed { index, item -> callbacks.remove(item.id)?.complete(index !in errors) }
                        confirmed.addAndGet(result.accepted.toLong())
                        uncertain.addAndGet(result.failed.toLong())
                        warn("partial batch retained; failed items require manual reconciliation, no replay")
                    } else {
                        val done = journal.move(inflight, "done")
                        confirmed.addAndGet(items.size.toLong())
                        items.forEach { callbacks.remove(it.id)?.complete(true) }
                        try { journal.deleteDone(done) } catch (e: IOException) {
                            warn("confirmed batch cleanup failed (${e.javaClass.simpleName}); done file retained, no replay")
                        }
                    }
                    backoff = ingestion.retryMs
                } catch (e: Exception) {
                    // Only an explicit SDK NOT_SENT diagnostic authorizes retry.
                    // Cancellation without a diagnostic is conservatively UNKNOWN.
                    val failure = e.flameDBFailure()
                    lastFailure = failure
                    val safe = failure?.executionStatus == ExecutionStatus.NOT_SENT
                    if (Files.exists(inflight)) journal.move(inflight, if (safe) "pending" else "uncertain")
                    if (!safe) {
                        uncertain.addAndGet(items.size.toLong())
                        items.forEach { callbacks.remove(it.id)?.complete(false) }
                    }
                    warn("batch failed (${failure?.kind ?: e.javaClass.simpleName}, status=${failure?.executionStatus ?: "UNKNOWN"}); ${if (safe) "unsent retained" else "no replay"}")
                    if (e is CancellationException) throw e
                    if (safe) {
                        delay(backoff)
                        backoff = minOf(30_000, backoff * 2)
                    }
                }
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            // Journal failures halt the sender. Admission also closes so a broken worker
            // cannot make callers believe the service is healthy.
            runCatching { journal.stopAdmission() }
            logger.severe("FlameDB buffered write sender stopped (${e.javaClass.simpleName}); journal retained at ${journal.directory}")
        }
    }

    override fun close() { runBlocking { shutdown() } }

    suspend fun shutdown() = withContext(NonCancellable) { shutdownLock.withLock {
        if (shutdownComplete) return@withLock
        stopping = true
        try {
            flusher.cancelAndJoin()
            try { journal.stopAdmission() } catch (e: Exception) {
                logger.severe("FlameDB journal could not seal during shutdown (${e.javaClass.simpleName}); audit retained")
                sender.cancel()
            }
            if (withTimeoutOrNull(ingestion.shutdownMs) { sender.join(); true } != true) sender.cancelAndJoin()
        } finally {
            withContext(NonCancellable) {
                flusher.cancelAndJoin()
                sender.cancelAndJoin()
                scope.cancel()
                // Also closes a public reader's initialization that raced shutdown.
                connectLock.withLock { client?.close() }
                callbacks.values.forEach { it.complete(false) }
                callbacks.clear()
                journal.close()
            }
        }
        shutdownComplete = true
        logger.info("FlameDB buffered write shutdown: admitted=${admitted.get()}, confirmed=${confirmed.get()}, uncertain=${uncertain.get()}, rejected=${rejected.get()}; unsent/audit files retained at ${journal.directory}")
    } }
}

/** Length + CRC framing catches incomplete UTF-8/JSON without reading an unbounded file.
 * .inflight is a durable claim; restart never automatically replays claimed batches. */
internal class WriteJournal(val directory: Path, private val batchSize: Int, private val maxJournalBytes: Long = 256L * 1024 * 1024) : AutoCloseable {
    private val json = Json
    private val lockChannel: FileChannel
    private val fileLock: java.nio.channels.FileLock
    private var stream: FileOutputStream? = null
    private var active: Path? = null
    private var count = 0
    private var bytes = 0
    private var openedAt = 0L
    private var accepting = true
    private val sequence = AtomicLong()
    private val retainedBytes = AtomicLong()
    // Sender-owned, bounded lookahead: avoid a directory rescan per batch while
    // keeping a large disk backlog out of heap. Newly sealed files sort after it.
    private val pendingCache = java.util.ArrayDeque<Path>()
    var auditFilesAtStartup = 0L
        private set

    init {
        Files.createDirectories(directory)
        lockChannel = FileChannel.open(directory.resolve("writer.lock"), StandardOpenOption.CREATE, StandardOpenOption.WRITE)
        fileLock = try { requireNotNull(lockChannel.tryLock()) { "write journal already in use" } }
        catch (e: Exception) { lockChannel.close(); throw e }
        try {
            Files.newDirectoryStream(directory).use { files ->
                for (file in files) if (Files.isRegularFile(file)) {
                    retainedBytes.addAndGet(Files.size(file))
                    file.fileName.toString().substringBefore('-').toLongOrNull()?.let { order -> sequence.updateAndGet { maxOf(it, order) } }
                }
            }
            Files.newDirectoryStream(directory).use { files ->
                for (file in files) when (file.fileName.toString().substringAfterLast('.')) {
                    "inflight" -> { move(file, "uncertain"); auditFilesAtStartup++ }
                    "open" -> {
                        // Preserve valid prefix and retain damaged original for inspection.
                        val recovered = try { read(file, allowTruncatedTail = true) } catch (e: Exception) {
                            move(file, "damaged")
                            auditFilesAtStartup++
                            continue
                        }
                        move(file, "recovered")
                        auditFilesAtStartup++
                        if (recovered.isNotEmpty()) writeRecovered(file, recovered)
                    }
                    "done" -> deleteDone(file)
                    "uncertain", "partial", "damaged", "recovered", "rebuild" -> auditFilesAtStartup++
                }
            }
        } catch (e: Exception) { fileLock.release(); lockChannel.close(); throw e }
    }

    private fun newPath(extension: String): Path {
        val now = Instant.now().let { it.epochSecond * 1_000_000_000L + it.nano }
        val order = sequence.updateAndGet { maxOf(now, it + 1) }
        return directory.resolve("${order.toString().padStart(19, '0')}-${UUID.randomUUID()}.$extension")
    }

    @Synchronized fun append(entry: JournalWrite) {
        if (!accepting) throw FlameDBException("buffered write journal closed", FlameDBErrorKind.CLIENT_CLOSED,
            command = "WRITE", mutating = true, executionStatus = ExecutionStatus.NOT_SENT, phase = FlameDBPhase.POOL_WAIT)
        val encoded = json.encodeToString(entry).toByteArray(Charsets.UTF_8)
        require(encoded.size <= MAX_ENTRY_BYTES) { "metric exceeds journal entry limit" }
        if (retainedBytes.get() > maxJournalBytes - encoded.size - 8) throw FlameDBException(
            "buffered write journal admission limit exceeded", FlameDBErrorKind.OVERLOADED,
            command = "WRITE", mutating = true, executionStatus = ExecutionStatus.NOT_SENT, phase = FlameDBPhase.POOL_WAIT,
        )
        if (count >= batchSize || (bytes > 0 && bytes + encoded.size + 8 > MAX_BATCH_BYTES)) seal()
        if (stream == null) {
            active = newPath("open")
            stream = FileOutputStream(active!!.toFile())
            openedAt = System.nanoTime()
        }
        try {
            writeFrame(stream!!, encoded)
            retainedBytes.addAndGet(encoded.size.toLong() + 8)
            count++
            bytes += encoded.size + 8
        } catch (e: Exception) {
            accepting = false
            throw e
        }
    }

    private fun writeFrame(output: OutputStream, encoded: ByteArray) {
        val frame = ByteBuffer.allocate(encoded.size + 8)
            .putInt(encoded.size).putInt(CRC32().apply { update(encoded) }.value.toInt()).put(encoded).array()
        output.write(frame)
    }

    private fun writeRecovered(original: Path, entries: List<JournalWrite>) {
        // A crash during rebuild leaves both audit files; no ambiguous automatic replay.
        val file = original.resolveSibling(original.fileName.toString().substringBeforeLast('.') + ".rebuild")
        FileOutputStream(file.toFile()).use { output ->
            entries.forEach { writeFrame(output, json.encodeToString(it).toByteArray(Charsets.UTF_8)) }
            retainedBytes.addAndGet(output.channel.size())
            output.fd.sync()
        }
        move(file, "pending")
    }

    private fun seal() {
        val output = stream ?: return
        output.fd.sync()
        output.close()
        stream = null
        move(active!!, "pending")
        active = null
        count = 0
        bytes = 0
    }

    @Synchronized fun flushIfDue(ms: Long) {
        if (stream != null && System.nanoTime() - openedAt >= ms * 1_000_000) seal()
    }
    @Synchronized fun stopAdmission() { accepting = false; seal() }
    fun oldestPending(): Path? {
        if (pendingCache.isNotEmpty()) return pendingCache.peekFirst()
        val selected = java.util.PriorityQueue<Path>(compareByDescending { it.fileName.toString() })
        Files.newDirectoryStream(directory, "*.pending").use { paths ->
            for (path in paths) {
                if (selected.size < 256) selected.add(path)
                else if (path.fileName.toString() < selected.peek().fileName.toString()) {
                    selected.poll()
                    selected.add(path)
                }
            }
        }
        selected.sortedBy { it.fileName.toString() }.forEach { pendingCache.addLast(it) }
        return pendingCache.peekFirst()
    }
    fun move(file: Path, extension: String): Path {
        val moved = Files.move(file,
            file.resolveSibling(file.fileName.toString().substringBeforeLast('.') + ".$extension"), StandardCopyOption.ATOMIC_MOVE)
        if (file.fileName.toString().endsWith(".pending")) pendingCache.remove(file)
        if (file.fileName.toString().endsWith(".inflight") && extension == "pending") pendingCache.clear()
        return moved
    }
    fun deleteDone(file: Path) {
        val size = Files.size(file)
        if (Files.deleteIfExists(file)) retainedBytes.addAndGet(-size)
    }
    fun retainedBytes(): Long = retainedBytes.get()
    fun auditFiles(limit: Int): List<Path> {
        require(limit in 1..1024)
        val selected = java.util.PriorityQueue<Path>(compareByDescending { it.fileName.toString() })
        Files.newDirectoryStream(directory).use { paths ->
            for (path in paths) if (path.fileName.toString().substringAfterLast('.') in AUDIT_EXTENSIONS) {
                if (selected.size < limit) selected.add(path)
                else if (path.fileName.toString() < selected.peek().fileName.toString()) {
                    selected.poll()
                    selected.add(path)
                }
            }
        }
        return selected.sortedBy { it.fileName.toString() }
    }
    fun inspectAudit(file: Path): List<JournalWrite> {
        val resolved = file.toRealPath()
        require(resolved.parent == directory.toRealPath()) { "audit file must belong to this journal" }
        require(resolved.fileName.toString().substringAfterLast('.') in AUDIT_EXTENSIONS) { "not an audit file" }
        return read(resolved)
    }
    fun saveResult(file: Path, result: BatchResult) {
        val target = file.resolveSibling(file.fileName.toString().substringBeforeLast('.') + ".result.json")
        FileOutputStream(target.toFile()).use { it.write(json.encodeToString(result).toByteArray(Charsets.UTF_8)); it.fd.sync(); retainedBytes.addAndGet(it.channel.size()) }
    }
    fun read(file: Path, allowTruncatedTail: Boolean = false): List<JournalWrite> {
        require(Files.size(file) <= MAX_BATCH_BYTES + MAX_ENTRY_BYTES + 8) { "oversized journal batch" }
        val result = ArrayList<JournalWrite>()
        DataInputStream(BufferedInputStream(Files.newInputStream(file))).use { input ->
            while (input.available() > 0) {
                try {
                    val size = input.readInt()
                    require(size in 1..MAX_ENTRY_BYTES) { "invalid journal frame length" }
                    val checksum = input.readInt()
                    val payload = ByteArray(size)
                    input.readFully(payload)
                    check(CRC32().apply { update(payload) }.value.toInt() == checksum) { "journal CRC mismatch" }
                    val text = Charsets.UTF_8.newDecoder()
                        .onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
                        .onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT)
                        .decode(ByteBuffer.wrap(payload)).toString()
                    result += json.decodeFromString<JournalWrite>(text)
                    require(result.size <= 512) { "too many journal entries" }
                } catch (e: EOFException) {
                    if (!allowTruncatedTail) throw e
                    break
                }
            }
        }
        return result
    }
    @Synchronized override fun close() {
        try { stopAdmission() } finally {
            try { stream?.close() } finally { fileLock.release(); lockChannel.close() }
        }
    }
    companion object {
        private const val MAX_ENTRY_BYTES = 64 * 1024
        private const val MAX_BATCH_BYTES = 512 * 1024
        private val AUDIT_EXTENSIONS = setOf("uncertain", "partial", "damaged", "recovered", "rebuild")
    }
}
