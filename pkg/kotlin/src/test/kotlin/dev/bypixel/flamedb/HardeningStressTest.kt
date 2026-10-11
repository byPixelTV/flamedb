package dev.bypixel.flamedb

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicIntegerArray
import kotlinx.coroutines.*
import kotlin.math.roundToLong
import kotlin.test.*

class HardeningStressTest {
    @Test fun mixedRequestsAndHighParallelOverloadStayBoundedAndCorrect() = runBlocking {
        val count = 100_000
        val next = AtomicInteger()
        val seen = AtomicIntegerArray(count)
        val timings = AtomicInteger()
        val callNanos = LongArray(count)
        val queueNanos = LongArray(count)
        val requestNanos = LongArray(count)
        fun heapMiB(): Long = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024 * 1024)
        System.gc()
        val heapBefore = heapMiB()
        val started = System.nanoTime()
        TcpFixture(recordCommands = false) { socket, line, _ ->
            val id = line.split(' ')[1].substringAfter(':').toInt()
            check(seen.incrementAndGet(id) == 1) { "replayed request $id" }
            socket.reply(if (line.startsWith("LEADERBOARD"))
                """{"leaderboard":[{"entity_id":"m:$id","value":$id}]}""" else "{}")
        }.use { server ->
            FlameDB.connect(server.config(10_000).copy(onCommandTiming = { timing ->
                check(timing.success)
                val index = timings.getAndIncrement()
                queueNanos[index] = timing.queueWaitNanos
                requestNanos[index] = timing.requestNanos
            })).use { db ->
                withTimeout(120_000) {
                    (0 until 128).map { async(Dispatchers.Default) {
                        while (true) {
                            val id = next.getAndIncrement()
                            if (id >= count) break
                            val callStarted = System.nanoTime()
                            if (id % 4 == 0) db.write("m:$id", id.toDouble())
                            else {
                                val row = db.leaderboard("m:$id").single()
                                check(row.entityId == "m:$id" && row.score == id.toDouble()) { "wrong response for $id: $row" }
                            }
                            callNanos[id] = System.nanoTime() - callStarted
                        }
                    } }.awaitAll()
                }
                assertEquals(3, server.connections.get())
                assertEquals(count, timings.get())
                repeat(count) { assertEquals(1, seen.get(it), "request $it missing or replayed") }
            }
        }
        val elapsedMs = (System.nanoTime() - started) / 1_000_000
        val heapBeforeGC = heapMiB()
        System.gc()
        val heapAfterGC = heapMiB()
        fun percentile(values: LongArray, p: Double): Long {
            values.sort()
            return values[((values.size - 1) * p).toInt()] / 1000
        }
        val latency50 = percentile(callNanos, .50)
        val latency99 = percentile(callNanos, .99)
        val queue99 = percentile(queueNanos, .99)
        val request99 = percentile(requestNanos, .99)

        val held = CountDownLatch(1)
        val release = CountDownLatch(1)
        val overloadStarted = System.nanoTime()
        TcpFixture { socket, line, _ ->
            if (line.contains("held")) { held.countDown(); check(release.await(30, TimeUnit.SECONDS)) }
            socket.reply(if (line.startsWith("LEADERBOARD")) """{"leaderboard":[]}""" else "{}")
        }.use { server ->
            FlameDB.connect(server.config(30_000).copy(maxQueuedWrites = 64, maxQueuedReads = 16)).use { db ->
                val active = async { db.write("held", 1.0) }
                try {
                    held.awaitReceived()
                    val calls = (0 until 10_000).map { id -> async(start = CoroutineStart.UNDISPATCHED) {
                        runCatching { db.write("waiting:$id", 1.0) }
                    } }
                    val waiting = calls.filter { !it.isCompleted }
                    assertEquals(64, waiting.size)
                    val rejected = calls.filter { it.isCompleted }.map { it.await().exceptionOrNull() }
                    assertEquals(9936, rejected.size)
                    rejected.forEach {
                        val failure = assertIs<FlameDBException>(it)
                        assertEquals(FlameDBErrorKind.OVERLOADED, failure.kind)
                        assertEquals(ExecutionStatus.NOT_SENT, failure.executionStatus)
                    }
                    // Reads still proceed after 10,000 write callers hit the client.
                    withTimeout(2000) { repeat(20) { db.leaderboard("probe:$it") } }
                    assertFalse(active.isCompleted)
                    waiting.take(32).forEach { it.cancelAndJoin() }
                    val replacements = (0 until 32).map { id -> async(start = CoroutineStart.UNDISPATCHED) {
                        db.write("replacement:$id", 1.0)
                    } }
                    assertTrue(replacements.none { it.isCompleted }, "cancelled callers retained waiting capacity")
                    assertEquals(FlameDBErrorKind.OVERLOADED, assertFailsWith<FlameDBException> { db.write("overflow", 1.0) }.kind)
                    release.countDown()
                    active.await()
                    waiting.drop(32).forEach { assertTrue(it.await().isSuccess) }
                    replacements.awaitAll()
                    val writes = server.commands.filter { it.startsWith("WRITE ") }
                    assertEquals(65, writes.size)
                    assertEquals(65, writes.toSet().size)
                    assertTrue(writes.none { it.startsWith("WRITE waiting:") && it.split(' ')[1].substringAfter(':').toInt() < 32 })
                } finally { release.countDown() }
            }
        }
        val overloadMs = (System.nanoTime() - overloadStarted) / 1_000_000
        println("SDK_STRESS_VERIFIED requests=100000 callers=10000 elapsedMs=$elapsedMs opsPerSec=${(count * 1000.0 / elapsedMs).roundToLong()} " +
            "p50Micros=$latency50 p99Micros=$latency99 queueP99Micros=$queue99 requestP99Micros=$request99 " +
            "heapBeforeMiB=$heapBefore heapBeforeGcMiB=$heapBeforeGC heapAfterGcMiB=$heapAfterGC overloadMs=$overloadMs rejected=9936 queued=64")
    }
}
