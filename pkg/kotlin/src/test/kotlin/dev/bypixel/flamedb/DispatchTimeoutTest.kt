package dev.bypixel.flamedb

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.*
import kotlinx.serialization.json.Json
import kotlin.concurrent.thread
import kotlin.test.*

class DispatchTimeoutTest {
    @Test fun timeoutBeforeIoStartsKeepsHealthySocketWithoutSendingOrReplay() = runBlocking {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val dispatcher = Dispatchers.IO.limitedParallelism(1)
        TcpFixture { socket, _, _ -> socket.reply("{}") }.use { server ->
            val pool = CommandPool(server.config(200), Json, dispatcher)
            pool.connect()
            val blocker = async(dispatcher) { entered.countDown(); release.await(5, TimeUnit.SECONDS) }
            entered.awaitReceived()
            try {
                val error = assertFailsWith<FlameDBException> { pool.command(listOf("WRITE neverSent 1")) }
                assertEquals(ExecutionStatus.NOT_SENT, error.executionStatus)
                assertEquals(FlameDBErrorKind.REQUEST_TIMEOUT, error.kind)
                assertTrue(error.message!!.contains("IO dispatch timeout"))
                assertTrue(server.commands.isEmpty())
                release.countDown(); blocker.await()
                pool.command(listOf("WRITE after 1"))
                assertEquals(1, server.connections.get(), "a never-started command discarded a healthy authenticated socket")
                assertEquals(listOf("WRITE after 1"), server.commands.toList())
            } finally { release.countDown(); blocker.await(); pool.close() }
        }
    }

    @Test fun expiredWorkerWithDelayedTimerDoesNotStartIoOrDiscardHealthySocket() = runBlocking {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val dispatcher = Dispatchers.IO.limitedParallelism(1)
        TcpFixture { socket, _, _ -> socket.reply("{}") }.use { server ->
            val pool = CommandPool(server.config(200), Json, dispatcher)
            pool.connect()
            val blocker = async(dispatcher) { entered.countDown(); release.await(5, TimeUnit.SECONDS) }
            entered.awaitReceived()
            val call = async(start = CoroutineStart.UNDISPATCHED) { runCatching { pool.command(listOf("WRITE expired 1")) } }
            // Occupy the caller's event-loop thread so its coroutine timeout
            // cannot run; independently release the IO worker after the deadline.
            val timer = thread(isDaemon = true) { Thread.sleep(300); release.countDown() }
            try {
                Thread.sleep(500)
                val error = assertIs<FlameDBException>(call.await().exceptionOrNull())
                assertEquals(ExecutionStatus.NOT_SENT, error.executionStatus)
                assertEquals(FlameDBErrorKind.REQUEST_TIMEOUT, error.kind)
                assertTrue(error.toString().contains("command=WRITE"))
                assertTrue(error.toString().contains("executionStatus=NOT_SENT"))
                blocker.await()
                pool.command(listOf("WRITE after 1"))
                assertEquals(1, server.connections.get(), "an expired worker discarded a healthy socket before sending")
                assertEquals(listOf("WRITE after 1"), server.commands.toList())
            } finally { release.countDown(); blocker.await(); timer.join(1000); pool.close() }
        }
    }
}
