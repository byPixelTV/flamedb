# Kotlin SDK timeout follow-up — 2026-10-12

The reported application initializes the SDK with four positional arguments and `flamedb.timeout` defaulting to 5000. Thus the pools retain their defaults: **two reads and one write**, a total five-second budget per command when the config does not override it, and separate finite waiting limits. The initialization snippet does not show command mix, request rate, server latency or timing observations.

The supplied logs contain pool-wait, command, connect/reconnect and auth failures with `SocketTimeoutException: total command deadline exceeded`. The auth entries shown are deadline failures, not evidence that the API key was rejected. Pool-wait failures explicitly mean no command was sent. The command-failure messages alone do not prove whether a mutation was applied. Some connect/auth paths are reached with an already-exhausted overall budget; pool admission and dispatcher delay can consume that budget before meaningful socket work.

## Reproduced SDK defect and fix

Two local TCP regressions failed against the previous code:

1. A command acquired an authenticated connection, but its IO worker was held behind a blocked dispatcher. Its timeout closed the untouched connection, requiring a second TCP/auth session for the next command.
2. A worker reached execution after the deadline while the caller's coroutine timer was delayed. It entered the transport path and discarded the healthy socket without sending the command.

Both tests observed **two connections instead of the expected one**, while only the subsequent command reached the fixture. The fix tracks worker start under the same lock as cancellation, checks the actual deadline before any transport work, and preserves untouched connections. Failed/partial handshakes still invalidate themselves, and errors after a command write still discard the socket. Actual in-flight cancellation, response ownership and no-replay behavior remain intact.

An expired/cancelled dispatcher task retains its lease until it drains. Returning that lease immediately while leaving raw tasks in a blocked dispatcher would allow cancelled scheduled work to grow beyond the configured pool bounds. Shutdown still closes every connection and wakes callers, including tasks that have not started.

Pre-worker expiration now reports `REQUEST_TIMEOUT`/`NOT_SENT` and an explicit `IO dispatch timeout` message. `CONNECT` is the preparation phase in that case; it does not establish that TCP connect/auth has begun. Exception stack-trace headers include the existing `kind`, `command`, `phase` and `executionStatus` fields. No new public config/data-class fields or enum values were introduced by this follow-up; exception message getters and constructor signatures are preserved.

## Application diagnostics and handling

`onCommandTiming` distinguishes pool admission (`queueWaitNanos`) from all work after admission (`requestNanos`). The latter includes IO dispatcher waiting, auth, network/server time and decoding; it is not isolated database execution time. Use bounded counters/histograms in production. For a temporary sampled diagnostic, the application's existing connection setup can add:

```kotlin
val lastDiagnostic = java.util.concurrent.atomic.AtomicLong(System.nanoTime() - 5_000_000_000L)
val diagnosticLogger = java.util.logging.Logger.getLogger("FlameDB")

val db = FlameDB.connect(FlameDBConfig(
    host = flamedbHost,
    port = flamedbPort,
    apiKey = flamedbApiKey,
    timeoutMs = flamedbTimeoutMs,
    onCommandTiming = { timing ->
        if (!timing.success) {
            val now = System.nanoTime()
            val previous = lastDiagnostic.get()
            if (now - previous >= 5_000_000_000L && lastDiagnostic.compareAndSet(previous, now)) {
                diagnosticLogger.warning(
                    "FlameDB ${timing.command} read=${timing.read} " +
                    "queueMs=${timing.queueWaitNanos / 1_000_000.0} " +
                    "requestMs=${timing.requestNanos / 1_000_000.0} failure=${timing.failure}"
                )
            }
        }
    },
))
```

This samples at most one failed-call diagnostic per five seconds and does not print the API key or command payload. The callback observes failures; it **does not catch** the exception for the caller. Handle each SDK operation at its application coroutine boundary and rethrow native cancellation. The supplied `StandaloneCoroutine{Cancelling}` traces show uncaught operation failures at `launch` boundaries; a non-supervised parent can also cancel siblings. Handle/reconcile failed writes rather than treating logged exceptions as successful writes. Do not automatically repeat `UNKNOWN` mutations.

The default single write connection processes one complete round trip at a time. A producer faster than that service rate will accumulate waiting commands even with a healthy server. Increasing only queue length retains more waits; it does not increase throughput. More write connections can help independent operations, but concurrent SET/DELETE/increment ordering and database capacity must be evaluated before choosing a value. This change does not silently raise pool sizes, extend deadlines, retry writes or introduce automatic batching.

## Verification and limits

`gradlew.bat test --no-daemon --console=plain` passed **34 regression tests**, including both previously failing counterexamples. `gradlew.bat sdkStressTest --no-daemon --console=plain` passed 100,000 mixed requests with 128 workers, three authenticated connections, and no missing, repeated or misassigned requests. This follow-up run took **3216 ms** (31,095 requests/s), with complete-call p99 **23.121 ms**, queue p99 **22.636 ms** and request-work p99 **0.522 ms**. The 10,000-caller overload probe retained exactly **64 waits and 9936 early rejections**, released cancellation capacity and allowed reads during the held write. Whole-probe elapsed time was **337 ms**.

Used-heap snapshots were **7 MiB after initial GC**, **10 MiB before final GC**, and **8 MiB after final GC**, rounded down and covering the fixture/client/runtime together. These are short local-loopback observations, not a database benchmark, allocation/RSS profile or long-term production RAM guarantee.

The unnecessary reconnect defect is reproduced and fixed. The logs alone do **not** prove it caused every production timeout: command rate/mix, configured timeout override, queue/request timings, application dispatcher load and database/network latency remain unknown. No deployment or version publication was performed.
