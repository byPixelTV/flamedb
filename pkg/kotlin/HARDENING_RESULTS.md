# Kotlin SDK hardening verification — 2026-10-11

All checks ran locally on Windows with the project's JVM toolchain 21, Kotlin 2.4.20, kotlinx-coroutines 1.11.0 and kotlinx-serialization-json 1.11.0. The fixtures use real loopback TCP sockets, newline framing and authentication, but **do not run FlameDB, Pebble, query execution or replication**. Client and fixture share one test JVM.

## Final verified run

```text
gradlew.bat test --no-daemon --console=plain
gradlew.bat sdkStressTest --no-daemon --console=plain
```

The regression run passed **32 tests**, including all 11 previously existing tests. The separate stress task passed its one combined mixed-traffic/overload test. The commands, expectations and execution fingerprints are recorded in `HARDENING_GATES.md`.

| Mixed-traffic measurement | Observed |
|---|---:|
| Protocol requests | 100,000: 25,000 writes / 75,000 leaderboard reads |
| Concurrent workers | 128, sharing one client |
| Connections | 2 reserved reads + 1 write, 3 authenticated sockets |
| Total fixture-run elapsed time | 3,027 ms |
| Observed throughput | 33,036 requests/s |
| Complete SDK-call p50 / p99 | 3.380 ms / 23.562 ms |
| Queue wait p99 | 23.157 ms |
| Request work p99 | 0.467 ms |
| Missing, repeated or misassigned requests | 0 |
| Failed requests | 0 |

Elapsed time includes fixture setup, handshake, the request loop, exact-once checks and teardown; throughput uses that whole interval. Percentiles are calculated from 100,000 measured calls/timing records. Request work includes lazy authentication and typed result conversion. These are observations from this machine/run, not guaranteed performance or a comparison with an earlier database benchmark.

The overload probe used **10,000 additional write callers** while a separate active write was held by a server latch. With `maxQueuedWrites=64`, exactly **64 waited and 9,936 failed with `OVERLOADED`/`NOT_SENT`**. Twenty reads completed while the write remained blocked. Cancelling 32 queued callers admitted 32 replacements; one further caller was rejected. After release, exactly 65 distinct writes reached the fixture (the held write plus 64 queued/replacement writes), with no cancelled/rejected write sent and no replay. The whole probe took **273 ms**, including startup, reads, cancellation, replacement writes and teardown; this is not an isolated overload-rejection latency.

JVM used-heap snapshots for the mixed-request phase were **7 MiB after an initial GC**, **26 MiB before final GC**, and **8 MiB after final GC**. Values are rounded down to MiB and cover the entire test JVM, including fixtures, loaded classes, coroutine runtime, verification arrays and client. `System.gc()` is a hint; these snapshots are not a peak heap/RSS measurement, an SDK-only allocation profile, or proof of a long-term production RAM slope.

## Regression coverage and counterexample

- Independent read/write queue limits, zero-wait mode, 1,000-caller overflow, native queued cancellation and reuse of freed reservations.
- Pool-wait deadline while an already-timed-out worker still holds a lease during decoder cleanup; lease reuse is delayed until that worker exits.
- Both full queues and active requests wake on `close()`, including leased operations whose IO worker has not started because its dispatcher is occupied, with `NOT_SENT` versus sent mutation/read status preserved.
- Reads during blocked writes, bounded read queues while writes progress, and response identity checks under parallel traffic.
- Actual TCP listener restart on the same port, lost acknowledgements, failed initial/reconnect authentication, cause preservation, next-call reauthentication and no write replay.
- Auth challenge and auth reply byte limits; oversized unterminated command replies are rejected before the server sends LF or closes.
- Exact byte boundaries, CRLF, split multibyte UTF-8, malformed/incomplete UTF-8, malformed JSON/object/result fields and EOF before LF, even after otherwise complete JSON.
- Slow trickle responses cannot refresh the total deadline. Invalid replies discard the affected connection; ordinary server rejections retain healthy connections.
- Native active cancellation and per-command timing failure details; reused cancellation causes retain at most one SDK diagnostic rather than accumulating a history.
- Bulk-deletion result compatibility: existing error text plus structured per-metric mutation uncertainty.

`ResponseTest.knownUnsafeReadLineCounterexampleFailsTheSameLimitOracle` runs the **same oversize-rejection oracle** against two local TCP reads. The former `BufferedReader.readLine()` behavior accepts the oversized frame and makes the oracle fail with an assertion, which the counterexample test explicitly expects. `ResponseReader` rejects it with `RESPONSE_TOO_LARGE`, so that oracle passes. This proves the size check detects a known unsafe implementation rather than passing vacuously. Separate tests withhold the remainder/LF of a response to verify early, incremental enforcement.

## Limits and compatibility

This run uses small synthetic results and a short loopback workload. It does not reproduce the previously reported million-request run, benchmark the real database, prove server throughput, measure production network latency, or establish long-term RAM behavior. Bounds on waiting reservations, response accumulation and active sockets are enforced in code and exercised in tests; application-created coroutines, retained results, callback sinks and decoded JSON objects remain part of the application's memory budget.

New finite defaults intentionally reject bursts beyond admitted capacity and responses above 8 MiB; strict LF/UTF-8 framing rejects inputs previously tolerated by `readLine()`. Added data-class fields change generated JVM signatures, so consumers must recompile. Existing `FlameDBException` catches and its two old constructors are retained. Full configuration/error/outcome semantics are in `README.md`.

Only `pkg/kotlin` was changed. No version was published and no automatic write-batch queue was added.
