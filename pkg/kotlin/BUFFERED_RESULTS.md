# Buffered Kotlin SDK results — 2026-10-12

All 55 regression tests passed: 34 existing transport tests and 21 SDK-managed buffering tests. Both stress tests passed. Verification uses local TCP servers and real SDK sockets; it performs no database/storage-engine work. No SDK version was published and no EraMC files were changed by this SDK task.

The regression cases cover minimal/default factory usage without per-event coroutines, value/tag/board/quorum/timestamp snapshots, direct write acknowledgement compatibility, journal capacity/reuse, persisted audit capacity, exclusive ownership, offline/auth/restart recovery, lost/truncated/oversized/slow/inconsistent/partial ACKs without replay, controlled callbacks, cancellation/close and process restart. Added checks also cover independent flushing during blocked socket I/O, strict UTF-8 even with a valid CRC, bounded cold initialization read queues and deadlines, cancellation capacity release, and initialization racing shutdown without leaked sockets. Invalid configurations fail before claiming a directory.

One existing admission test used a 100 ms initial auth budget for an unrelated decoder/pool-wait oracle and failed during setup on the loaded host. That test now uses 500 ms while retaining all assertions: active request timeout, queued POOL_WAIT_TIMEOUT/NOT_SENT, capacity reuse during worker cleanup and no queued write transmission. Production timeout defaults remain 5000 ms.

Commands: `gradlew.bat test --no-daemon --console=plain` and `gradlew.bat sdkStressTest --no-daemon --console=plain`. BUFFERED_GATES.md records automatic command evidence and manual API/document review.

## Final measured stress output
```text
BURST events=100000 producers=64 admissionMs=3177 p50Us=30 p99Us=3167 RPCs=782 maxConcurrentWrites=1 preserved=100000 backlogFiles=781 backlogHeapDeltaBytes=446992
BUFFERED_SDK_STRESS_VERIFIED events=100000 producers=64

SDK_STRESS_VERIFIED requests=100000 callers=10000 elapsedMs=4195 opsPerSec=23838 p50Micros=4404 p99Micros=30470 queueP99Micros=30054 requestP99Micros=1081 heapBeforeMiB=8 heapBeforeGcMiB=21 heapAfterGcMiB=8 overloadMs=307 rejected=9936 queued=64

```

## Recovery and known counterexample output

```text
NO_REPLAY mode=DROP original=8 healthy=2 auditFiles=1
RECOVERY offline/restart/auth-failure=501 events confirmed exactly once; closed admission rejected
NO_REPLAY mode=BAD_ACK original=8 healthy=2 auditFiles=1
NO_REPLAY mode=TRUNCATED original=8 healthy=2 auditFiles=1
LIVE_RESTART confirmed=16 uncertain=8 duplicateWrites=0 readPoolRecovered=true
NO_REPLAY mode=TIMEOUT original=8 healthy=2 auditFiles=1
CONFIRMATION bounded=2 deadline-expired=500 events retained; capacity reused
SHUTDOWN unsent records recovered; inflight quarantined; truncated UTF-8 journal prefix recovered
NO_REPLAY mode=PARTIAL original=8 healthy=2 auditFiles=1
NO_REPLAY mode=OVERSIZE original=8 healthy=2 auditFiles=1
NEGATIVE_CONTROL legacy per-event coroutines: errors=989/1000 detected overload as expected
```
## Limits of these measurements

Admission time is synchronous local journal I/O, serialization and contention across 64 producers, not server execution time. Native TCP RPC count demonstrates batching, and exact request comparison verifies preservation rather than database durability. The burst fixture intentionally pauses the first write and uses a 60-second test timeout for that artificial hold; SDK defaults remain 5000 ms. It checks one active write and successful reserved reads while writes are blocked. The mock adds a small per-batch response delay.

Post-GC backlog heap delta is approximate process accounting with test arrays/class initialization and does not measure allocations, RSS, OS page cache or a long-term soak. Pending/audit storage is bounded by admission budget; default limits cannot guarantee capacity through an unlimited outage. Raw SDK stress before-GC heap includes transient test/client allocations and is not a product heap ceiling. File-system stalls can delay admission/sealing. Local OS-cache admission is not an fsync or remote ACK per event; power-loss and filesystem metadata durability remain separate limitations.

UNKNOWN/partial outcomes require manual reconciliation, not automatic retries. Counters on restart do not constitute exactly-once proof. The successful and negative controls cannot prove production ingestion throughput, real query speed, replication durability, or Minecraft tick latency. Timing varies substantially with JVM warmup, host load and local storage. See BUFFERED_WRITES.md for API and acknowledgement semantics.