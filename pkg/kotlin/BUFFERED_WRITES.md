# SDK-managed buffered writes

`FlameDB.buffered(config, journalDirectory)` creates a `BufferedFlameDB` synchronously,
including when the database is unavailable. It owns its workers and lazily creates one
normal pooled FlameDB client. It has no Bukkit, EraMC or other application dependency.
The dedicated directory belongs to exactly one writer, protected by a file lock.
Use a local writable disk; admission includes local I/O and can stall on slow storage.

The minimal example is in [README.md](README.md). No publishing or dependency update
has been performed; consumers of the currently published artifact cannot call this
new API until a later authorized release.

## Acknowledgement and errors

- `record(metric, value, options): Boolean` snapshots values, tags, boards and options,
  assigning a locally unique nanosecond admission timestamp only if none was supplied.
  It writes one framed journal record. `true` acknowledges bytes written to the OS
  cache, **not remote application or power-loss durability**. `false` is an explicit
  local rejection; `lastFailure` preserves one structured cause and counters include
  rejected. Errors are logged with a rate limit; no per-event application coroutine
  is required. Invalid arguments, closed admission, full budget and storage errors
  are distinguishable through the cause/kind. Do not automatically resubmit a local
  I/O failure: a failed append can leave bytes needing manual recovery.
- Native WRITE_BATCH sends each original event separately; it does not sum values,
  change tags or combine signed transactions into a new event. Server batch results
  must have consistent totals and unique, in-range per-item error indexes before
  confirmations are delivered. Batch success is not an atomic transaction.
- Only an explicit SDK `NOT_SENT` failure permits automatic retry. Before the first
  batch is claimed, connection/auth failure also proves that its events are unsent.
  These retries back off from retryMs to at most 30 seconds. SDK request timeouts and
  response limits continue to apply to each connection/protocol response.
- UNKNOWN, missing/truncated/oversized/malformed/inconsistent acknowledgements, server
  rejection and partial failures never authorize replay. Payloads remain available
  for manual reconciliation. Healthy subsequent batches can proceed. Validated partial
  errors can mean a write applied but failed quorum; accepted and failed indexes must
  not be blindly replayed together.
- `statistics()` is a bounded immutable snapshot of admitted, confirmed, uncertain,
  rejected and retainedJournalBytes. Counters describe this instance's work; confirmed
  can include recovered events admitted by an older instance. Retained bytes include
  pending and audit material. No timing/error history is accumulated. `lastFailure`
  remains the last diagnostic even after recovery; it is not a per-record receipt.

Direct `FlameDB.connect().write()` keeps its confirmed, suspending semantics. Buffering
is an explicit one-call SDK mode; it does not silently weaken existing calls or create
unexpected files for existing clients. SET/DELETE/direct writes are not automatically
batched or ordered against pending records. Await direct writes for dependent direct
reads. Buffered reads are eventually consistent with admitted records.

## Defaults and capacity

| BufferedWriteConfig field | Default | Meaning |
| --- | --- | --- |
| batchSize | 128 | Original events per batch, configurable 1..512 |
| flushMs | 100 | Independent partial-segment seal interval, configurable 1..60000 ms |
| retryMs | 1000 | Initial safe-retry backoff, configurable 1..60000 ms |
| confirmationMs | 5000 | Optional confirmation/callback wait budget |
| maxConfirmations | 128 | Reserved best-effort callback jobs, configurable 1..4096 |
| shutdownMs | 5000 | Sender drain budget, zero cancels immediately |
| maxJournalBytes | 268435456 | Admission budget including retained pending/audit bytes |

The batch payload is bounded to 512 KiB and an individual journal record to 64 KiB.
Only 256 pending paths are cached; only one batch is being sent. Callback jobs are
separately bounded. Events waiting for delivery live on disk, not in a RAM queue.
The normal SDK keeps its two read connections and one write connection, bounded waits,
strict response decoding, response byte limits and total per-command deadlines. This
mode requires at least one reserved read connection. Raising writeConnections alone
does not increase the buffered sender's concurrency. Initial connection/auth lock
waiting is bounded by timeoutMs and maxQueuedReads, so parallel cold reads cannot
queue for minutes or bypass the read-admission memory limit. Cancellation and timeout
release those reservations. Shutdown also closes initialization racing a public read.

The journal budget is an **admission** limit. It rejects before adding a new event and
does not evict old or uncertain data. Successful ACK cleanup restores capacity; audit
data continues to consume capacity across restart. Recovery copies and partial-result
metadata can exceed the limit conservatively instead of discarding already admitted
data. Monitor disk free space as well as this budget. Reconcile/clean audit data only
with the writer stopped, then reopen so accounting reflects operator changes.

Admission warnings include the structured failure kind/status, reason, current
`journal-bytes` and configured `max-journal-bytes`. For example, an existing
0.99 GiB journal exceeds the default 256 MiB budget even after a fresh SDK start.
Restart does not remove audit files or reset their disk usage. With adequate free
disk space, an operator can explicitly increase the admission budget (for example
to 4 GiB) and reopen the writer to admit new events while preserving those files.
This supplies backlog capacity; it does not resolve uncertain outcomes or prove
sustainable server throughput. Rejected events were not admitted to the journal.

Journal append goes to the OS cache; sealing fsyncs before send. The flusher operates
independently of socket I/O/backoff. Scheduler delays, synchronous filesystem calls,
power loss and filesystem metadata durability can still exceed/affect this interval.
This is not an fsync acknowledgement per event. Process restart can recover complete
frames; incomplete tails are retained and an intact unsent prefix is rebuilt. CRC32,
frame/entry/batch bounds, strict UTF-8 and JSON detect damaged records before replay.

## Bounded optional callbacks and reads

```kotlin
db.recordThen("kills", 1.0, WriteOptions(tags = mapOf("player" to playerId))) {
    val confirmedTotal = db.getSum("kills", options = GetOptions(where = mapOf("player" to playerId)))
    // Refresh a UI from a confirmed value.
}
```

`recordThen` has the same local-admission Boolean as record.
The callback is best effort: exhausted callback capacity or a confirmation deadline
skips it while keeping the event. It is useful for UI refreshes, not as a durable
business-operation receipt. Callback failures are controlled/logged and cancellation
never resubmits events. Callback slots are released even when shutdown cancels a job
before its body starts. Keep callbacks cooperative and short.

`getSum`, `leaderboard` and `groupLeaderboard` delegate to reserved SDK connections.
`directClient()` exposes additional normal SDK operations on the owned client; do not
close it separately. It does not flush records. Read failures propagate normally rather
than fabricating zero totals. Its initialization wait and each native protocol command
have separate bounded timeout budgets; enclosing coroutine deadlines remain respected.

## Recovery, audit inspection and shutdown

| Extension | State |
| --- | --- |
| .open | Unsent partial segment; intact prefix can recover |
| .pending | Definitely unsent, eligible to send |
| .inflight | Claim before sending; restart converts it to uncertain |
| .done | Full consistent successful ACK recorded locally, cleanup only |
| .uncertain | Ambiguous execution, manual reconciliation |
| .partial | Partial ACK; full payload plus matching .result.json retained |
| .damaged / .recovered / .rebuild | Recovery/audit material, never replayed directly |

After stopping the writer, `auditFiles(limit = 100)` lists a bounded set and
`inspectAudit(file)` returns at most one bounded batch of `BufferedWriteRecord` values
(local ID plus exact WriteBatchItem). These helpers only read and never replay. They
check journal ownership, framing, size, CRC and UTF-8. Damaged/incomplete files can
raise a diagnostic instead of providing a complete list. Partial ACK error indexes
are retained in the adjacent .result.json. A local record ID is **not** a server
deduplication ID. Do not rename uncertain/partial data to pending without proving
that replay is safe. A crash during recovery can conservatively require manual
reconciliation of retained files rather than risking duplicated increments.

`shutdown()` is suspending, idempotent and completes cleanup despite outer cancellation.
It stops the flusher, closes/seals admission and drains up to shutdownMs. It then
cancels active I/O, retains NOT_SENT work as pending and ambiguous work as uncertain,
closes its owned client and releases directory ownership. `close()`/`use` provide the
same behavior for blocking lifecycles. Stop producers before closing. Physical I/O
stalls and non-cooperative callback/dispatcher work are not strict wall-clock promises.
A restarted client resumes definitely unsent data and reports retained audit files.
No exactly-once claim is made across a crash between remote ACK and local completion.

See [BUFFERED_RESULTS.md](BUFFERED_RESULTS.md) for the final local TCP checks and limits.
