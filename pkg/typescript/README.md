# FlameDB — TypeScript SDK

Node.js SDK for FlameDB. Uses a bounded TCP pool with lazy auth and FIFO pipelining per connection.

## Concurrency, deadlines and timings

The default pool has **two read connections and one write connection**. Connections open lazily; `connect()` authenticates one write connection. GET, LEADERBOARD, GROUP_LEADERBOARD, STATS, METRICS and PREVIEW_DELETE use reserved reads; mutations use writes. A blocked write cannot occupy read capacity. `pipelineSize` defaults to 64 pending requests per connection; further callers wait for admission. Responses remain FIFO on each socket.

```typescript
const db = new FlameDB({
  host: "127.0.0.1", port: 7777, apiKey,
  timeout: 5000, readConnections: 2, writeConnections: 1,
  pipelineSize: 64,
  onCommandTiming: timing => {
    // queueWaitMs and requestMs are milliseconds.
    // Forward to a bounded, non-blocking metrics sink.
  },
});
```

`timeout` is a total deadline per protocol command, including pool wait, lazy connect/auth, socket I/O and response JSON parsing. Multi-command helpers such as `deleteByTags` apply it per command. `queueWaitMs` measures admission; `requestMs` measures transport work after admission (including lazy authentication). A call that never acquires capacity has zero request time. `success` reports the transport result, before public result conversion. Observers run after admission capacity is released; keep them quick and non-blocking. Observer exceptions do not fail commands. As with any Node timer, a blocked event loop can delay timeout delivery.

Concurrent operations have **no ordering guarantee across connections**. Await a write before issuing a dependent read; do not depend on launch order. Increasing write capacity can reorder concurrent SET/DELETE/increment operations. Replication consistency is unchanged. For one shared connection, set `readConnections: 0, writeConnections: 1`.

A queued deadline expires without sending the command or interrupting another caller's socket. An active timeout closes its affected connection and rejects its entire pipeline; ambiguous writes are **not retried**. `disconnect()` rejects queued calls and closes all active sockets; a later command can reconnect. Pool sizes are fixed, failed sockets are replaced on demand, and no timing history is retained by the SDK. Timers and failed response buffers are released on settlement.

## Install

```bash
npm install flamedb
```

## Usage

```typescript
import FlameDB from "flamedb";

const db = new FlameDB({
  host: "127.0.0.1",
  port: 7777,
  apiKey: "flame_abc123",
  timeout: 5000,
});

// single write
await db.write("kills", 1, {
  leaderboardEntity: "pixel",
  tags: { player: "pixel", region: "eu" },
});

// batch write
await db.writeBatch([
  { metric: "kills", value: 1, options: { leaderboardEntity: "pixel", tags: { player: "pixel" } } },
  { metric: "deaths", value: 1, options: { leaderboardEntity: "pixel" } },
]);

// leaderboard
const top = await db.leaderboard("kills", { limit: 10 });
console.log(top); // [{ entity_id: "pixel", score: 42 }, ...]

// get events
const result = await db.get("kills", {
  where: { region: "eu" },
  from: new Date("2025-01-01"),
  limit: 100,
  order: "DESC",
});

// group leaderboard
const groups = await db.groupLeaderboard("kills", [
  { name: "team_red", members: ["pixel", "notch"] },
  { name: "team_blue", members: ["dream"] },
]);

// stats (cardinality)
const stats = await db.stats("kills", ["player", "region"]);

// cleanup
db.disconnect();
```

## API

### `new FlameDB(config)`
| field | type | default | description |
|---|---|---|---|
| `host` | `string` | — | FlameDB host |
| `port` | `number` | — | FlameDB port |
| `apiKey` | `string` | — | API key |
| `timeout` | `number` | `5000` | Total command deadline including pool wait (ms) |
| `readConnections` | `number` | `2` | Reserved reads; zero shares the write pool |
| `writeConnections` | `number` | `1` | Write pool size |
| `pipelineSize` | `number` | `64` | Pending commands per connection |
| `onCommandTiming` | `(timing: CommandTiming) => void` | — | Queue/request timing observer |

### Methods

| method | description |
|---|---|
| `connect()` | Explicitly connect (lazy otherwise) |
| `disconnect()` | Close all connections and reject active/queued calls |
| `write(metric, value, opts?)` | Write a single event |
| `writeBatch(items)` | Write multiple events in one command |
| `set(metric, value, entity?)` | Set absolute leaderboard value |
| `delete(metric, opts?)` | Delete leaderboard entry or events |
| `get(metrics, opts?)` | Fetch raw events |
| `leaderboard(metric, opts?)` | Fetch sorted leaderboard |
| `groupLeaderboard(metric, groups, opts?)` | Ad-hoc group leaderboard |
| `stats(metric, tags)` | Cardinality stats |


### Exact nanosecond timestamps (Node.js >= 22)

Use `bigint` for precise write timestamps, for example `timestampNs: 1790000000123456789n`.
Read exact values from `event.timestampNs` and `point.tsNs`. Existing `timestamp`/`ts`
number fields remain available but may round nanoseconds. Convert bigint fields to
strings when serializing SDK results with `JSON.stringify`.

An active timeout closes the affected connection and rejects its pending commands.
A write may already have been applied; do not automatically retry increments.
The next call replaces the failed connection automatically.

## Connection recovery

After a connection error, timeout, or invalid JSON response, pending commands on
that connection fail. The next call creates and authenticates a new connection;
concurrent calls share that connection and its authentication handshake. Failed
commands are never replayed automatically. Writes may already have been applied,
so do not blindly retry increments or a failed pipeline.

`disconnect()` closes all pool connections. As before, a later `connect()` or
command opens a new one. Server error responses do not invalidate a healthy connection.

## Metric discovery and player UUIDs

Requires a server supporting `METRICS` and read permission. Returns sorted, unique names across all known cluster nodes (an unavailable node causes an error, not a partial list). Empty results are `[]`.

```typescript
const keys = await db.listMetrics();
const playerKeys = await db.listMetrics({uuid: playerUUID});
const events = await db.get("kills", {where: {uuid: playerUUID}});
```

Store the UUID explicitly as an event tag, e.g. `WRITE kills 1 lb="<uuid>" uuid="<uuid>"`. The `lb` entity alone does not create a UUID tag. Multiple filters use AND and must match the same event. Filtered discovery includes only metrics with matching stored events; unfiltered discovery also includes leaderboard-only metrics. Discovery scans stored keys and is intended for administrative operations, not per-tick calls.

Deleting `lb="<uuid>"` resets only that all-time leaderboard entry. It does not delete historical events or reset windowed aggregates. Use tag-filtered deletion below to remove one player's historical events.

## Delete events by tag

Requires the updated server. `DELETE kills WHERE player="abc"` deletes only events with that exact tag value. Tag names are arbitrary, multiple conditions use AND, and optional FROM/TO bounds use an inclusive start and exclusive end. All secondary indexes are removed with the events, so GET, aggregates and windowed leaderboards reflect the deletion. Explicitly empty SDK filters are rejected; omit the filter only for the existing unfiltered deletion behavior.

```typescript
const where = {player: playerUUID};
await db.delete("kills", {where});
for (const metric of await db.listMetrics(where)) {
  await db.delete(metric, {where});
}
// Full reset, including leaderboard-only entries:
for (const metric of await db.listMetrics()) {
  await db.delete(metric, {where, leaderboardEntity: playerUUID});
}
```

A tag filter alone does not change all-time leaderboard scores: stored events do not record the `lb` entity, and SET can independently override its score. To reset a known entity, combine the filter with `leaderboardEntity`; this removes that **entire** all-time entry regardless of the event filter/time range, atomically with the matching events for this metric. It does not subtract event values or infer entity IDs from tag values.

The loop across metrics is not an atomic cluster-wide operation. Stop concurrent writes for the player during a complete reset; new events may otherwise arrive during or after it. Errors propagate, and completed metric deletions are not rolled back. The server uses the existing primary routing and replication path; default replication remains asynchronous. Protocol callers can request `QUORUM`.

## Generic bulk deletion and preview

`deleteByTags` (`DeleteByTags` in Go) requires a non-empty tag filter and processes discovered metrics sequentially. Optional time bounds and an explicit leaderboard entity have the same semantics as single-metric deletion. With an explicit leaderboard entity, discovery includes all metrics so leaderboard-only entries are handled too.

```typescript
const where = {account: accountId};
const preview = await db.deleteByTags(where, {dryRun: true});
console.log(preview.events, preview.leaderboard_entries);
const deleted = await db.deleteByTags(where);
for (const item of deleted.metrics) {
  if (item.error) console.error(item.metric, item.error);
  else console.log(item.metric, item.result.events);
}
// Optional explicit all-time entity removal, including leaderboard-only metrics:
await db.deleteByTags(where, {leaderboardEntity: accountId});
const one = await db.delete("requests", {where, dryRun: true});
```

Results contain the dry-run flag, total event/leaderboard-entry counts, and a per-metric list with either a result or an error. In preview mode counts mean **would be deleted**; otherwise they mean **confirmed deleted**. A missing entity and an existing zero-score entity are distinguished. Zero matches succeed with zero counts.

Discovery failures abort the call. Per-metric failures are collected while remaining metrics are attempted. Totals exclude failed/unknown outcomes: a transport or replication error may occur after the primary already applied a deletion. The helper does not retry failed mutations automatically. Go returns a partial report plus an error on context cancellation; Kotlin propagates coroutine cancellation.

Preview and execution are separate calls, not a reserved snapshot or a transaction across metrics. Concurrent writes can change the counts. Inspect `metrics` for errors before treating a preview or deletion as complete. Both require write permission; discovery additionally requires read permission.

Update servers before using these SDK methods. The preview sends `PREVIEW_DELETE <metric> [WHERE ...] [lb="..."] [FROM ...] [TO ...]`, which performs no replication or data changes. Actual `DELETE` now returns `{"delete":{"dry_run":false,"events":42,"leaderboard_entries":1}}`; preview returns the same shape with `dry_run:true`. Missing/inconsistent counters are reported as errors instead of being presented as a successful zero count.
