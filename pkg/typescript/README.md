# FlameDB — TypeScript SDK

Node.js SDK for FlameDB. Uses a persistent TCP connection with lazy auth.

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
| `timeout` | `number` | `5000` | Command timeout (ms) |

### Methods

| method | description |
|---|---|
| `connect()` | Explicitly connect (lazy otherwise) |
| `disconnect()` | Close connection |
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

A timeout closes the connection and rejects every pending command. A write may
already have been applied; do not automatically retry increments. Call
`disconnect()` before establishing a new connection.

## Connection recovery

After a connection error, timeout, or invalid JSON response, pending commands on
that connection fail. The next call creates and authenticates a new connection;
concurrent calls share that connection and its authentication handshake. Failed
commands are never replayed automatically. Writes may already have been applied,
so do not blindly retry increments or a failed pipeline.

`disconnect()` closes the current connection. As before, a later `connect()` or
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
