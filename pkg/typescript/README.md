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
