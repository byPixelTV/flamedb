import * as net from "net";

// ─── Types ────────────────────────────────────────────────────────────────────

export interface FlameDBConfig {
  host: string;
  port: number;
  apiKey: string;
  /** Timeout in ms for individual commands (default: 5000) */
  timeout?: number;
  /** Max pending commands in pipeline before flushing (default: 64) */
  pipelineSize?: number;
}

export interface Event {
  timestamp: number; // unix nanoseconds
  metric: string;
  value: number;
  tags?: Record<string, string>;
}

export interface LeaderboardEntry {
  entity_id: string;
  score: number;
}

export interface GroupLeaderboardEntry {
  group: string;
  score: number;
}

export interface SeriesPoint {
  ts: number;
  value: number;
  count: number;
}

export interface AggregateResult {
  type: string;
  value: number;
  count: number;
}

export interface StatsResult {
  metric: string;
  tag_stats: TagStats[];
}

export interface TagStats {
  tag_key: string;
  cardinality: number;
}

export interface BatchResult {
  ok: boolean;
  accepted: number;
  failed: number;
  errors?: { index: number; error: string }[];
}

export interface WriteOptions {
  leaderboardEntity?: string;
  tags?: Record<string, string>;
  timestampNs?: number;
  quorum?: boolean;
}

export interface GetOptions {
  where?: Record<string, string>;
  from?: Date;
  to?: Date;
  limit?: number;
  offset?: number;
  order?: "ASC" | "DESC";
}

export interface GetResult {
  events?: Event[];
  metrics?: Record<string, Event[]>;
  aggregate?: AggregateResult;
  aggregates?: Record<string, AggregateResult>;
  series?: SeriesPoint[];
  series_by_metric?: Record<string, SeriesPoint[]>;
}

export interface LeaderboardOptions {
  limit?: number;
  offset?: number;
}

export interface GroupDef {
  name: string;
  members: string[];
}

export interface WriteBatchItem {
  metric: string;
  value: number;
  options?: WriteOptions;
}

// ─── Connection ────────────────────────────────────────────────────────────────

class FlameConnection {
  private socket: net.Socket;
  private buffer = "";
  private queue: Array<{
    resolve: (line: string) => void;
    reject: (err: Error) => void;
  }> = [];
  private authenticated = false;
  private connectPromise: Promise<void>;

  constructor(
    private config: Required<FlameDBConfig>,
  ) {
    this.socket = new net.Socket();
    this.connectPromise = this.connect();
  }

  private connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.socket.connect(this.config.port, this.config.host, () => {
        resolve();
      });

      this.socket.on("data", (chunk) => {
        this.buffer += chunk.toString();
        const lines = this.buffer.split("\n");
        this.buffer = lines.pop()!;
        for (const line of lines) {
          const trimmed = line.trim();
          if (!trimmed) continue;
          const pending = this.queue.shift();
          if (pending) pending.resolve(trimmed);
        }
      });

      this.socket.on("error", (err) => {
        reject(err);
        // drain pending
        for (const p of this.queue) p.reject(err);
        this.queue = [];
      });

      this.socket.on("close", () => {
        const err = new Error("FlameDB connection closed");
        for (const p of this.queue) p.reject(err);
        this.queue = [];
      });
    });
  }

  async ready(): Promise<void> {
    await this.connectPromise;
    if (this.authenticated) return;

    // expect {"auth":"required"}
    const challenge = await this.recv();
    const parsed = JSON.parse(challenge);
    if (parsed.auth !== "required") {
      throw new Error(`Unexpected auth challenge: ${challenge}`);
    }

    this.send(`AUTH ${this.config.apiKey}`);
    const resp = await this.recv();
    const authResp = JSON.parse(resp);
    if (authResp.auth !== "ok") {
      throw new Error(`Auth failed: ${authResp.error ?? resp}`);
    }
    this.authenticated = true;
  }

  send(line: string): void {
    this.socket.write(line + "\n");
  }

  recv(): Promise<string> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        const idx = this.queue.findIndex((q) => q.resolve === resolve);
        if (idx !== -1) this.queue.splice(idx, 1);
        reject(new Error("FlameDB command timeout"));
      }, this.config.timeout);

      this.queue.push({
        resolve: (line) => {
          clearTimeout(timer);
          resolve(line);
        },
        reject: (err) => {
          clearTimeout(timer);
          reject(err);
        },
      });
    });
  }

  async command(line: string): Promise<unknown> {
    await this.ready();
    this.send(line);
    const raw = await this.recv();
    const parsed = JSON.parse(raw);
    if (parsed.error) throw new Error(parsed.error);
    return parsed;
  }

  async multilineCommand(lines: string[]): Promise<unknown> {
    await this.ready();
    for (const l of lines) this.send(l);
    const raw = await this.recv();
    const parsed = JSON.parse(raw);
    if (parsed.error) throw new Error(parsed.error);
    return parsed;
  }

  destroy(): void {
    this.socket.destroy();
  }
}

// ─── FlameDB Client ──────────────────────────────────────────────────────────

export class FlameDB {
  private config: Required<FlameDBConfig>;
  private conn: FlameConnection | null = null;

  constructor(config: FlameDBConfig) {
    this.config = {
      timeout: 5000,
      pipelineSize: 64,
      ...config,
    };
  }

  private getConn(): FlameConnection {
    if (!this.conn) {
      this.conn = new FlameConnection(this.config);
    }
    return this.conn;
  }

  /** Explicitly connect and authenticate. Called lazily otherwise. */
  async connect(): Promise<void> {
    await this.getConn().ready();
  }

  /** Close the TCP connection. */
  disconnect(): void {
    this.conn?.destroy();
    this.conn = null;
  }

  // ─── Write ──────────────────────────────────────────────────────────────────

  async write(
    metric: string,
    value: number,
    options: WriteOptions = {},
  ): Promise<void> {
    const parts: string[] = [`WRITE ${metric} ${value}`];

    if (options.leaderboardEntity !== undefined) {
      parts.push(`lb="${options.leaderboardEntity}"`);
    }
    if (options.tags) {
      for (const [k, v] of Object.entries(options.tags)) {
        parts.push(`${k}="${v}"`);
      }
    }
    if (options.timestampNs !== undefined) {
      parts.push(`ts=${options.timestampNs}`);
    }
    if (options.quorum) {
      parts.push("QUORUM");
    }

    await this.getConn().command(parts.join(" "));
  }

  /** Write multiple events in a single WRITE_BATCH command. */
  async writeBatch(items: WriteBatchItem[]): Promise<BatchResult> {
    const lines = ["WRITE_BATCH"];
    for (const item of items) {
      const parts: string[] = [`WRITE ${item.metric} ${item.value}`];
      const opts = item.options ?? {};
      if (opts.leaderboardEntity !== undefined) {
        parts.push(`lb="${opts.leaderboardEntity}"`);
      }
      if (opts.tags) {
        for (const [k, v] of Object.entries(opts.tags)) {
          parts.push(`${k}="${v}"`);
        }
      }
      if (opts.timestampNs !== undefined) parts.push(`ts=${opts.timestampNs}`);
      if (opts.quorum) parts.push("QUORUM");
      lines.push(parts.join(" "));
    }
    lines.push("END");

    const result = await this.getConn().multilineCommand(lines);
    return result as BatchResult;
  }

  // ─── Set ────────────────────────────────────────────────────────────────────

  async set(
    metric: string,
    value: number,
    leaderboardEntity?: string,
  ): Promise<void> {
    let cmd = `SET ${metric} ${value}`;
    if (leaderboardEntity !== undefined) cmd += ` lb="${leaderboardEntity}"`;
    await this.getConn().command(cmd);
  }

  // ─── Delete ─────────────────────────────────────────────────────────────────

  async delete(
    metric: string,
    options: { leaderboardEntity?: string; from?: Date; to?: Date } = {},
  ): Promise<void> {
    const parts = [`DELETE ${metric}`];
    if (options.leaderboardEntity !== undefined) {
      parts.push(`lb="${options.leaderboardEntity}"`);
    }
    if (options.from) parts.push(`FROM ${fmtDate(options.from)}`);
    if (options.to) parts.push(`TO ${fmtDate(options.to)}`);
    await this.getConn().command(parts.join(" "));
  }

  // ─── Get ────────────────────────────────────────────────────────────────────

  async get(
    metrics: string | string[],
    options: GetOptions = {},
  ): Promise<GetResult> {
    const metricStr = Array.isArray(metrics) ? metrics.join(",") : metrics;
    const parts = [`GET ${metricStr}`];

    if (options.where && Object.keys(options.where).length > 0) {
      const clauses = Object.entries(options.where)
        .map(([k, v]) => `${k}="${v}"`)
        .join(" AND ");
      parts.push(`WHERE ${clauses}`);
    }
    if (options.from) parts.push(`FROM ${fmtDate(options.from)}`);
    if (options.to) parts.push(`TO ${fmtDate(options.to)}`);
    if (options.limit !== undefined) parts.push(`LIMIT ${options.limit}`);
    if (options.offset !== undefined) parts.push(`OFFSET ${options.offset}`);
    if (options.order) parts.push(`ORDER ${options.order}`);

    return (await this.getConn().command(parts.join(" "))) as GetResult;
  }

  // ─── Leaderboard ────────────────────────────────────────────────────────────

  async leaderboard(
    metric: string,
    options: LeaderboardOptions = {},
  ): Promise<LeaderboardEntry[]> {
    const parts = [`LEADERBOARD ${metric}`];
    if (options.limit !== undefined) parts.push(`LIMIT ${options.limit}`);
    if (options.offset !== undefined) parts.push(`OFFSET ${options.offset}`);
    const result = (await this.getConn().command(parts.join(" "))) as {
      leaderboard: LeaderboardEntry[];
    };
    return result.leaderboard ?? [];
  }

  // ─── Group Leaderboard ───────────────────────────────────────────────────────

  async groupLeaderboard(
    metric: string,
    groups: GroupDef[],
    options: LeaderboardOptions = {},
  ): Promise<GroupLeaderboardEntry[]> {
    const parts = [`GROUP_LEADERBOARD ${metric}`];
    for (const g of groups) {
      parts.push(`GROUP "${g.name}:${g.members.join(",")}"`);
    }
    if (options.limit !== undefined) parts.push(`LIMIT ${options.limit}`);
    if (options.offset !== undefined) parts.push(`OFFSET ${options.offset}`);
    const result = (await this.getConn().command(parts.join(" "))) as {
      leaderboard: GroupLeaderboardEntry[];
    };
    return result.leaderboard ?? [];
  }

  // ─── Stats ───────────────────────────────────────────────────────────────────

  async stats(metric: string, tags: string[]): Promise<StatsResult> {
    const cmd = `STATS ${metric} TAGS ${tags.join(" ")}`;
    return (await this.getConn().command(cmd)) as StatsResult;
  }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

function fmtDate(d: Date): string {
  return d.toISOString().slice(0, 10); // YYYY-MM-DD
}

export default FlameDB;
