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
  timestamp: number; // compatibility field; may lose nanosecond precision
  timestampNs?: bigint; // exact value on supported Node.js runtimes
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
  tsNs?: bigint;
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
  timestampNs?: number | bigint;
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
  private socket = new net.Socket();
  private buffer = "";
  private inbox: string[] = [];
  private queue: Array<{ resolve: (line: string) => void; reject: (err: Error) => void }> = [];
  private failure: Error | null = null;
  private readyPromise: Promise<void>;

  constructor(private config: Required<FlameDBConfig>) {
    if (!Number.isInteger(config.timeout) || config.timeout <= 0 || !Number.isInteger(config.pipelineSize) || config.pipelineSize < 1) throw new Error("Invalid connection limits");
    validateLine(config.apiKey);
    this.socket.setEncoding("utf8");
    this.socket.on("data", (chunk: string) => {
      this.buffer += chunk;
      if (this.buffer.length > 64 * 1024 * 1024) { this.fail(new Error("FlameDB response too large")); return; }
      let end: number;
      while ((end = this.buffer.indexOf("\n")) >= 0) {
        const line = this.buffer.slice(0, end).trim();
        this.buffer = this.buffer.slice(end + 1);
        if (!line) continue;
        const pending = this.queue.shift();
        if (pending) pending.resolve(line);
        else if (this.inbox.length < 2) this.inbox.push(line);
        else { this.fail(new Error("Unexpected FlameDB response")); return; }
      }
    });
    this.socket.on("error", (err) => this.fail(err));
    this.socket.on("close", () => this.fail(new Error("FlameDB connection closed")));
    this.readyPromise = this.handshake();
  }

  private fail(err: Error): void {
    this.failure ??= err;
    for (const pending of this.queue.splice(0)) pending.reject(this.failure);
    this.inbox = [];
    this.socket.destroy();
  }

  private async handshake(): Promise<void> {
    try {
      // Register before connecting: an immediate server greeting must not be lost.
      const challenge = this.recv();
      this.socket.connect(this.config.port, this.config.host);
      if (JSON.parse(await challenge).auth !== "required") throw new Error("Unexpected auth challenge");
      const response = this.recv();
      this.socket.write(`AUTH ${this.config.apiKey}\n`);
      const auth = JSON.parse(await response);
      if (auth.auth !== "ok") throw new Error(`Auth failed: ${auth.error ?? "invalid response"}`);
    } catch (err) { this.fail(err as Error); throw err; }
  }

  ready(): Promise<void> { return this.readyPromise; }

  private recv(): Promise<string> {
    if (this.failure) return Promise.reject(this.failure);
    if (this.inbox.length) return Promise.resolve(this.inbox.shift()!);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => this.fail(new Error("FlameDB command timeout; outcome unknown")), this.config.timeout);
      this.queue.push({
        resolve: (line) => { clearTimeout(timer); resolve(line); },
        reject: (err) => { clearTimeout(timer); reject(err); },
      });
    });
  }

  command(line: string): Promise<unknown> { return this.multilineCommand([line]); }

  async multilineCommand(lines: string[]): Promise<unknown> {
    for (const line of lines) validateLine(line);
    await this.ready();
    if (this.failure) throw this.failure;
    if (this.queue.length >= this.config.pipelineSize) throw new Error("FlameDB pipeline is full");
    const response = this.recv();
    this.socket.write(lines.join("\n") + "\n");
    let parsed: any;
    try { parsed = JSON.parse(await response, function (key: string, value: unknown, context?: { source: string }) {
        if ((key === "timestamp" || key === "ts") && typeof value === "number" && context?.source) {
          this[key === "timestamp" ? "timestampNs" : "tsNs"] = BigInt(context.source);
        }
        return value;
      }); } catch (err) { this.fail(err as Error); throw err; }
    if (parsed.error) throw new Error(parsed.error);
    return parsed;
  }

  destroy(): void { this.fail(new Error("FlameDB connection closed")); }
}

function validateLine(line: string): void {
  if (/[\r\n\x00\x1f]/.test(line)) throw new Error("Invalid command framing");
}

function identifier(value: string): string {
  if (!value || /[\s:=",]/.test(value)) throw new Error("Invalid identifier");
  return value;
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
    const parts: string[] = [`WRITE ${identifier(metric)} ${finite(value)}`];

    if (options.leaderboardEntity !== undefined) {
      parts.push(`lb=${JSON.stringify(options.leaderboardEntity)}`);
    }
    if (options.tags) {
      for (const [k, v] of Object.entries(options.tags)) {
        parts.push(`${identifier(k)}=${JSON.stringify(v)}`);
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
      const parts: string[] = [`WRITE ${identifier(item.metric)} ${finite(item.value)}`];
      const opts = item.options ?? {};
      if (opts.leaderboardEntity !== undefined) {
        parts.push(`lb=${JSON.stringify(opts.leaderboardEntity)}`);
      }
      if (opts.tags) {
        for (const [k, v] of Object.entries(opts.tags)) {
          parts.push(`${identifier(k)}=${JSON.stringify(v)}`);
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
    let cmd = `SET ${identifier(metric)} ${finite(value)}`;
    if (leaderboardEntity !== undefined) cmd += ` lb=${JSON.stringify(leaderboardEntity)}`;
    await this.getConn().command(cmd);
  }

  // ─── Delete ─────────────────────────────────────────────────────────────────

  async delete(
    metric: string,
    options: { leaderboardEntity?: string; from?: Date; to?: Date } = {},
  ): Promise<void> {
    const parts = [`DELETE ${identifier(metric)}`];
    if (options.leaderboardEntity !== undefined) {
      parts.push(`lb=${JSON.stringify(options.leaderboardEntity)}`);
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
    const metricStr = Array.isArray(metrics) ? metrics.map(identifier).join(",") : identifier(metrics);
    const parts = [`GET ${metricStr}`];

    if (options.where && Object.keys(options.where).length > 0) {
      const clauses = Object.entries(options.where)
        .map(([k, v]) => `${identifier(k)}=${JSON.stringify(v)}`)
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
    const parts = [`LEADERBOARD ${identifier(metric)}`];
    if (options.limit !== undefined) parts.push(`LIMIT ${options.limit}`);
    if (options.offset !== undefined) parts.push(`OFFSET ${options.offset}`);
    const result = (await this.getConn().command(parts.join(" "))) as {
      leaderboard: LeaderboardEntry[];
    };
    return (result.leaderboard ?? []).map((entry: any) => ({ entity_id: entry.entity_id, score: entry.value }));
  }

  // ─── Group Leaderboard ───────────────────────────────────────────────────────

  async groupLeaderboard(
    metric: string,
    groups: GroupDef[],
    options: LeaderboardOptions = {},
  ): Promise<GroupLeaderboardEntry[]> {
    const parts = [`GROUP_LEADERBOARD ${identifier(metric)}`];
    for (const g of groups) {
      parts.push(`GROUP ${JSON.stringify(`${g.name}:${g.members.join(",")}`)}`);
    }
    if (options.limit !== undefined) parts.push(`LIMIT ${options.limit}`);
    if (options.offset !== undefined) parts.push(`OFFSET ${options.offset}`);
    const result = (await this.getConn().command(parts.join(" "))) as {
      leaderboard: GroupLeaderboardEntry[];
    };
    return (result.leaderboard ?? []).map((entry: any) => ({ group: entry.entity_id, score: entry.value }));
  }

  // ─── Stats ───────────────────────────────────────────────────────────────────

  async stats(metric: string, tags: string[]): Promise<StatsResult> {
    const cmd = `STATS ${identifier(metric)} TAGS ${tags.map(identifier).join(" ")}`;
    return ((await this.getConn().command(cmd)) as { stats: StatsResult }).stats;
  }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

function fmtDate(d: Date): string {
  return d.toISOString().slice(0, 10); // YYYY-MM-DD
}

export default FlameDB;

function finite(value: number): number {
  if (!Number.isFinite(value)) throw new Error("Value must be finite");
  return value;
}
