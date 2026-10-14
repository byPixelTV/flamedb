import * as net from "net";

// ─── Types ────────────────────────────────────────────────────────────────────

export interface FlameDBConfig {
  host: string;
  port: number;
  apiKey: string;
  /** Total deadline including admission, connect/auth and socket I/O (default: 5000). */
  timeout?: number;
  /** Max pending commands per connection (default: 64). Further callers wait. */
  pipelineSize?: number;
  /** Reserved read connections (default: 2). Zero shares the write pool. */
  readConnections?: number;
  /** Write connections (default: 1). */
  writeConnections?: number;
  /** Called after releasing admission capacity. Keep it quick/non-blocking. */
  onCommandTiming?: (timing: CommandTiming) => void;
}

export interface CommandTiming {
  command: string;
  read: boolean;
  queueWaitMs: number;
  /** Includes lazy connect/auth, socket I/O and JSON parsing. */
  requestMs: number;
  success: boolean;
}

type ResolvedConfig = Required<Omit<FlameDBConfig, "onCommandTiming">> & Pick<FlameDBConfig, "onCommandTiming">;

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

export interface DeleteResult {
  dry_run: boolean;
  events: number;
  leaderboard_entries: number;
}
export interface DeleteOptions {
  leaderboardEntity?: string;
  from?: Date;
  to?: Date;
  where?: Record<string, string>;
  dryRun?: boolean;
}
export interface MetricDeleteResult {
  metric: string;
  result?: DeleteResult;
  error?: string;
}
export interface DeleteByTagsResult extends DeleteResult {
  metrics: MetricDeleteResult[];
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
  leaderboardEntities?: Record<string, string>;
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
  board?: string;
}

/** Selects an all-time board or a half-open event window. */
export interface RankingOptions extends LeaderboardOptions {
  from?: Date;
  to?: Date;
  entityTag?: string;
}

/** Global one-based ordinal rank in the selected leaderboard's tie order. */
export interface RankedEntry {
  entity_id: string;
  score: number;
  rank: number;
}

interface RankingResult {
  rank: number | null;
  score: number | null;
  count: number;
  around?: RankedEntry[];
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

  constructor(private config: ResolvedConfig) {
    if (!Number.isInteger(config.timeout) || config.timeout <= 0 || !Number.isInteger(config.pipelineSize) || config.pipelineSize < 1) throw new Error("Invalid connection limits");
    validateLine(config.apiKey);
    this.socket.setNoDelay(true);
    this.socket.setEncoding("utf8");
    this.socket.on("data", (chunk: string) => {
      if (this.failure) return;
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
    // A deadline can expire just after constructing a lazy connection, before
    // sendCommand attaches its await. Preserve rejection for callers while
    // avoiding an unhandled background handshake rejection in that gap.
    void this.readyPromise.catch(() => {});
  }

  private fail(err: Error): void {
    this.failure ??= err;
    for (const pending of this.queue.splice(0)) pending.reject(this.failure);
    this.buffer = "";
    this.inbox = [];
    this.socket.destroy();
  }

  private async handshake(): Promise<void> {
    const timer = setTimeout(() => this.fail(new Error("FlameDB connect timeout")), this.config.timeout);
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
    finally { clearTimeout(timer); }
  }

  ready(): Promise<void> { return this.readyPromise; }

  isFailed(): boolean { return this.failure !== null; }

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

  async multilineCommand(lines: string[], deadline: number): Promise<unknown> {
    const remaining = deadline - performance.now();
    if (remaining <= 0) throw new Error("FlameDB deadline exceeded before send");
    let timer: ReturnType<typeof setTimeout>;
    const expired = new Promise<never>((_, reject) => {
      timer = setTimeout(() => {
        const error = new Error("FlameDB command timeout; outcome unknown; not retried");
        this.fail(error);
        reject(error);
      }, remaining);
    });
    try { return await Promise.race([this.sendCommand(lines, deadline), expired]); }
    finally { clearTimeout(timer!); }
  }

  private async sendCommand(lines: string[], deadline: number): Promise<unknown> {
    for (const line of lines) validateLine(line);
    await this.ready();
    if (this.failure) throw this.failure;
    if (performance.now() >= deadline) throw new Error("FlameDB deadline exceeded before send");
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
    if (performance.now() >= deadline) throw new Error("FlameDB command deadline exceeded; not retried");
    return parsed;
  }

  destroy(): void { this.fail(new Error("FlameDB connection closed")); }
}

type PoolSlot = { connection?: FlameConnection; load: number };
type PoolWaiter = { resolve: (slot: PoolSlot) => void; reject: (error: Error) => void; timer: ReturnType<typeof setTimeout> };

// Pipelining stays FIFO on each socket. Separate pools prevent write traffic
// from consuming read admission capacity or delaying read responses.
class CommandPool {
  private slots: PoolSlot[];
  private waiters = new Set<PoolWaiter>();
  private closed = false;

  constructor(private config: ResolvedConfig, size: number) {
    this.slots = Array.from({length: size}, () => ({load: 0}));
  }

  private available(): PoolSlot | undefined {
    let chosen: PoolSlot | undefined;
    for (const slot of this.slots) {
      if (slot.load < this.config.pipelineSize && (!chosen || slot.load < chosen.load)) chosen = slot;
    }
    return chosen;
  }

  private acquire(deadline: number): Promise<PoolSlot> {
    if (this.closed) return Promise.reject(new Error("FlameDB pool closed"));
    if (performance.now() >= deadline) return Promise.reject(new Error("FlameDB pool wait timeout; command was not sent"));
    const slot = this.available();
    if (slot) { slot.load++; return Promise.resolve(slot); }
    return new Promise((resolve, reject) => {
      const waiter: PoolWaiter = {resolve, reject, timer: setTimeout(() => {
        this.waiters.delete(waiter);
        reject(new Error("FlameDB pool wait timeout; command was not sent"));
      }, Math.max(1, deadline - performance.now()))};
      this.waiters.add(waiter);
    });
  }

  private release(slot: PoolSlot): void {
    slot.load--;
    if (this.closed) return;
    const waiter = this.waiters.values().next().value;
    if (waiter) {
      this.waiters.delete(waiter);
      clearTimeout(waiter.timer);
      slot.load++;
      waiter.resolve(slot);
    }
  }

  private connection(slot: PoolSlot): FlameConnection {
    if (!slot.connection || slot.connection.isFailed()) slot.connection = new FlameConnection(this.config);
    return slot.connection;
  }

  async connect(): Promise<void> { await this.connection(this.slots[0]).ready(); }

  async command(lines: string[], deadline: number, acquired: () => void): Promise<unknown> {
    const slot = await this.acquire(deadline);
    acquired();
    try {
      if (this.closed) throw new Error("FlameDB pool closed");
      if (performance.now() >= deadline) throw new Error("FlameDB pool wait timeout; command was not sent");
      return await this.connection(slot).multilineCommand(lines, deadline);
    } finally { this.release(slot); }
  }

  close(): void {
    this.closed = true;
    for (const waiter of this.waiters) { clearTimeout(waiter.timer); waiter.reject(new Error("FlameDB pool closed")); }
    this.waiters.clear();
    for (const slot of this.slots) slot.connection?.destroy();
  }
}

function validateLine(line: string): void {
  if (/[\r\n\x00\x1f]/.test(line)) throw new Error("Invalid command framing");
}

function identifier(value: string): string {
  if (!value || /[\s=",\x00\x1f]/.test(value)) throw new Error("Invalid identifier");
  return value;
}

// ─── FlameDB Client ──────────────────────────────────────────────────────────

export class FlameDB {
  private config: ResolvedConfig;
  private readPool: CommandPool | null = null;
  private writePool: CommandPool | null = null;

  constructor(config: FlameDBConfig) {
    this.config = {
      timeout: 5000,
      pipelineSize: 64,
      readConnections: 2,
      writeConnections: 1,
      ...config,
    };
    for (const value of [this.config.timeout, this.config.pipelineSize, this.config.writeConnections]) {
      if (!Number.isSafeInteger(value) || value < 1) throw new Error("Invalid connection limits");
    }
    if (!Number.isInteger(this.config.readConnections) || this.config.readConnections < 0 ||
        this.config.readConnections > 1024 || this.config.writeConnections > 1024) throw new Error("Invalid pool sizes");
  }

  private pool(read: boolean): CommandPool {
    this.writePool ??= new CommandPool(this.config, this.config.writeConnections);
    if (!read || this.config.readConnections === 0) return this.writePool;
    return this.readPool ??= new CommandPool(this.config, this.config.readConnections);
  }

  private async command(input: string | string[]): Promise<unknown> {
    const lines = typeof input === "string" ? [input] : input;
    lines.forEach(validateLine);
    const command = lines[0].split(" ", 1)[0];
    const read = ["GET", "LEADERBOARD", "GROUP_LEADERBOARD", "STATS", "METRICS", "PREVIEW_DELETE", "RANK", "SCORE", "AROUND", "LEADERBOARD_COUNT"].includes(command);
    const started = performance.now();
    let admitted: number | undefined;
    let success = false;
    try {
      const result = await this.pool(read).command(lines, started + this.config.timeout, () => { admitted = performance.now(); });
      success = true;
      return result;
    } finally {
      const finished = performance.now();
      try { this.config.onCommandTiming?.({command, read, queueWaitMs: (admitted ?? finished) - started,
        requestMs: admitted === undefined ? 0 : finished - admitted, success}); } catch { /* Telemetry cannot fail a command. */ }
    }
  }

  /** Explicitly connect and authenticate. Called lazily otherwise. */
  async connect(): Promise<void> {
    await this.pool(false).connect();
  }

  /** Close all connections and reject active/queued commands. Later calls reconnect. */
  disconnect(): void {
    this.readPool?.close();
    this.writePool?.close();
    this.readPool = this.writePool = null;
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
    for (const [board, entity] of Object.entries(options.leaderboardEntities ?? {})) {
      parts.push(`lb.${identifier(board)}=${JSON.stringify(entity)}`);
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

    await this.command(parts.join(" "));
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
      for (const [board, entity] of Object.entries(opts.leaderboardEntities ?? {})) {
        parts.push(`lb.${identifier(board)}=${JSON.stringify(entity)}`);
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

    const result = await this.command(lines);
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
    await this.command(cmd);
  }

  // ─── Delete ─────────────────────────────────────────────────────────────────

  async delete(
    metric: string,
    options: DeleteOptions = {},
  ): Promise<DeleteResult> {
    const parts = [`DELETE ${identifier(metric)}`];
    if (options.dryRun) parts[0] = `PREVIEW_DELETE ${identifier(metric)}`;
    if (options.where !== undefined) {
      const clauses = Object.entries(options.where).map(([k, v]) => `${identifier(k)}=${JSON.stringify(v)}`).join(" AND ");
      if (!clauses) throw new Error("delete filter must not be empty");
      parts.push(`WHERE ${clauses}`);
    }
    if (options.leaderboardEntity !== undefined) {
      parts.push(`lb=${JSON.stringify(options.leaderboardEntity)}`);
    }
    if (options.from) parts.push(`FROM ${fmtDate(options.from)}`);
    if (options.to) parts.push(`TO ${fmtDate(options.to)}`);
    const response = await this.command(parts.join(" ")) as {delete?: DeleteResult};
    if (!response.delete || response.delete.dry_run !== (options.dryRun ?? false) ||
        !Number.isSafeInteger(response.delete.events) || response.delete.events < 0 ||
        !Number.isSafeInteger(response.delete.leaderboard_entries) || response.delete.leaderboard_entries < 0)
      throw new Error("missing or inconsistent delete result (updated server required)");
    return response.delete;
  }

  /** Totals include confirmed results only; errors may have applied remotely. */
  async deleteByTags(where: Record<string, string>, options: Omit<DeleteOptions, "where"> = {}): Promise<DeleteByTagsResult> {
    if (!Object.keys(where).length) throw new Error("delete filter must not be empty");
    Object.keys(where).forEach(identifier);
    const keys = await this.listMetrics(options.leaderboardEntity !== undefined ? {} : where);
    const report: DeleteByTagsResult = {dry_run: options.dryRun ?? false, events: 0, leaderboard_entries: 0, metrics: []};
    for (const metric of keys) {
      try {
        const result = await this.delete(metric, {...options, where});
        report.metrics.push({metric, result});
        report.events += result.events;
        report.leaderboard_entries += result.leaderboard_entries;
      } catch (error) {
        report.metrics.push({metric, error: error instanceof Error ? error.message : String(error)});
      }
    }
    return report;
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

    return (await this.command(parts.join(" "))) as GetResult;
  }

  // ─── Leaderboard ────────────────────────────────────────────────────────────

  async leaderboard(
    metric: string,
    options: LeaderboardOptions = {},
  ): Promise<LeaderboardEntry[]> {
    const parts = [`LEADERBOARD ${identifier(metric)}`];
    if (options.board !== undefined) parts.push(`BOARD ${identifier(options.board)}`);
    if (options.limit !== undefined) parts.push(`LIMIT ${options.limit}`);
    if (options.offset !== undefined) parts.push(`OFFSET ${options.offset}`);
    const result = (await this.command(parts.join(" "))) as {
      leaderboard: LeaderboardEntry[];
    };
    return (result.leaderboard ?? []).map((entry: any) => ({ entity_id: entry.entity_id, score: entry.value }));
  }

  /** One-based global rank, or null for a missing entity. */
  async getRank(metric: string, entity: string, options: RankingOptions = {}): Promise<number | null> {
    return (await this.ranking("RANK", metric, entity, undefined, options)).rank;
  }

  /** Current score, including zero, or null for a missing entity. */
  async getScore(metric: string, entity: string, options: RankingOptions = {}): Promise<number | null> {
    return (await this.ranking("SCORE", metric, entity, undefined, options)).score;
  }

  /** Entity plus up to radius neighbors on either side, retaining global ranks. */
  async getAroundEntity(metric: string, entity: string, radius: number, options: RankingOptions = {}): Promise<RankedEntry[]> {
    if (!Number.isSafeInteger(radius) || radius < 0 || radius > 100000) throw new Error("Invalid ranking radius");
    return (await this.ranking("AROUND", metric, entity, radius, options)).around!;
  }

  /** Full entity count; LIMIT/OFFSET do not change this count or global ranks. */
  async getLeaderboardCount(metric: string, options: RankingOptions = {}): Promise<number> {
    return (await this.ranking("LEADERBOARD_COUNT", metric, undefined, undefined, options)).count;
  }

  private async ranking(command: string, metric: string, entity: string | undefined, radius: number | undefined, options: RankingOptions): Promise<RankingResult> {
    const parts = [command, identifier(metric)];
    if (command !== "LEADERBOARD_COUNT") {
      if (typeof entity !== "string" || entity.length === 0) throw new Error("Invalid ranking entity");
      parts.push(JSON.stringify(entity));
    }
    if (radius !== undefined) parts.push(String(radius));
    const windowed = options.from !== undefined || options.to !== undefined;
    if (options.board !== undefined) {
      if (windowed || options.board.includes(":")) throw new Error("BOARD cannot be combined with FROM/TO or contain a colon");
      parts.push("BOARD", identifier(options.board));
    }
    if (windowed && options.entityTag === undefined) throw new Error("entityTag required with from or to");
    for (const [name, date] of [["FROM", options.from], ["TO", options.to]] as const) {
      if (date !== undefined) {
        if (!(date instanceof Date) || !Number.isFinite(date.getTime()) || date.getTime() < 0) throw new Error("Invalid ranking time range");
        parts.push(name, date.toISOString());
      }
    }
    if (options.from && options.to && options.from > options.to) throw new Error("Invalid ranking time range");
    if (options.entityTag !== undefined) {
      const tag = identifier(options.entityTag);
      if (windowed) parts.push("ENTITY", tag);
    }
    for (const [name, value] of [["LIMIT", options.limit], ["OFFSET", options.offset]] as const) {
      if (value !== undefined) {
        if (!Number.isSafeInteger(value) || value < 0 || name === "LIMIT" && value > 1000000) throw new Error("Invalid ranking pagination");
        parts.push(name, String(value));
      }
    }
    const response = await this.command(parts.join(" ")) as {ranking?: any};
    const r = response?.ranking;
    if (!r || !Number.isSafeInteger(r.count) || r.count < 0 ||
        !(r.rank === null || Number.isSafeInteger(r.rank) && r.rank >= 1 && r.rank <= r.count) ||
        !(r.score === null || typeof r.score === "number" && Number.isFinite(r.score)) ||
        (r.rank === null) !== (r.score === null)) throw new Error("Invalid ranking result (updated server required)");
    if (command === "AROUND" && !Array.isArray(r.around)) throw new Error("Missing ranking around entries");
    const around = r.around?.map((entry: any): RankedEntry => {
      if (!entry || typeof entry.entity_id !== "string" || entry.entity_id.length === 0 ||
          typeof entry.value !== "number" || !Number.isFinite(entry.value) ||
          !Number.isSafeInteger(entry.rank) || entry.rank < 1 || entry.rank > r.count) throw new Error("Invalid ranking around entry");
      return {entity_id: entry.entity_id, score: entry.value, rank: entry.rank};
    });
    return {rank: r.rank, score: r.score, count: r.count, ...(around === undefined ? {} : {around})};
  }

  // ─── Group Leaderboard ───────────────────────────────────────────────────────

  async groupLeaderboard(
    metric: string,
    groups: GroupDef[],
    options: LeaderboardOptions = {},
  ): Promise<GroupLeaderboardEntry[]> {
    const parts = [`GROUP_LEADERBOARD ${identifier(metric)}`];
    if (options.board !== undefined) parts.push(`BOARD ${identifier(options.board)}`);
    for (const g of groups) {
      parts.push(`GROUP ${JSON.stringify(`${g.name}:${g.members.join(",")}`)}`);
    }
    if (options.limit !== undefined) parts.push(`LIMIT ${options.limit}`);
    if (options.offset !== undefined) parts.push(`OFFSET ${options.offset}`);
    const result = (await this.command(parts.join(" "))) as {
      leaderboard: GroupLeaderboardEntry[];
    };
    return (result.leaderboard ?? []).map((entry: any) => ({ group: entry.entity_id, score: entry.value }));
  }

  // ─── Stats ───────────────────────────────────────────────────────────────────

  /** Lists cluster-wide metric names, optionally filtered by event tags. */
  async listMetrics(where: Record<string, string> = {}): Promise<string[]> {
    const clauses = Object.entries(where).map(([k, v]) => `${identifier(k)}=${JSON.stringify(v)}`).join(" AND ");
    const result = await this.command("METRICS" + (clauses ? ` WHERE ${clauses}` : "")) as { metric_keys: string[] };
    return result.metric_keys;
  }

  async stats(metric: string, tags: string[]): Promise<StatsResult> {
    const cmd = `STATS ${identifier(metric)} TAGS ${tags.map(identifier).join(" ")}`;
    return ((await this.command(cmd)) as { stats: StatsResult }).stats;
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
