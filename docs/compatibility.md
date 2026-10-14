# FlameDB compatibility contract

The frozen implementation is Git commit `a7bb892b881e59d207f79ae7258ceecdd4c63ac7`.
This inventory comes from that commit's source, tests and SDKs, rather than the
older project descriptions in `v1.md` and `v2.md`. The reference checkout used by
the integration harness is `.compat/reference`; source comparisons permit only
Windows CRLF versus repository LF differences. Existing method signatures,
authentication messages, command framing and result keys remain the baseline.

## TCP and query behavior

| Surface | Frozen behavior and externally observable result | Compatibility requirement |
| --- | --- | --- |
| Connection/authentication | UTF-8 TCP, one JSON response per command terminated by LF. Greeting `{"auth":"required"}`; `AUTH <key>` replies `{"auth":"ok","name":"<configured name>"}`. Auth deadline is 10 seconds. A non-AUTH line replies `authenticate first`; an invalid key replies `invalid key` and closes. Commands on an authenticated connection are sequential; clients may pipeline. | Preserve messages, envelope shape, response ownership and one-response rule. TLS is an explicit server option. Existing SDK sockets are plaintext. |
| Permissions | READ: GET, LEADERBOARD, GROUP_LEADERBOARD, STATS, METRICS. WRITE: WRITE, SET, DELETE; PREVIEW_DELETE is a DELETE query and requires write permission. REPL_BATCH and internal routing/receipt flags require internal permission. CLUSTER_TOPOLOGY is allowed for read/internal sessions; operational CLUSTER commands require internal permission. | New ranking commands require read permission. No implicit internal access through a user API key. |
| Framing limits | Public commands up to 8 MiB; internal lines up to 9 MiB; scanner ceiling slightly above 9 MiB. Blank lines ignored. Literal CR/LF/NUL/unit separator inside a query rejected. JSON/Go-style quoted strings decode escapes, including quote, Unicode, backslash and escaped newline. | Preserve normal inputs; configurable smaller operational limits must be explicit. Avoid sending a successful result for a truncated request. |
| Errors and empty results | An error is `{"error":"message"}`. WRITE/SET success is `{}`. Result fields use `omitempty`: empty single-metric events, leaderboards and series may be omitted. Public multi-metric merging normalizes empty event/series lists to []; public METRICS emits metric_keys:[]. DELETE has a `delete` object; batches have counters. | Preserve field names and old SDK empty-result interpretation. No extra wrapping of old commands. |
| Raw event JSON casing | Frozen server serializes storage.Event as uppercase `Timestamp/Metric/Value/Tags`; the frozen Go SDK decodes those case-insensitively. Frozen Kotlin/TypeScript raw-event models require lowercase keys, a pre-existing server/SDK discrepancy hidden by fixture-only tests. | User-approved additive correction emits lowercase timestamp/metric/value/tags aliases together with all unchanged uppercase fields. Exact integer timestamp digits are shared. Untagged lowercase tags is {}, while uppercase Tags retains null. |
| WRITE | `WRITE metric value [lb="entity"] [lb.board="entity"] [tag="value"] [ts=nanoseconds] [QUORUM]`. Finite signed float64; nonnegative timestamp; zero means assign a strictly increasing per-executor current timestamp. One raw event plus increments on each requested default/named board; leaderboard IDs are separate from raw event tags. | Persist event and board mutations together before successful acknowledgement. Preserve signed and zero values. |
| Duplicate timestamp | Raw event identity is `(metric,timestamp)`. A later write replaces that event and its old tag index entries, even with different tags. Each accepted WRITE still increments its requested absolute boards; replacing an event does not reverse earlier board increments. | PostgreSQL UPSERT must preserve this separation between event history and absolute boards. Timestamp generation across nodes is operationally distinct from explicit overwrite semantics. |
| SET | `SET metric value lb="entity"` or named `lb.board`. Replaces absolute board score, including zero or a negative value; emits no raw event. Missing target board/entity is invalid. | Preserve absolute mutation and empty `{}` acknowledgement. |
| GET | Single metric returns `events`; comma-separated metrics return `metrics`. WHERE combines exact tag equalities with AND; missing tags do not equal empty values. Time interval is `[FROM,TO)`, default FROM=0 and omitted TO unbounded. Parser defaults LIMIT=10/OFFSET=0/ORDER=DESC. Explicit LIMIT 0 means unlimited. Sorting/pagination apply per metric. | Preserve order, exact nanoseconds, half-open boundaries and independent per-metric pagination. |
| Query dates | RFC3339Nano, UTC `YYYY-MM-DD`, `now`, and signed relative times. Calendar `y`/`mo` differ from fixed durations; `m` means minute unless the duration contains a calendar unit. Negative timestamps, reversed ranges and invalid durations fail. Equal endpoints select nothing. | Use the unchanged parser; resolve relative dates per request. |
| GET aggregates | COUNT counts events; SUM sums signed values; AVG is sum/count and zero for no events. Result `aggregate:{type,value,count}` or per-metric `aggregates`. LIMIT/OFFSET/ORDER are ignored for aggregate input. A nonfinite derived sum/average produces `response serialization failed`, even though each stored value is finite. | Do not aggregate only the default top 10 events. Preserve the same overflow error for scalar, series, window and group results; COUNT remains valid. |
| GET series | `GROUP BY duration` emits nonempty UTC-aligned buckets as `{ts,value,count}`; default aggregate SUM. Fixed buckets align to Unix epoch; calendar buckets follow UTC calendar boundaries. Single `series`, multi `series_by_metric`; input pagination ignored. | Preserve bucket boundaries, absent empty buckets and aggregate counts. |
| All-time LEADERBOARD | Default board or `BOARD name`; descending score. Nonnegative-score ties sort entity ID ascending; negative-score ties sort ID descending because the frozen engine traverses its negative key range backward. Offset and limit select a page. Scores can be negative or zero. Scores exist independently of events and their tags. WHERE and ORDER do not change leaderboard selection. | Preserve exact deterministic ordering, named-board isolation and ties across pages, including the legacy negative-tie reversal. |
| Windowed LEADERBOARD | FROM or TO requires `ENTITY tag-key`. Sums raw event values by that tag over `[FROM,TO)`; skips missing/empty IDs. BOARD plus FROM/TO is invalid. Includes entities whose signed sum is zero. Stable score descending / ID ascending order. | Calculate from selected events; SET never appears in event windows. |
| GROUP_LEADERBOARD | `GROUP "name:member,member"` per group; sums requested members on the selected board/window. Includes specified groups with zero sum and repeated member IDs count repeatedly. Overlapping groups remain independent. Descending score and ascending group name; normal pagination. Empty group list returns an empty result. | Preserve ad-hoc grouping and no implicit distinct-member rewrite. |
| DELETE | No board target: removes matching raw events in `[FROM,TO)`. With `lb`/named target and no WHERE: removes entire all-time board entries and leaves events, even if a time interval is supplied. With target plus WHERE: removes both matching events and complete target entries. Raw event deletion never subtracts from absolute boards. | Preserve the combined operation and explicit effect counts. |
| Delete preview | `PREVIEW_DELETE` or DELETE DRY_RUN returns `delete:{dry_run,events,leaderboard_entries}` with the same selection and no mutation. DRY_RUN cannot combine with QUORUM/internal replica flags. No-WHERE DELETE may clear an entire metric's events. | Preserve distinction between preview counts and actual effects. Empty explicit SDK delete filter remains invalid. |
| METRICS | Unfiltered list includes event metrics and metrics with only board entries; sorts and deduplicates cluster-wide keys as `metric_keys`. WHERE selects metrics with at least one matching raw event. Empty public lists return metric_keys:[]. Reserved storage/replication key prefixes do not become metrics. | Preserve filtered event semantics and default/named board discovery. |
| STATS | `stats:{metric,tag_stats:[{tag_key,cardinality}]}` for requested tag keys in request order. Cardinality tracks distinct historically observed tag values, asynchronously; deletion does not decrement that history. Zero is valid before async flush. Index choice prefers higher distinct count, tie-breaking by tag key. | Preserve the envelope and historical counting; immediate post-write counts must not be treated as a synchronously fresh guarantee. |
| WRITE_BATCH | Newline `WRITE_BATCH [QUORUM]`, WRITE lines, `END`; one response `{ok,accepted,failed,errors?}`. Original zero-based indexes retained; failures may coexist with applied writes. Only WRITE items. Local plain writes grouped; board writes preserve their original deltas and events. Header/per-item quorum supported. Limits: 10,000 items / 8 MiB total / 30-second frame deadline. | Keep partial-success semantics; no all-or-nothing claim, dropped items, per-event response lines or silent retries. |
| Internal replication | Durable Pebble outbox plus internal `__op` receipts prevents replayed internal increments; REPL_BATCH can carry WRITE/SET/DELETE and returns exactly once. User writes do not carry an idempotency key. Baseline clustered acknowledgement can fail after a local mutation if replica delivery/quorum fails. | Old mutation errors can have an unknown outcome. PostgreSQL becomes authority in external mode; existing peer replication is not an external durability substitute. |
| QUORUM | Frozen cluster quorum uses configured majority and rejects application-level replica failure; RF does not shrink to healthy-node count. External storage mode must reject QUORUM before mutation when it cannot prove the requested synchronous topology guarantee. | This explicit rejection is an intentional external-mode difference, not a claim that quorum succeeded. Consumers that request quorum must handle it. |
| Identifiers | Metric/tag identifiers support colon namespaces and UTF-8. Metrics reject comma/quote/equality/space conflicts and reserved storage prefixes. Parser tag/entity-tag names reject quote/equality/space conflicts but can contain comma; SDK shared validators also reject comma. Boards reject colon, quote, equality, space and unit separator; SDK board validation is stricter about comma. Entity/tag values are quoted and may contain escaped control characters. | Preserve valid namespaces and quoted values. New ranking methods quote entities rather than treating them as identifiers. |

## SDK public contract inventory

No existing option type, constructor, method parameter, return type or exception
constructor was removed or changed. Old call sites compile against the updated
SDKs. Old SDK source is also compiled independently against the frozen checkout
and used as a TCP client of the new backend by `TestFrozenLegacySDKsAgainstNextgen`.
`TestFrozenRawEventSDKBaselineLimitation` separately reproduces the original
raw-event defect against the real frozen server: TypeScript lacks timestampNs;
Kotlin rejects missing timestamp/metric/value. Its successful negative-control
result confirms that baseline defect, not successful original raw decoding.
The approved aliases allow unchanged consumers to receive correctly typed events.

| SDK | Frozen entry points and signatures | Result/options/transport contracts |
| --- | --- | --- |
| Go construction/lifecycle | `New(Config) (*Client,error)`; `Close() error`. Every network operation takes `context.Context` first. | Config Host, Port, APIKey, Timeout, ReadConnections, WriteConnections, OnCommandTiming. Omitted pools: two reserved reads, one write. WriteConnections=1 with ReadConnections=0 shares one socket. Default total deadline 5s, includes queue/connect/auth/I/O. No reconnect/replay of a failed mutation; later calls can reconnect. 4 MiB scanner response ceiling. |
| Go write/delete | `Write(ctx,metric,value,WriteOpts) error`; `WriteBatch(ctx,[]WriteBatchItem) (*BatchResult,error)`; `Set(ctx,metric,value,entity) error`; `Delete(ctx,metric,DeleteOpts) error`; `DeleteWithResult(ctx,metric,DeleteOpts) (*DeleteResult,error)`; `DeleteByTags(ctx,where,DeleteOpts) (*DeleteByTagsResult,error)`. | WriteOpts includes default/named entities, tags, TimestampNs and Quorum. DeleteOpts includes DryRun, Where, default entity, From/To. Delete keeps error-only compatibility. Cross-metric deletion retains individual errors and totals confirmed effects; it is not atomic. |
| Go reads | `Get(ctx,[]string,GetOpts) (*GetResult,error)`; `Leaderboard(ctx,metric,LeaderboardOpts) ([]LeaderboardEntry,error)`; `GroupLeaderboard(ctx,metric,[]GroupDef,LeaderboardOpts) ([]GroupLeaderboardEntry,error)`; `Stats(ctx,metric,[]string) (*StatsResult,error)`; `ListMetrics(ctx,where) ([]string,error)`. | GetOpts Where, From/To time.Time, Limit/Offset, Order. Legacy Go formats dates to UTC day precision. LeaderboardOpts Limit, Offset, Board; no legacy window options or aggregate convenience methods. EntityID/Group and Score map wire `entity_id`/`value`. Event.Timestamp is int64. Zero-valued limit/offset fields are omitted, retaining server defaults. |
| TypeScript construction/lifecycle | `new FlameDB(FlameDBConfig)`; `connect():Promise<void>`; `disconnect():void`. Methods return promises. | Default timeout 5000 ms, pipeline 64 per connection, two reads/one write. Reserved pools, total admission/connect/auth/request deadlines. Disconnect rejects active/queued work; later calls recreate pools. Mutations are not replayed after ambiguous failure. Existing errors remain Error objects. Buffer limit 64 MiB. Timing callback cannot fail commands. |
| TypeScript mutations | `write(metric,value,WriteOptions?):Promise<void>`; `writeBatch(WriteBatchItem[]):Promise<BatchResult>`; `set(metric,value,entity?):Promise<void>`; `delete(metric,DeleteOptions?):Promise<DeleteResult>`; `deleteByTags(where,options?):Promise<DeleteByTagsResult>`. | WriteOptions supports default/named entities, tags, timestamp number/bigint and quorum. DeleteOptions includes default entity, Date from/to, where and dryRun. Structured partial per-metric deletion report; no `deleteWithResult` method existed. |
| TypeScript reads | `get(string|string[],GetOptions?):Promise<GetResult>`; `leaderboard(metric,LeaderboardOptions?):Promise<LeaderboardEntry[]>`; `groupLeaderboard(metric,GroupDef[],LeaderboardOptions?):Promise<GroupLeaderboardEntry[]>`; `listMetrics(where?):Promise<string[]>`; `stats(metric,string[]):Promise<StatsResult>`. | Legacy GetOptions Date from/to formats UTC dates; LeaderboardOptions limit/offset/board. Maps wire value to score, group entity_id to group. Empty missing leaderboard becomes []. Legacy timestamp number may lose ns precision; exact timestampNs/tsNs bigint companion fields remain. No legacy aggregate or window convenience API. |
| Kotlin construction/lifecycle | Suspended `FlameDB.connect(FlameDBConfig):FlameDB`; `close():Unit`/AutoCloseable; `FlameDB.buffered(config,Path,BufferedWriteConfig):BufferedFlameDB`. | Existing JVM config/data class and exception constructors retained. Defaults two read/one write sockets, 5000 ms total deadline, 1024 queued reads/writes independently, 8 MiB strict UTF-8 response limit. Native cancellation retained with structured failure diagnostics. |
| Kotlin mutations | Suspended `write(metric,Double,WriteOptions=...)`; `writeBatch(List<WriteBatchItem>):BatchResult`; `set(metric,Double,entity:String?=null)`; `delete(metric,entity?,from:LocalDate?,to:LocalDate?,where?,dryRun=false):DeleteResult`; `deleteByTags(where,entity?,from?,to?,dryRun=false):DeleteByTagsResult`. | Existing default argument order and JVM shapes retained. Finite values required. FlexibleDoubleSerializer supports prior numeric result formats. WriteOptions default/named entities, tags, Long timestamp and quorum unchanged. |
| Kotlin reads | Suspended `get(vararg metrics,options:GetOptions=...)`; `getAggregate(Aggregate,vararg metrics,options=...)`; `getSeries(groupBy,vararg metrics,aggregate?,options=...)`; `getSum/getCount/getAvg(vararg metrics,options=...)`; `leaderboard(metric,LeaderboardOptions=...)`; `groupLeaderboard(metric,List<GroupDef>,LeaderboardOptions=...)`; `listMetrics(where=emptyMap())`; `stats(metric,vararg tags)`. | GetResult contains events/metrics/aggregate(s)/series/by-metric series. Long timestamps preserve ns. GetOptions from/to are strings supporting relative times. LeaderboardOptions limit/offset/from/to/entityTag/board already exists; windows require entityTag. Empty absent list fields become empty lists/maps. |
| Kotlin diagnostics | FlameDBException keeps `(message)` and `(message,cause)` constructors plus kind, command, mutating, executionStatus, phase, cause; `Throwable.flameDBFailure()` retains cancellation diagnostics; CommandTiming adds failure information. | OVERLOADED/POOL_WAIT_TIMEOUT/CONNECTION/AUTHENTICATION/SERVER_REJECTION/INVALID_RESPONSE/RESPONSE_TOO_LARGE/REQUEST_TIMEOUT/REQUEST_FAILED/REQUEST_CANCELLED/CLIENT_CLOSED remain. NOT_SENT differs from UNKNOWN; RESPONSE_RECEIVED proves validated response receipt, not atomic batch success. |

## Kotlin buffered journal contract

| Surface | Frozen behavior that remains |
| --- | --- |
| Factory/admission | Offline-safe synchronous factory, one exclusive file-lock owner per directory. `record(metric,value,options):Boolean` snapshots original values, tags, named boards, timestamp and quorum; true acknowledges an OS-cache journal append, not remote application or per-event fsync. False means explicit local rejection with bounded lastFailure/statistics. No automatic conversion of direct writes to buffered writes. |
| Original-event preservation | Native batches retain each original event; signed deltas/tags are never merged. Defaults batchSize=128, flushMs=100, retryMs=1000, maxJournalBytes=256 MiB, shutdownMs=5000. Bounds 512 items, 512 KiB batch, 64 KiB record, 256 cached pending paths, one sender. Budget includes audit data and never evicts acknowledged records. |
| Seal/send/retry | Independent flusher fsyncs before sending. Only explicit NOT_SENT permits automatic retry with bounded backoff up to 30s. UNKNOWN, response corruption/limit errors, server rejection and partial acknowledgements never authorize replay. Native batch counters/indexes are validated before callback confirmation. |
| Journal state | `.open` partial unsent; `.pending` definitely unsent; `.inflight` claimed; `.done` full validated acknowledgement; `.uncertain` ambiguous; `.partial` plus `.result.json` partial results. Recovery/audit extensions `.damaged/.recovered/.rebuild` are never blindly replayed. CRC32/UTF-8/size/framing validation preserves incomplete material for reconciliation. |
| Restart | Definitely unsent complete frames resume; inflight becomes uncertain. Crash between remote ACK and local completion cannot provide exactly once. Local journal record IDs are not server deduplication IDs. Payload and exact event identities remain available through bounded `auditFiles`/`inspectAudit`. |
| Reads/callbacks | getSum/leaderboard/groupLeaderboard delegate to reserved sockets; directClient exposes other direct operations without flushing. Buffered reads are eventually consistent with admission. `recordThen` uses bounded best-effort callbacks (default 128, confirmationMs=5000); callback errors/cancellation/deadlines never resubmit events. |
| Shutdown | Suspended idempotent shutdown stops admission/flusher, seals and drains for the configured budget, cancels I/O, retains unsent/uncertain data and releases ownership; blocking close/use delegates. Filesystem stalls are not bounded wall-clock guarantees. |

## Config, storage and audit boundaries

The frozen YAML sections are `auth` (keys, internal_key), `server` (host, port,
advertise_addr, node_id, data_path), `cluster` (seeds, replication_factor,
read_policy, replication_queue_size, fanout_queue_size), and `storage.compression`
(none/snappy/zstd). `FLAMEDB_INTERNAL_KEY` overrides the configured internal key
and is deliberately omitted from saved YAML. Keys must be nonempty, unique,
nonconflicting and free of literal newlines; clustered mode requires the shared
internal key. Config Save uses a 0600 temporary file and rename. New storage,
TLS, HTTP and limit fields are opt-in additions; the old Pebble path remains
available. PostgreSQL is authoritative in external mode and Redis is rebuildable.

Legacy on-disk state includes raw event and tag indexes, absolute/named boards,
historical cardinality, durable replication outbox and receipt keys. Existing
Pebble automatic-migration tests require a preserved backup before format upgrade,
including locked/corrupt/incomplete-checkpoint failure behavior. Switching storage
engines does not automatically import an existing Pebble data directory. The
compatibility contract preserves public data behavior; it does not imply binary
compatibility between PostgreSQL schema and Pebble SSTs or reuse peer rebalance
KV payloads across storage engines.

| Existing evidence source | What it actually establishes |
| --- | --- |
| internal/query tests | Parser/routing flags, escaped/malformed inputs, native batches, replica receipt restart, namespaced identifiers, multiple boards, filtered deletion, UTC windows and buckets. |
| internal/storage tests | Primary/tag index isolation, historical overwrite/latest cache after restart, snapshot window concurrency, signed sums/pages, legacy codecs, corruption errors, memory bounds and Pebble migration/backup guards. |
| internal/aggregates tests | Signed/zero scores and concurrent absolute/increment operations. |
| internal/server tests | Auth/internal authorization, single replica-batch response, mixed mutations, two-node forwarding/quorum, metric discovery, refreshed/cleared response deadlines and close on failed reply. |
| internal/cluster/config tests | Durable outbox put/delete/recovery, replica application errors/configured-majority quorum checks and environment-secret save exclusion. |
| SDK_GATES.md and SDK test suites | TCP-fixture transport concurrency, reserved reads, total deadlines/cancellation, recovery and no ambiguous-mutation replay. These are SDK tests, not a live storage throughput benchmark. |
| pkg/kotlin BUFFERED/HARDENING results | Bounded journal/recovery/unknown outcomes/callbacks/overload and TCP fixture stress. Their recorded throughput is not evidence of database/server ingestion rate, durability, HA or production heap use. |
| internal/compat differential harness | Selected same-command real TCP responses versus the frozen server. Exact timestamps allow direct response comparison. Uncovered valid inputs remain outside its measured corpus. |
| internal/compat legacy SDK harness | Unchanged source compiled as independent Go/TypeScript/Kotlin consumers and exercised against the real PostgreSQL/Redis server, including Kotlin event windows/aggregates and buffered confirmation/shutdown. It does not exhaust every argument combination or outage. |

## Additive ranking API

All ranks are exact one-based ordinal positions in the selected leaderboard's
score-descending order. All-time negative-score ties preserve descending entity
IDs; nonnegative, window and group ties use ascending IDs. Tied scores occupy
different deterministic positions.
An entity present with zero score has a rank and score zero; an absent entity
has null rank/score. Count includes every entity in the selected board/window
and does not shrink when LIMIT/OFFSET are supplied. AROUND retains global ranks.

Wire commands are `RANK metric "entity"`, `SCORE metric "entity"`,
`AROUND metric "entity" radius`, and `LEADERBOARD_COUNT metric`. They accept
the existing BOARD/FROM/TO/ENTITY/LIMIT/OFFSET suffix. BOARD and time windows
cannot be combined; a window requires ENTITY. Results use
`{"ranking":{"rank":2,"score":0,"count":3,"around":[{"entity_id":"a","value":1,"rank":1}]}}`.
The around list is required for AROUND, including [] when the entity is absent;
other commands may omit it. SDKs reject missing/inconsistent envelopes, preserving
old-server errors instead of fabricating a zero or null success.

| SDK | New API | Selection options |
| --- | --- | --- |
| Go | GetRank -> *int64; GetScore -> *float64; GetAroundEntity -> []RankedEntry; GetLeaderboardCount -> int64; all also return error and take context first. | RankingOpts embeds unchanged LeaderboardOpts, adds From/To time.Time and EntityTag. New dates preserve RFC3339Nano precision. |
| TypeScript | getRank -> Promise<number\|null>; getScore -> Promise<number\|null>; getAroundEntity -> Promise<RankedEntry[]>; getLeaderboardCount -> Promise<number>. | RankingOptions extends unchanged LeaderboardOptions with Date from/to and entityTag. Unsafe integer rank/count responses are rejected. |
| Kotlin | Public suspend extension functions getRank -> Long?; getScore -> Double?; getAroundEntity -> List<RankedEntry>; getLeaderboardCount -> Long. Import `dev.bypixel.flamedb.*` or the individual extensions. | Reuses existing LeaderboardOptions. Internal bridge retains transport decoding/diagnostics and reserved read pools. Existing JVM member method signatures are untouched. |

The new methods require the updated server and are direct reads. They do not
flush buffered writes. New Kotlin operations on a buffered client use
`buffered.directClient().getRank(...)`; admission can still be pending. SDK wire
tests cover escaping, options, exact rank interpretation, zero/null distinction,
read classification and rejection of malformed/old-server results. Production
performance and failure-recovery claims require separate real backend evidence.

The TypeScript SDK is also exercised on Bun 1.4.1: its 20 TCP-fixture tests run
with `bun test`, and `node internal/compat/sdk/bun-wire.cjs` hosts an independent
TCP fixture while every SDK operation executes in Bun. The latter checks exact
nanosecond timestamps, reserved ranking reads during an ambiguous write timeout,
and verifies that the write was sent once. The real-backend SDK oracle compiles
the unchanged frozen TypeScript source and runs both frozen and additive consumers
on Node and Bun. These runtime checks require local socket access; a restricted
sandbox's connection refusal is not evidence of SDK behavior.
