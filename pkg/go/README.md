# FlameDB — Go SDK

Go client for FlameDB. Thread-safe via an internal mutex over a single persistent TCP connection.

## Install

```bash
go get github.com/byPixelTV/flamedb/pkg/go
```

## Usage

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/byPixelTV/flamedb/pkg/go/flamedb"
)

func main() {
	db, err := flamedb.New(flamedb.Config{
		Host:   "127.0.0.1",
		Port:   7777,
		APIKey: "flame_abc123",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()

	// single write
	err = db.Write(ctx, "kills", 1, flamedb.WriteOpts{
		LeaderboardEntity: "pixel",
		Tags:              map[string]string{"player": "pixel", "region": "eu"},
	})

	// batch write
	result, err := db.WriteBatch(ctx, []flamedb.WriteBatchItem{
		{Metric: "kills", Value: 1, Opts: flamedb.WriteOpts{LeaderboardEntity: "pixel"}},
		{Metric: "deaths", Value: 1, Opts: flamedb.WriteOpts{LeaderboardEntity: "pixel"}},
	})
	fmt.Printf("batch: accepted=%d failed=%d\n", result.Accepted, result.Failed)

	// leaderboard
	entries, err := db.Leaderboard(ctx, "kills", flamedb.LeaderboardOpts{Limit: 10})
	for _, e := range entries {
		fmt.Printf("%s: %.0f\n", e.EntityID, e.Score)
	}

	// get events
	get, err := db.Get(ctx, []string{"kills"}, flamedb.GetOpts{
		Where: map[string]string{"region": "eu"},
		Limit: 100,
		Order: "DESC",
	})
	fmt.Println(get.Events)

	// group leaderboard
	groups, err := db.GroupLeaderboard(ctx, "kills", []flamedb.GroupDef{
		{Name: "team_red", Members: []string{"pixel", "notch"}},
		{Name: "team_blue", Members: []string{"dream"}},
	}, flamedb.LeaderboardOpts{})
	_ = groups

	// stats
	stats, err := db.Stats(ctx, "kills", []string{"player", "region"})
	_ = stats
	_ = err
}
```

## API

### `flamedb.New(cfg Config) (*Client, error)`
Opens + authenticates a TCP connection. Returns an error if auth fails.

### `(*Client).Write(ctx, metric, value, opts) error`
### `(*Client).WriteBatch(ctx, items) (*BatchResult, error)`
### `(*Client).Set(ctx, metric, value, entity) error`
### `(*Client).Delete(ctx, metric, opts) error`
### `(*Client).Get(ctx, metrics, opts) (*GetResult, error)`
### `(*Client).Leaderboard(ctx, metric, opts) ([]LeaderboardEntry, error)`
### `(*Client).GroupLeaderboard(ctx, metric, groups, opts) ([]GroupLeaderboardEntry, error)`
### `(*Client).Stats(ctx, metric, tags) (*StatsResult, error)`
### `(*Client).Close() error`

## Connection recovery

After a transport failure, timeout, or invalid JSON response, the client discards
the connection. The next command reconnects and authenticates under the same
mutex used for commands. Reconnection honors the caller's context and the configured
timeout. `Close()` is permanent; subsequent commands do not reconnect.

Failed commands are never replayed automatically. A write may already have been
applied before its reply was lost, so do not blindly retry increments. Server
error responses do not invalidate an otherwise healthy connection.

## Metric discovery and player UUIDs

Requires a server supporting `METRICS` and read permission. Returns sorted, unique names across all known cluster nodes (an unavailable node causes an error, not a partial list). Empty results are `[]`.

```go
keys, err := client.ListMetrics(ctx, nil)
playerKeys, err := client.ListMetrics(ctx, map[string]string{"uuid": playerUUID})
events, err := client.Get(ctx, []string{"kills"}, flamedb.GetOpts{Where: map[string]string{"uuid": playerUUID}})
```

Store the UUID explicitly as an event tag, e.g. `WRITE kills 1 lb="<uuid>" uuid="<uuid>"`. The `lb` entity alone does not create a UUID tag. Multiple filters use AND and must match the same event. Filtered discovery includes only metrics with matching stored events; unfiltered discovery also includes leaderboard-only metrics. Discovery scans stored keys and is intended for administrative operations, not per-tick calls.

Deleting `lb="<uuid>"` resets only that all-time leaderboard entry. It does not delete historical events or reset windowed aggregates. Use tag-filtered deletion below to remove one player's historical events.

## Delete events by tag

Requires the updated server. `DELETE kills WHERE player="abc"` deletes only events with that exact tag value. Tag names are arbitrary, multiple conditions use AND, and optional FROM/TO bounds use an inclusive start and exclusive end. All secondary indexes are removed with the events, so GET, aggregates and windowed leaderboards reflect the deletion. Explicitly empty SDK filters are rejected; omit the filter only for the existing unfiltered deletion behavior.

```go
filter := map[string]string{"player": playerUUID}
keys, err := client.ListMetrics(ctx, filter)
if err != nil { return err }
for _, metric := range keys {
    if err := client.Delete(ctx, metric, flamedb.DeleteOpts{Where: filter}); err != nil {
        return err
    }
}
// For a full reset, discover all keys with ListMetrics(ctx, nil) and
// also set LeaderboardEntity: playerUUID in DeleteOpts.
```

A tag filter alone does not change all-time leaderboard scores: stored events do not record the `lb` entity, and SET can independently override its score. To reset a known entity, combine the filter with `leaderboardEntity`; this removes that **entire** all-time entry regardless of the event filter/time range, atomically with the matching events for this metric. It does not subtract event values or infer entity IDs from tag values.

The loop across metrics is not an atomic cluster-wide operation. Stop concurrent writes for the player during a complete reset; new events may otherwise arrive during or after it. Errors propagate, and completed metric deletions are not rolled back. The server uses the existing primary routing and replication path; default replication remains asynchronous. Protocol callers can request `QUORUM`.

## Generic bulk deletion and preview

`deleteByTags` (`DeleteByTags` in Go) requires a non-empty tag filter and processes discovered metrics sequentially. Optional time bounds and an explicit leaderboard entity have the same semantics as single-metric deletion. With an explicit leaderboard entity, discovery includes all metrics so leaderboard-only entries are handled too.

```go
filter := map[string]string{"account": accountID}
preview, err := client.DeleteByTags(ctx, filter, flamedb.DeleteOpts{DryRun: true})
if err != nil { return err }
fmt.Println(preview.Events, preview.LeaderboardEntries)
result, err := client.DeleteByTags(ctx, filter, flamedb.DeleteOpts{})
if err != nil { return err }
for _, item := range result.Metrics {
    if item.Error != "" { fmt.Println(item.Metric, item.Error) }
}
// Delete keeps its error-only signature. Use DeleteWithResult for counts:
one, err := client.DeleteWithResult(ctx, "requests", flamedb.DeleteOpts{Where: filter, DryRun: true})
```

Results contain the dry-run flag, total event/leaderboard-entry counts, and a per-metric list with either a result or an error. In preview mode counts mean **would be deleted**; otherwise they mean **confirmed deleted**. A missing entity and an existing zero-score entity are distinguished. Zero matches succeed with zero counts.

Discovery failures abort the call. Per-metric failures are collected while remaining metrics are attempted. Totals exclude failed/unknown outcomes: a transport or replication error may occur after the primary already applied a deletion. The helper does not retry failed mutations automatically. Go returns a partial report plus an error on context cancellation; Kotlin propagates coroutine cancellation.

Preview and execution are separate calls, not a reserved snapshot or a transaction across metrics. Concurrent writes can change the counts. Inspect `metrics` for errors before treating a preview or deletion as complete. Both require write permission; discovery additionally requires read permission.

Update servers before using these SDK methods. The preview sends `PREVIEW_DELETE <metric> [WHERE ...] [lb="..."] [FROM ...] [TO ...]`, which performs no replication or data changes. Actual `DELETE` now returns `{"delete":{"dry_run":false,"events":42,"leaderboard_entries":1}}`; preview returns the same shape with `dry_run:true`. Missing/inconsistent counters are reported as errors instead of being presented as a successful zero count.
