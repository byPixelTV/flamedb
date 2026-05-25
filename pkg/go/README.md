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
