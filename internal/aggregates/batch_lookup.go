package aggregates

import (
	"encoding/json"
	"fmt"

	"github.com/byPixelTV/flamedb/internal/keyspace"
	"github.com/cockroachdb/pebble/v2"
)

// LockedBatchLookup shares a legacy scan among new entities on the same board.
// The caller must hold the entity stripes for every target for its whole lifetime.
// Cache only requested entities, never the whole board or scores across batches.
type LockedBatchLookup struct {
	lb      *Leaderboard
	targets map[string]map[string]struct{}
	legacy  map[string]map[string]float64
	stats   *LookupStats
}

func (l *Leaderboard) NewLockedBatchLookup(targets map[string]map[string]struct{}, stats *LookupStats) *LockedBatchLookup {
	return &LockedBatchLookup{lb: l, targets: targets, legacy: make(map[string]map[string]float64), stats: stats}
}

func (lookup *LockedBatchLookup) Get(metric, entity string) (float64, error) {
	if _, ok := lookup.targets[metric][entity]; !ok {
		return 0, fmt.Errorf("score lookup target is not locked")
	}
	// Always prefer the current entity index over a legacy sorted-score entry.
	data, closer, err := lookup.lb.db.Get(lbEntityKey(metric, entity))
	if err == nil {
		value, valid := decodeFloat64(data)
		closer.Close()
		if valid {
			return value, nil
		}
	} else if err != pebble.ErrNotFound {
		return 0, err
	}
	if values, ok := lookup.legacy[metric]; ok {
		return values[entity], nil
	}

	prefix := []byte("lb:" + keyspace.Component(metric) + ":")
	iter, err := lookup.lb.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: []byte("lb:" + keyspace.Component(metric) + ";")})
	if err != nil {
		return 0, err
	}
	defer iter.Close()
	if lookup.stats != nil {
		lookup.stats.LegacyScans++
	}
	values := make(map[string]float64)
	for iter.First(); iter.Valid(); iter.Next() {
		if lookup.stats != nil {
			lookup.stats.LegacyEntries++
		}
		var entry LeaderboardEntry
		if json.Unmarshal(iter.Value(), &entry) != nil {
			continue
		}
		if _, wanted := lookup.targets[metric][entry.EntityID]; !wanted {
			continue
		}
		// Match Get's first matching legacy row even if old data has stale keys.
		if _, already := values[entry.EntityID]; !already {
			values[entry.EntityID] = entry.Value
		}
		if len(values) == len(lookup.targets[metric]) {
			break
		}
	}
	if err := iter.Error(); err != nil {
		return 0, err
	}
	lookup.legacy[metric] = values
	return values[entity], nil
}
