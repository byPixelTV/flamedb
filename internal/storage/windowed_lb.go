package storage

import (
	"container/heap"
	"fmt"
	"math"
	"sort"

	"github.com/byPixelTV/flamedb/internal/types"
	"github.com/cockroachdb/pebble/v2"
)

// WindowedLeaderboard scans primary events in [from, to). Primary keys are
// time-ordered, unlike the entity-first tag index: historical events outside
// the window are never visited and there is no random Get per matching event.
// The iterator supplies a consistent read view, including unflushed writes.
func (s *Storage) WindowedLeaderboard(metric, entityTag string, from, to int64, limit, offset int) ([]types.LeaderboardEntry, error) {
	if limit < 0 || offset < 0 {
		return nil, fmt.Errorf("invalid pagination")
	}
	if to <= 0 {
		to = math.MaxInt64
	}
	if from < 0 {
		from = 0
	}
	if from >= to {
		return []types.LeaderboardEntry{}, nil
	}
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: eventKey(metric, from),
		UpperBound: eventKey(metric, to),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	// A byte-slice map lookup does not allocate. Copy an ID only when it is
	// first seen; never retain iterator-owned bytes beyond the current row.
	entities := make(map[string]int)
	var sums []types.LeaderboardEntry
	for valid := iter.First(); valid; valid = iter.Next() {
		data, err := iter.ValueAndErr()
		if err != nil {
			return nil, err
		}
		id, value, found, err := eventEntityValue(data, entityTag)
		if err != nil {
			return nil, err
		}
		if !found || len(id) == 0 {
			continue
		}
		if i, exists := entities[string(id)]; exists {
			sums[i].Value += value
		} else {
			entity := string(id)
			entities[entity] = len(sums)
			sums = append(sums, types.LeaderboardEntry{EntityID: entity, Value: value})
		}
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return leaderboardPage(sums, limit, offset), nil
}

func leaderboardBefore(a, b types.LeaderboardEntry) bool {
	if a.Value != b.Value {
		return a.Value > b.Value
	}
	return a.EntityID < b.EntityID
}

// Keep only offset+limit candidates when selecting a small page. Ties remain
// deterministic and pagination never retains a whole leaderboard backing array.
func leaderboardPage(sums []types.LeaderboardEntry, limit, offset int) []types.LeaderboardEntry {
	if offset >= len(sums) {
		return []types.LeaderboardEntry{}
	}
	k := len(sums)
	if limit > 0 && limit < k-offset {
		k = offset + limit // Cannot overflow: bounded by len(sums).
	}
	if k < len(sums)/2 {
		selected := leaderboardHeap(append([]types.LeaderboardEntry(nil), sums[:k]...))
		heap.Init(&selected)
		for _, entry := range sums[k:] {
			if leaderboardBefore(entry, selected[0]) {
				selected[0] = entry
				heap.Fix(&selected, 0)
			}
		}
		sums = selected
	}
	sort.Slice(sums, func(i, j int) bool { return leaderboardBefore(sums[i], sums[j]) })
	page := make([]types.LeaderboardEntry, k-offset)
	copy(page, sums[offset:k])
	return page
}

// Worst candidate first so replacement is O(log(offset+limit)).
type leaderboardHeap []types.LeaderboardEntry

func (h leaderboardHeap) Len() int           { return len(h) }
func (h leaderboardHeap) Less(i, j int) bool { return leaderboardBefore(h[j], h[i]) }
func (h leaderboardHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *leaderboardHeap) Push(v any)        { *h = append(*h, v.(types.LeaderboardEntry)) }
func (h *leaderboardHeap) Pop() any {
	last := len(*h) - 1
	v := (*h)[last]
	*h = (*h)[:last]
	return v
}

// WindowedEntitySums retains selective member index scans for small, explicitly
// named groups. A shared snapshot keeps index references and event values from
// different members consistent during concurrent writes/deletes.
func (s *Storage) WindowedEntitySums(metric, entityTag string, entityIDs []string, from, to int64) (map[string]float64, error) {
	sums := make(map[string]float64, len(entityIDs))
	if to <= 0 {
		to = math.MaxInt64
	}
	if from < 0 {
		from = 0
	}
	snapshot := s.db.NewSnapshot()
	defer snapshot.Close()
	iter, err := snapshot.NewIter(nil)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	for _, entityID := range entityIDs {
		if _, seen := sums[entityID]; seen {
			continue
		}
		sums[entityID] = 0
		if from >= to {
			continue
		}
		lower := indexKey(metric, entityTag, entityID, from)
		upper := indexKey(metric, entityTag, entityID, to)
		iter.SetBounds(lower, upper)
		var sum float64
		for valid := iter.First(); valid; valid = iter.Next() {
			// Values may contain ':'; discard longer entity IDs whose keys
			// happen to fall inside this entity's timestamp bounds.
			if len(iter.Key()) != len(lower) {
				continue
			}
			key, err := iter.ValueAndErr()
			if err != nil {
				return nil, err
			}
			data, closer, err := snapshot.Get(key)
			if err == pebble.ErrNotFound {
				continue // Stale secondary index from a historical overwrite.
			}
			if err != nil {
				return nil, err
			}
			id, value, found, decodeErr := eventEntityValue(data, entityTag)
			if decodeErr == nil && found && string(id) == entityID {
				sum += value
			}
			closer.Close()
			if decodeErr != nil {
				return nil, decodeErr
			}
		}
		if err := iter.Error(); err != nil {
			return nil, err
		}
		sums[entityID] = sum
	}
	return sums, nil
}
