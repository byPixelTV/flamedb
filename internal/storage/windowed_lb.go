package storage

import (
	"encoding/binary"
	"sort"

	"github.com/byPixelTV/flamedb/internal/types"
	"github.com/cockroachdb/pebble/v2"
)

// WindowedLeaderboard computes a leaderboard directly from the
// secondary index (idx:metric:entityTag:entityID:ts -> primaryKey).
//
// Strategy for large datasets:
//   - Iterate directly over idx:metric:entityTag:* without ReadRange
//     or a heap-allocated event slice.
//   - Extract the timestamp from the last 8 bytes of the index key (zero-copy).
//   - Use db.Get for each match to load only relevant event values.
//   - Aggregate directly into a map: O(events in the window) time, O(entities) space.
func (s *Storage) WindowedLeaderboard(
	metric string,
	entityTag string,
	from, to int64,
	limit, offset int,
) ([]types.LeaderboardEntry, error) {

	prefix := indexTagPrefix(metric, entityTag)
	upperBound := make([]byte, len(prefix))
	copy(upperBound, prefix)
	upperBound[len(upperBound)-1]++ // ':' (0x3A) → ';' (0x3B)

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	sums := make(map[string]float64, 1024)
	prefixLen := len(prefix)

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		// Minimum length: prefix + at least 1 entityID byte + colon + 8 timestamp bytes.
		if len(key) < prefixLen+1+1+8 {
			continue
		}

		// Timestamp: last 8 bytes.
		ts := int64(binary.BigEndian.Uint64(key[len(key)-8:]))
		if from > 0 && ts < from {
			continue
		}
		if to > 0 && ts >= to {
			continue
		}

		// entityID: between the prefix and the last colon before the timestamp.
		entityEnd := len(key) - 1 - 8 // -1 for ':', -8 for the timestamp.
		if entityEnd <= prefixLen {
			continue
		}
		entityID := string(key[prefixLen:entityEnd])

		// Load the value from the primary event.
		primaryKey := iter.Value()
		data, closer, err := s.db.Get(primaryKey)
		if err != nil {
			continue // Event deleted or not yet flushed.
		}
		e, err := decodeEventValue(data, metric)
		closer.Close()
		if err != nil {
			continue
		}

		if e.Tags[entityTag] == entityID {
			sums[entityID] += e.Value
		}
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}

	entries := make([]types.LeaderboardEntry, 0, len(sums))
	for id, val := range sums {
		entries = append(entries, types.LeaderboardEntry{EntityID: id, Value: val})
	}

	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].Value != entries[j].Value {
			return entries[i].Value > entries[j].Value
		}
		return entries[i].EntityID < entries[j].EntityID
	})

	if offset >= len(entries) {
		return []types.LeaderboardEntry{}, nil
	}
	entries = entries[offset:]
	if limit > 0 && len(entries) > limit {
		entries = entries[:limit]
	}
	return entries, nil
}

// WindowedEntitySums sums events within a time window for each known entity ID.
// Used by GROUP_LEADERBOARD with FROM/TO.
// Known member IDs allow an exactly bounded index scan for each member,
// so no manual timestamp filtering is needed.
func (s *Storage) WindowedEntitySums(
	metric string,
	entityTag string,
	entityIDs []string,
	from, to int64,
) (map[string]float64, error) {
	sums := make(map[string]float64, len(entityIDs))

	for _, entityID := range entityIDs {
		lower := indexKey(metric, entityTag, entityID, from)
		upper := indexKey(metric, entityTag, entityID, to)

		iter, err := s.db.NewIter(&pebble.IterOptions{
			LowerBound: lower,
			UpperBound: upper,
		})
		if err != nil {
			return nil, err
		}

		var sum float64
		for iter.First(); iter.Valid(); iter.Next() {
			data, closer, err := s.db.Get(iter.Value())
			if err != nil {
				continue
			}
			e, err := decodeEventValue(data, metric)
			closer.Close()
			if err != nil {
				continue
			}
			if e.Tags[entityTag] == entityID {
				sum += e.Value
			}
		}
		iterErr := iter.Error()
		iter.Close()
		if iterErr != nil {
			return nil, iterErr
		}

		sums[entityID] = sum
	}

	return sums, nil
}

// indexTagPrefix builds the idx:metric:tagKey: prefix without an entity ID.
// Used by WindowedLeaderboard when entity IDs are not known in advance.
func indexTagPrefix(metric, tagKey string) []byte {
	buf := make([]byte, 0, 4+len(metric)+1+len(tagKey)+1)
	buf = append(buf, 'i', 'd', 'x', ':')
	buf = append(buf, metric...)
	buf = append(buf, ':')
	buf = append(buf, tagKey...)
	buf = append(buf, ':')
	return buf
}
