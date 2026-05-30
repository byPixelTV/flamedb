package storage

import (
	"encoding/binary"
	"sort"

	"github.com/byPixelTV/flamedb/internal/types"
	"github.com/cockroachdb/pebble"
)

// WindowedLeaderboard berechnet einen Leaderboard on-the-fly direkt aus dem
// Sekundärindex (idx:metric:entityTag:entityID:ts → primaryKey).
//
// Strategie für Billionen Rows:
//   - Direkter Pebble-Iterator über idx:metric:entityTag:* — kein ReadRange,
//     keine Event-Slice im Heap.
//   - Timestamp wird aus den letzten 8 Bytes des Indexkeys extrahiert (zero-copy).
//   - db.Get pro Match holt nur den Wert der relevanten Events.
//   - Aggregation direkt in eine Map — O(Events im Fenster) Zeit, O(Entities) Speicher.
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
		// Mindestlänge: prefix + mindestens 1 Byte entityID + ':' + 8 Byte ts
		if len(key) < prefixLen+1+1+8 {
			continue
		}

		// Timestamp: letzte 8 Bytes
		ts := int64(binary.BigEndian.Uint64(key[len(key)-8:]))
		if from > 0 && ts < from {
			continue
		}
		if to > 0 && ts >= to {
			continue
		}

		// entityID: zwischen prefix und letztem ':' vor dem timestamp
		entityEnd := len(key) - 1 - 8 // -1 für ':', -8 für timestamp
		if entityEnd <= prefixLen {
			continue
		}
		entityID := string(key[prefixLen:entityEnd])

		// Wert aus primärem Event laden
		primaryKey := iter.Value()
		data, closer, err := s.db.Get(primaryKey)
		if err != nil {
			continue // Event gelöscht oder noch nicht geflusht
		}
		e, err := decodeEventValue(data, metric)
		closer.Close()
		if err != nil {
			continue
		}

		sums[entityID] += e.Value
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

// WindowedEntitySums summiert Events im Zeitfenster pro bekannter entity-ID.
// Genutzt von GROUP_LEADERBOARD mit FROM/TO.
// Da die Member-IDs bekannt sind, nutzt jeder einen exakt gebundenen Index-Scan —
// kein manuelles Timestamp-Filtering nötig.
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
			sum += e.Value
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

// indexTagPrefix baut das Prefix idx:metric:tagKey: ohne entityID.
// Genutzt für WindowedLeaderboard wenn alle entityIDs unbekannt sind.
func indexTagPrefix(metric, tagKey string) []byte {
	buf := make([]byte, 0, 4+len(metric)+1+len(tagKey)+1)
	buf = append(buf, 'i', 'd', 'x', ':')
	buf = append(buf, metric...)
	buf = append(buf, ':')
	buf = append(buf, tagKey...)
	buf = append(buf, ':')
	return buf
}
