package aggregates

import (
	"encoding/binary"
	"encoding/json"
	"math"
	"sync"

	"github.com/cockroachdb/pebble"
)

type LeaderboardEntry struct {
	EntityID string // player id, server id, whatever
	Value    float64
}

type Leaderboard struct {
	db *pebble.DB
	mu sync.Map // entity-level locking: "metric:entityID" → *sync.Mutex
}

func New(db *pebble.DB) *Leaderboard {
	return &Leaderboard{db: db}
}

// key format: lb:metric:inverted_score:entity
// inverted score damit pebble range scan automatisch descending sorted ist
func lbKey(metric, entityID string, value float64) []byte {
	inverted := math.MaxUint64 - math.Float64bits(value)
	score := make([]byte, 8)
	binary.BigEndian.PutUint64(score, inverted)
	return []byte("lb:" + metric + ":" + string(score) + ":" + entityID)
}

func (l *Leaderboard) lockFor(metric, entityID string) *sync.Mutex {
	key := metric + ":" + entityID
	mu, _ := l.mu.LoadOrStore(key, &sync.Mutex{})
	return mu.(*sync.Mutex)
}

func (l *Leaderboard) Increment(metric, entityID string, delta float64) error {
	mu := l.lockFor(metric, entityID)
	mu.Lock()
	defer mu.Unlock()

	current, _ := l.Get(metric, entityID)
	oldKey := lbKey(metric, entityID, current)
	newValue := current + delta
	newKey := lbKey(metric, entityID, newValue)

	batch := l.db.NewBatch()
	batch.Delete(oldKey, pebble.Sync)
	entry := LeaderboardEntry{EntityID: entityID, Value: newValue}
	val, _ := json.Marshal(entry)
	batch.Set(newKey, val, pebble.Sync)
	return batch.Commit(pebble.Sync)
}

func (l *Leaderboard) Get(metric, entityID string) (float64, error) {
	// scan to find current entry for this entity
	prefix := []byte("lb:" + metric + ":")
	iter, err := l.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix, 0xFF),
	})
	if err != nil {
		return 0, err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		var entry LeaderboardEntry
		if err := json.Unmarshal(iter.Value(), &entry); err != nil {
			continue
		}
		if entry.EntityID == entityID {
			return entry.Value, nil
		}
	}
	return 0, nil
}

func (l *Leaderboard) Delete(metric, entityID string) error {
	current, err := l.Get(metric, entityID)
	if err != nil {
		return nil // existiert nicht, okay
	}
	key := lbKey(metric, entityID, current)
	return l.db.Delete(key, pebble.Sync)
}

func (l *Leaderboard) TopN(metric string, limit, offset int) ([]LeaderboardEntry, error) {
	prefix := []byte("lb:" + metric + ":")
	iter, err := l.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix, 0xFF),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var results []LeaderboardEntry
	i := 0
	for iter.First(); iter.Valid(); iter.Next() {
		if i < offset {
			i++
			continue
		}
		if len(results) >= limit {
			break
		}
		var entry LeaderboardEntry
		if err := json.Unmarshal(iter.Value(), &entry); err != nil {
			continue
		}
		results = append(results, entry)
		i++
	}
	return results, iter.Error()
}
