package aggregates

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"sync"

	"github.com/byPixelTV/flamedb/internal/types"
	"github.com/cockroachdb/pebble/v2"
)

// LeaderboardEntry ist ein Alias auf types.LeaderboardEntry für Rückwärtskompatibilität.
type LeaderboardEntry = types.LeaderboardEntry

type Leaderboard struct {
	db *pebble.DB
	mu [1024]sync.Mutex // Bounded striped locks for entity mutations.
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

func lbEntityKey(metric, entityID string) []byte {
	return []byte("lb-entity:" + metric + ":" + entityID)
}

func encodeFloat64(v float64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, math.Float64bits(v))
	return buf
}

func decodeFloat64(b []byte) (float64, bool) {
	if len(b) != 8 {
		return 0, false
	}
	return math.Float64frombits(binary.BigEndian.Uint64(b)), true
}

func (l *Leaderboard) lockFor(metric, entityID string) *sync.Mutex {
	h := fnv.New32a()
	_, _ = h.Write([]byte(metric + ":" + entityID))
	return &l.mu[h.Sum32()%uint32(len(l.mu))]
}

func (l *Leaderboard) Increment(metric, entityID string, delta float64) error {
	mu := l.lockFor(metric, entityID)
	mu.Lock()
	defer mu.Unlock()
	current, err := l.Get(metric, entityID)
	if err != nil {
		return err
	}
	return l.setLocked(metric, entityID, current, current+delta)
}

func (l *Leaderboard) Set(metric, entityID string, value float64) error {
	mu := l.lockFor(metric, entityID)
	mu.Lock()
	defer mu.Unlock()
	current, err := l.Get(metric, entityID)
	if err != nil {
		return err
	}
	return l.setLocked(metric, entityID, current, value)
}

func (l *Leaderboard) setLocked(metric, entityID string, current, value float64) error {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return fmt.Errorf("leaderboard value must be finite")
	}
	if value == 0 {
		value = 0
	} // Canonical positive zero.
	batch := l.db.NewBatch()
	defer batch.Close()
	if err := batch.Delete(lbKey(metric, entityID, current), nil); err != nil {
		return err
	}
	val, err := json.Marshal(LeaderboardEntry{EntityID: entityID, Value: value})
	if err != nil {
		return err
	}
	if err = batch.Set(lbKey(metric, entityID, value), val, nil); err != nil {
		return err
	}
	if err = batch.Set(lbEntityKey(metric, entityID), encodeFloat64(value), nil); err != nil {
		return err
	}
	return batch.Commit(pebble.Sync)
}

func (l *Leaderboard) Get(metric, entityID string) (float64, error) {
	// O(1) lookup via entity index
	data, closer, err := l.db.Get(lbEntityKey(metric, entityID))
	if err == nil {
		defer closer.Close()
		if v, ok := decodeFloat64(data); ok {
			return v, nil
		}
	}
	if err != nil && !errors.Is(err, pebble.ErrNotFound) {
		return 0, err
	}

	// fallback: scan (für alte Daten ohne entity-index)
	prefix := []byte("lb:" + metric + ":")
	iter, err := l.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: []byte("lb:" + metric + ";"),
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
	mu := l.lockFor(metric, entityID)
	mu.Lock()
	defer mu.Unlock()
	current, err := l.Get(metric, entityID)
	if err != nil {
		return err
	}
	batch := l.db.NewBatch()
	defer batch.Close()
	batch.Delete(lbKey(metric, entityID, current), pebble.Sync)
	batch.Delete(lbEntityKey(metric, entityID), pebble.Sync)
	return batch.Commit(pebble.Sync)
}

func (l *Leaderboard) TopN(metric string, limit, offset int) ([]LeaderboardEntry, error) {
	if limit < 0 || offset < 0 {
		return nil, fmt.Errorf("invalid pagination")
	}
	prefix := []byte("lb:" + metric + ":")
	split := append(append([]byte(nil), prefix...), 0x80)
	results := []LeaderboardEntry{}
	// Preserve the on-disk format: nonnegative scores scan forward, negative
	// scores backward. This fixes signed ordering without a data migration.
	for pass := 0; pass < 2; pass++ {
		lower, upper := split, []byte("lb:"+metric+";")
		if pass == 1 {
			lower, upper = prefix, split
		}
		iter, err := l.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
		if err != nil {
			return nil, err
		}
		valid := iter.First()
		if pass == 1 {
			valid = iter.Last()
		}
		for valid {
			var entry LeaderboardEntry
			if err := json.Unmarshal(iter.Value(), &entry); err != nil {
				iter.Close()
				return nil, err
			}
			if offset > 0 {
				offset--
			} else {
				results = append(results, entry)
			}
			if limit > 0 && len(results) >= limit {
				break
			}
			if pass == 0 {
				valid = iter.Next()
			} else {
				valid = iter.Prev()
			}
		}
		err = iter.Error()
		iter.Close()
		if err != nil {
			return nil, err
		}
		if limit > 0 && len(results) >= limit {
			break
		}
	}
	return results, nil
}

// AtomicMutation holds the entity lock until events, scores and receipts commit.
func (l *Leaderboard) AtomicMutation(metric, entity string, fn func(*pebble.Batch) error) error {
	mu := l.lockFor(metric, entity)
	mu.Lock()
	defer mu.Unlock()
	batch := l.db.NewBatch()
	defer batch.Close()
	if err := fn(batch); err != nil {
		return err
	}
	return batch.Commit(pebble.Sync)
}

func (l *Leaderboard) StageValue(batch *pebble.Batch, metric, entity string, value float64, remove bool) error {
	current, err := l.Get(metric, entity)
	if err != nil {
		return err
	}
	if err := batch.Delete(lbKey(metric, entity, current), nil); err != nil {
		return err
	}
	if remove {
		return batch.Delete(lbEntityKey(metric, entity), nil)
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return fmt.Errorf("leaderboard value must be finite")
	}
	if value == 0 {
		value = 0
	}
	data, err := json.Marshal(LeaderboardEntry{EntityID: entity, Value: value})
	if err != nil {
		return err
	}
	if err := batch.Set(lbKey(metric, entity, value), data, nil); err != nil {
		return err
	}
	return batch.Set(lbEntityKey(metric, entity), encodeFloat64(value), nil)
}
