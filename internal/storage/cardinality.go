package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
)

// key format: card:metric:tagkey:tagvalue → count
func cardKey(metric, tagKey, tagValue string) []byte {
	return []byte(fmt.Sprintf("card:%s:%s:%s", metric, tagKey, tagValue))
}

// key format: card-count:metric:tagkey → unique value count
func cardCountKey(metric, tagKey string) []byte {
	return []byte(fmt.Sprintf("card-count:%s:%s", metric, tagKey))
}

func cardCacheKey(metric, tagKey, tagValue string) string {
	return metric + "\x00" + tagKey + "\x00" + tagValue
}

func (s *Storage) updateCardinality(batch *pebble.Batch, metric string, tags map[string]string) error {
	for tagKey, tagValue := range tags {
		ckey := cardCacheKey(metric, tagKey, tagValue)
		if s.cardCache != nil && s.cardCache.seen(ckey) {
			continue
		}

		ck := cardKey(metric, tagKey, tagValue)

		// Check whether this tag value is already known.
		_, closer, err := s.db.Get(ck)
		if err == pebble.ErrNotFound {
			// New unique value: increment the cardinality count.
			closer = nil
			countKey := cardCountKey(metric, tagKey)
			count := s.getCardCount(countKey)
			count++
			buf := make([]byte, 8)
			binary.BigEndian.PutUint64(buf, uint64(count))
			batch.Set(countKey, buf, nil)

			// Mark the tag value as known.
			batch.Set(ck, []byte{1}, nil)
			if s.cardCache != nil {
				s.cardCache.add(ckey)
			}
		} else if err == nil {
			closer.Close()
			if s.cardCache != nil {
				s.cardCache.add(ckey)
			}
			// Already known; nothing to do.
		} else {
			return err
		}
	}
	return nil
}

func (s *Storage) getCardCount(countKey []byte) uint64 {
	data, closer, err := s.db.Get(countKey)
	if err != nil {
		return 0
	}
	defer closer.Close()
	if len(data) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(data)
}

// GetCardinality returns the number of unique values for a tag.
func (s *Storage) GetCardinality(metric, tagKey string) uint64 {
	return s.getCardCount(cardCountKey(metric, tagKey))
}

// BestIndexTag prefers higher distinct-value counts as a selectivity heuristic.
// Actual value frequencies may be skewed; this is not an exact cost estimate.
func (s *Storage) BestIndexTag(metric string, tags map[string]string) (string, string) {
	var bestKey, bestVal string
	var bestCard uint64

	for k, v := range tags {
		card := s.GetCardinality(metric, k)
		if bestKey == "" || card > bestCard || (card == bestCard && k < bestKey) {
			bestCard = card
			bestKey = k
			bestVal = v
		}
	}

	// Fallback when cardinality is still zero (no data yet).
	if bestKey == "" {
		for k, v := range tags {
			bestKey = k
			bestVal = v
			break
		}
	}

	return bestKey, bestVal
}

// TagStats provides debugging and introspection data.
type TagStats struct {
	TagKey      string `json:"tag_key"`
	Cardinality uint64 `json:"cardinality"`
}

func (s *Storage) GetTagStats(metric string, tagKeys []string) []TagStats {
	stats := make([]TagStats, 0, len(tagKeys))
	for _, k := range tagKeys {
		stats = append(stats, TagStats{
			TagKey:      k,
			Cardinality: s.GetCardinality(metric, k),
		})
	}
	return stats
}

// For JSON marshaling in introspection queries.
var _ = json.Marshal
