package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/cockroachdb/pebble"
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

		// check ob dieser tagvalue bereits bekannt ist
		_, closer, err := s.db.Get(ck)
		if err == pebble.ErrNotFound {
			// neuer unique value — cardinality count erhöhen
			closer = nil
			countKey := cardCountKey(metric, tagKey)
			count := s.getCardCount(countKey)
			count++
			buf := make([]byte, 8)
			binary.BigEndian.PutUint64(buf, uint64(count))
			batch.Set(countKey, buf, nil)

			// tagvalue als bekannt markieren
			batch.Set(ck, []byte{1}, nil)
			if s.cardCache != nil {
				s.cardCache.add(ckey)
			}
		} else if err == nil {
			closer.Close()
			if s.cardCache != nil {
				s.cardCache.add(ckey)
			}
			// bereits bekannt, nichts tun
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

// GetCardinality gibt die anzahl unique values für einen tag zurück
func (s *Storage) GetCardinality(metric, tagKey string) uint64 {
	return s.getCardCount(cardCountKey(metric, tagKey))
}

// BestIndexTag gibt den tag mit der niedrigsten cardinality zurück
// niedrige cardinality = selektivster filter = bester primary index tag
func (s *Storage) BestIndexTag(metric string, tags map[string]string) (string, string) {
	var bestKey, bestVal string
	var bestCard uint64 = ^uint64(0) // max uint64

	for k, v := range tags {
		card := s.GetCardinality(metric, k)
		if card < bestCard {
			bestCard = card
			bestKey = k
			bestVal = v
		}
	}

	// fallback falls cardinality noch 0 ist (keine daten yet)
	if bestKey == "" {
		for k, v := range tags {
			bestKey = k
			bestVal = v
			break
		}
	}

	return bestKey, bestVal
}

// TagStats für debugging / introspection
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

// für json marshal in introspection queries
var _ = json.Marshal
