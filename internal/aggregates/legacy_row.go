package aggregates

import (
	"bytes"
	"unicode/utf8"
)

// plainLegacyEntity recognizes the ordinary encoding emitted by leaderboard
// writers. It is only a rejection filter: matching rows are still decoded by
// encoding/json, as are escaped IDs, alternate field order and older encodings.
func plainLegacyEntity(data []byte) ([]byte, bool) {
	const prefix = `{"entity_id":"`
	const scorePrefix = `","value":`
	if !bytes.HasPrefix(data, []byte(prefix)) || data[len(data)-1] != '}' {
		return nil, false
	}
	rest := data[len(prefix):]
	end := bytes.IndexByte(rest, '"')
	if end < 0 || !bytes.HasPrefix(rest[end:], []byte(scorePrefix)) {
		return nil, false
	}
	entity := rest[:end]
	if bytes.IndexByte(entity, '\\') >= 0 || !utf8.Valid(entity) {
		return nil, false
	}
	// No additional property can appear without a quote. If the value is
	// malformed the original decoder would reject the row anyway.
	if bytes.IndexByte(rest[end+len(scorePrefix):], '"') >= 0 {
		return nil, false
	}
	return entity, true
}
