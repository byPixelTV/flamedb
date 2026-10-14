package analytics

import (
	"encoding/base64"
	"fmt"
	"strings"
	"unicode/utf8"
)

const stringPrefix = "\x1eFlame64:"

// EncodeString preserves arbitrary protocol byte strings in PostgreSQL text.
// The prefix is escaped too, so the mapping remains one-to-one.
func EncodeString(value string) string {
	if strings.IndexByte(value, 0) >= 0 || !utf8.ValidString(value) || strings.HasPrefix(value, stringPrefix) {
		return stringPrefix + base64.StdEncoding.EncodeToString([]byte(value))
	}
	return value
}
func DecodeString(value string) (string, error) {
	if !strings.HasPrefix(value, stringPrefix) {
		return value, nil
	}
	data, err := base64.StdEncoding.DecodeString(value[len(stringPrefix):])
	if err != nil {
		return "", fmt.Errorf("invalid encoded database string: %w", err)
	}
	return string(data), nil
}
func EncodeTags(tags map[string]string) map[string]string {
	out := make(map[string]string, len(tags))
	for k, v := range tags {
		out[EncodeString(k)] = EncodeString(v)
	}
	return out
}
func decodeTags(tags map[string]string) (map[string]string, error) {
	if len(tags) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(tags))
	for k, v := range tags {
		key, err := DecodeString(k)
		if err != nil {
			return nil, err
		}
		value, err := DecodeString(v)
		if err != nil {
			return nil, err
		}
		out[key] = value
	}
	return out, nil
}
