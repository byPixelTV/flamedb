package keyspace

import (
	"encoding/hex"
	"strings"
)

// Component escapes delimiter-bearing identifiers while preserving existing keys.
// The marker is forbidden in protocol identifiers, so legacy names cannot collide.
func Component(name string) string {
	if !strings.ContainsAny(name, ":\x1f") {
		return name
	}
	return "\x1f" + hex.EncodeToString([]byte(name))
}

// DecodeComponent restores the public identifier for metric discovery.
func DecodeComponent(name string) string {
	if strings.HasPrefix(name, "\x1f") {
		decoded, err := hex.DecodeString(name[1:])
		if err == nil {
			return string(decoded)
		}
	}
	return name
}
