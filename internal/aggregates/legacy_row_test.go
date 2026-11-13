package aggregates

import (
	"encoding/json"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

func TestLegacyLookupFilterPreservesDecodedIdentity(t *testing.T) {
	db, err := pebble.Open("db", &pebble.Options{FS: vfs.NewMem()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	lb := New(db)
	// Old/imported rows can use noncanonical JSON, or a key suffix different
	// from the JSON identity. Lookup continues to follow the decoded identity.
	rows := []struct {
		key, payload, entity string
		want                 float64
	}{
		{"not-the-identity", `{"entity_id":"plain","value":8}`, "plain", 8},
		{"reordered", `{"value":-2,"entity_id":"alternate"}`, "alternate", -2},
		{"escaped", `{"entity_id":"a\u0000\"b","value":4}`, "a\x00\"b", 4},
		{"duplicate", `{"entity_id":"ignore","value":3,"ENTITY_ID":"actual"}`, "actual", 3},
		{"unicode", `{"entity_id":"玩家","value":6}`, "玩家", 6},
		{"invalid-value", `{"entity_id":"invalid","value":nope}`, "invalid", 0},
		{"invalid-utf8", "{\"entity_id\":\"\xff\",\"value\":7}", "\ufffd", 7},
	}
	targets := map[string]map[string]struct{}{"m": {"missing": {}}}
	for _, row := range rows {
		if err := db.Set(lbKey("m", row.key, row.want), []byte(row.payload), pebble.Sync); err != nil {
			t.Fatal(err)
		}
		targets["m"][row.entity] = struct{}{}
	}
	reader := lb.NewLockedBatchLookup(targets, nil)
	for _, row := range rows {
		if score, err := lb.Get("m", row.entity); err != nil || score != row.want {
			t.Fatalf("single %q: %v %v", row.entity, score, err)
		}
		if score, err := reader.Get("m", row.entity); err != nil || score != row.want {
			t.Fatalf("batch %q: %v %v", row.entity, score, err)
		}
	}
}

func FuzzPlainLegacyEntity(f *testing.F) {
	for _, s := range []string{`{"entity_id":"known","value":9}`, `{"entity_id":"ignored","value":1,"ENTITY_ID":"actual"}`, `{"entity_id":"\u003a","value":9}`, `{"entity_id":"x","value":1}`} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		entity, ok := plainLegacyEntity(data)
		if !ok {
			return
		}
		var decoded LeaderboardEntry
		if json.Unmarshal(data, &decoded) == nil && decoded.EntityID != string(entity) {
			t.Fatalf("filter identity %q differs from decoded identity %q", entity, decoded.EntityID)
		}
	})
}
