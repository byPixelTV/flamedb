package cluster

import (
	"testing"

	"github.com/cockroachdb/pebble"
)

func TestReplicationOutboxPutBatchDelete(t *testing.T) {
	db, err := pebble.Open(t.TempDir(), &pebble.Options{})
	if err != nil {
		t.Fatalf("pebble.Open() error = %v", err)
	}
	defer db.Close()

	outbox := newReplicationOutbox(db)
	records := []replicationRecord{
		{node: Node{ID: "node-2", Addr: "127.0.0.1:7778"}, query: `WRITE kills 1 __replica`},
		{node: Node{ID: "node-3", Addr: "127.0.0.1:7779"}, query: `WRITE kills 2 __replica`},
	}

	keys, err := outbox.putBatch(records)
	if err != nil {
		t.Fatalf("putBatch() error = %v", err)
	}
	if len(keys) != len(records) {
		t.Fatalf("len(keys) = %d, want %d", len(keys), len(records))
	}
	if outbox.depth.Load() != int64(len(records)) {
		t.Fatalf("depth = %d, want %d", outbox.depth.Load(), len(records))
	}

	for _, key := range keys {
		if _, closer, err := db.Get(key); err != nil {
			t.Fatalf("db.Get(%q) error = %v", string(key), err)
		} else {
			closer.Close()
		}
	}

	outbox.deleteBatch(keys)
	if outbox.depth.Load() != 0 {
		t.Fatalf("depth after delete = %d, want 0", outbox.depth.Load())
	}
	for _, key := range keys {
		if _, closer, err := db.Get(key); err == nil {
			closer.Close()
			t.Fatalf("db.Get(%q) succeeded after delete", string(key))
		}
	}
}
