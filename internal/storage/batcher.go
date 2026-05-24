package storage

import (
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
)

type writeEntry struct {
	key   []byte
	value []byte
}

type WriteBatcher struct {
	db      *pebble.DB
	mu      sync.Mutex
	pending []writeEntry
	flushCh chan struct{}
	doneCh  chan error
}

func NewWriteBatcher(db *pebble.DB) *WriteBatcher {
	b := &WriteBatcher{
		db:      db,
		flushCh: make(chan struct{}, 1),
		doneCh:  make(chan error, 1),
	}
	go b.run()
	return b
}

func (b *WriteBatcher) Add(key, value []byte) {
	b.mu.Lock()
	b.pending = append(b.pending, writeEntry{key: key, value: value})
	b.mu.Unlock()
}

func (b *WriteBatcher) Flush() error {
	b.flushCh <- struct{}{}
	return <-b.doneCh
}

func (b *WriteBatcher) run() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.flush()
		case <-b.flushCh:
			err := b.flush()
			b.doneCh <- err
		}
	}
}

func (b *WriteBatcher) flush() error {
	b.mu.Lock()
	if len(b.pending) == 0 {
		b.mu.Unlock()
		return nil
	}
	entries := b.pending
	b.pending = nil
	b.mu.Unlock()

	batch := b.db.NewBatch()
	for _, e := range entries {
		batch.Set(e.key, e.value, nil)
	}
	return batch.Commit(pebble.Sync)
}
