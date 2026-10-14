package storage

import (
	"fmt"
	legacy "github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/v2"
)

// SnapshotIterator exposes a stopped database read-only to the migration tool.
// Values borrow iterator memory until it advances. No format upgrade occurs.
type SnapshotIterator interface {
	First() bool
	Next() bool
	Valid() bool
	Key() []byte
	Value() []byte
	Error() error
	Close() error
}

func OpenReadOnlySnapshot(path string) (SnapshotIterator, func() error, error) {
	db, err := pebble.Open(path, &pebble.Options{ReadOnly: true, ErrorIfNotExists: true})
	if err == nil {
		iter, err := db.NewIter(nil)
		if err != nil {
			db.Close()
			return nil, nil, err
		}
		return iter, db.Close, nil
	}
	old, oldErr := legacy.Open(path, &legacy.Options{ReadOnly: true, ErrorIfNotExists: true})
	if oldErr != nil {
		return nil, nil, fmt.Errorf("read-only snapshot: v2: %v; v1: %w", err, oldErr)
	}
	iter, err := old.NewIter(nil)
	if err != nil {
		old.Close()
		return nil, nil, err
	}
	return iter, old.Close, nil
}

func DecodeSnapshotEvent(value []byte, metric string) (Event, error) {
	return decodeEventValue(value, metric)
}
