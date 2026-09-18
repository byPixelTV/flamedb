package main

import (
	"github.com/byPixelTV/flamedb/internal/storage"
	legacy "github.com/cockroachdb/pebble"
	"path/filepath"
	"testing"
)

func TestMigrationPreservesSourceAndData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	db, err := legacy.Open(path, &legacy.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Set([]byte("test"), []byte("value"), legacy.Sync); err != nil {
		t.Fatal(err)
	}
	db.Close()
	target := filepath.Join(t.TempDir(), "copy")
	if err = migrate(path, target); err != nil {
		t.Fatal(err)
	}
	s, err := storage.Open(target, "none")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	val, closer, err := s.DB().Get([]byte("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer closer.Close()
	if string(val) != "value" {
		t.Fatal(string(val))
	}
	old, err := legacy.Open(path, &legacy.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	old.Close()
	if err = migrate(path, target); err == nil {
		t.Fatal("overwrote destination")
	}
}
