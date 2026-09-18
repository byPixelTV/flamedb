package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	legacy "github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/v2"
	legacyvfs "github.com/cockroachdb/pebble/vfs"
)

func createLegacy(t *testing.T, path string) {
	t.Helper()
	db, err := legacy.Open(path, &legacy.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"event", "idx:metric:tag:value", "lb-entity:metric:player", "repl-outbox:node:123"} {
		if err := db.Set([]byte(key), []byte("original"), legacy.Sync); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	} // Also exercise old SSTables, not just WAL.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAutomaticMigrationAndPreservedBackup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	createLegacy(t, path)
	store, err := Open(path, "none")
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"event", "idx:metric:tag:value", "lb-entity:metric:player", "repl-outbox:node:123"} {
		data, closer, err := store.DB().Get([]byte(key))
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != "original" {
			t.Errorf("lost %s", key)
		}
		closer.Close()
	}
	if err := store.DB().Set([]byte("event"), []byte("new"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	backups, err := filepath.Glob(path + ".pre-v2-*/db")
	if err != nil || len(backups) != 1 {
		t.Fatalf("backups: %v %v", backups, err)
	}
	backup, err := legacy.Open(backups[0], &legacy.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	data, closer, err := backup.Get([]byte("event"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "original" {
		t.Fatal("backup was modified")
	}
	closer.Close()
	backup.Close()
	store, err = Open(path, "none")
	if err != nil {
		t.Fatal(err)
	}
	store.Close()
	again, _ := filepath.Glob(path + ".pre-v2-*/db")
	if len(again) != 1 {
		t.Fatal("migration repeated on restart")
	}
}

func TestBackupFailureDoesNotUpgradeFormat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	createLegacy(t, path)
	before, err := legacy.Peek(path, legacyvfs.Default)
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("simulated disk full")
	_, err = upgradeLegacy(path, func(*legacy.DB, string) error { return failure })
	if !errors.Is(err, failure) {
		t.Fatal(err)
	}
	after, err := legacy.Peek(path, legacyvfs.Default)
	if err != nil {
		t.Fatal(err)
	}
	if before.FormatMajorVersion != after.FormatMajorVersion {
		t.Fatal("format changed without backup")
	}
	store, err := Open(path, "none")
	if err != nil {
		t.Fatal(err)
	}
	store.Close()
}

func TestLockedLegacyDatabaseIsNotMigrated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	createLegacy(t, path)
	db, err := legacy.Open(path, &legacy.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if store, err := Open(path, "none"); err == nil {
		store.Close()
		t.Fatal("opened locked database")
	}
	backups, _ := filepath.Glob(path + ".pre-v2-*")
	if len(backups) != 0 {
		t.Fatal("backed up locked database")
	}
}

func TestFreshDatabaseDoesNotCreateMigrationBackup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	store, err := Open(path, "none")
	if err != nil {
		t.Fatal(err)
	}
	store.Close()
	backups, _ := filepath.Glob(path + ".pre-v2-*")
	if len(backups) != 0 {
		t.Fatal(backups)
	}
}

func TestCurrentFormatIsNotMigrated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := pebble.Open(path, &pebble.Options{FormatMajorVersion: pebble.FormatNewest})
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	store, err := Open(path, "none")
	if err != nil {
		t.Fatal(err)
	}
	store.Close()
	backups, _ := filepath.Glob(path + ".pre-v2-*")
	if len(backups) != 0 {
		t.Fatal(backups)
	}
}

func TestCorruptLegacyManifestDoesNotTriggerUpgrade(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	createLegacy(t, path)
	current := filepath.Join(path, "CURRENT")
	if err := os.WriteFile(current, []byte("invalid-manifest\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if store, err := Open(path, "none"); err == nil {
		store.Close()
		t.Fatal("opened corrupt manifest")
	}
	backups, _ := filepath.Glob(path + ".pre-v2-*")
	if len(backups) != 0 {
		t.Fatal("migration attempted for corrupt database")
	}
	data, err := os.ReadFile(current)
	if err != nil || string(data) != "invalid-manifest\n" {
		t.Fatalf("manifest changed: %q %v", data, err)
	}
}

func TestIncompleteCheckpointDoesNotUpgrade(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	createLegacy(t, path)
	before, err := legacy.Peek(path, legacyvfs.Default)
	if err != nil {
		t.Fatal(err)
	}
	_, err = upgradeLegacy(path, func(_ *legacy.DB, backup string) error { return os.Mkdir(backup, 0700) })
	if err == nil {
		t.Fatal("accepted an incomplete checkpoint")
	}
	after, err := legacy.Peek(path, legacyvfs.Default)
	if err != nil {
		t.Fatal(err)
	}
	if before.FormatMajorVersion != after.FormatMajorVersion {
		t.Fatal("format changed without verified checkpoint")
	}
}
