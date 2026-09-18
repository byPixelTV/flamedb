package storage

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	legacy "github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	legacyvfs "github.com/cockroachdb/pebble/vfs"
)

// prepareLegacy migrates only an identified, existing legacy database. Errors
// opening current/future/corrupt databases must never trigger a format rewrite.
func prepareLegacy(path string) error {
	// v2 Peek does not recognize every legacy CURRENT/manifest layout. Inspect
	// with v1 first; never interpret an unrecognized old DB as an empty directory.
	old, err := peekLegacy(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err == nil {
		if !old.Exists || pebble.FormatMajorVersion(old.FormatMajorVersion) >= pebble.FormatMinSupported {
			return nil
		}
		_, err = upgradeLegacy(path, func(db *legacy.DB, backup string) error {
			return db.Checkpoint(backup, legacy.WithFlushedWAL())
		})
		return err
	}
	current, currentErr := pebble.Peek(path, vfs.Default)
	if currentErr == nil && current.Exists && current.FormatMajorVersion >= pebble.FormatMinSupported {
		return nil
	}
	if currentErr != nil {
		return currentErr
	}
	return fmt.Errorf("cannot identify database %q: %w", path, err)
}

// upgradeLegacy holds Pebble's exclusive database lock through checkpoint and
// format ratcheting. Pebble persists each format transition and can resume after
// interruption. Every attempt receives a fresh backup directory; none is deleted.
func upgradeLegacy(path string, checkpoint func(*legacy.DB, string) error) (backup string, err error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	db, err := legacy.Open(absolute, &legacy.Options{ErrorIfNotExists: true})
	if err != nil {
		return "", fmt.Errorf("open legacy database for migration: %w", err)
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	// Another process may have completed migration between Peek and this lock.
	if pebble.FormatMajorVersion(db.FormatMajorVersion()) >= pebble.FormatMinSupported {
		return "", nil
	}
	container, err := os.MkdirTemp(filepath.Dir(absolute), filepath.Base(absolute)+".pre-v2-")
	if err != nil {
		return "", fmt.Errorf("create migration backup directory: %w", err)
	}
	backup = filepath.Join(container, "db")
	log.Printf("legacy database detected at %s; creating pre-migration checkpoint at %s", absolute, backup)
	if err := checkpoint(db, backup); err != nil {
		return backup, fmt.Errorf("migration stopped before format upgrade; checkpoint %s failed: %w", backup, err)
	}
	// Verify the checkpoint can be opened before modifying the source format.
	check, err := legacy.Open(backup, &legacy.Options{ReadOnly: true, ErrorIfNotExists: true})
	if err != nil {
		return backup, fmt.Errorf("migration checkpoint verification failed: %w", err)
	}
	if err := check.Close(); err != nil {
		return backup, err
	}
	if err := db.RatchetFormatMajorVersion(legacy.FormatNewest); err != nil {
		return backup, fmt.Errorf("automatic format migration failed (checkpoint retained at %s): %w", backup, err)
	}
	log.Printf("database format upgraded at %s; pre-migration checkpoint retained at %s", absolute, backup)
	return backup, nil
}

// Legacy Peek leaks marker directory handles on malformed metadata. Track only
// its short-lived directory handles and close any it leaves behind on all exits.
func peekLegacy(path string) (*legacy.DBDesc, error) {
	fs := &legacyPeekFS{FS: legacyvfs.Default}
	defer func() {
		for _, f := range fs.dirs {
			_ = f.Close()
		}
	}()
	return legacy.Peek(path, fs)
}

type legacyPeekFS struct {
	legacyvfs.FS
	dirs []*peekDirectory
}

func (fs *legacyPeekFS) OpenDir(name string) (legacyvfs.File, error) {
	f, err := fs.FS.OpenDir(name)
	if err != nil {
		return nil, err
	}
	tracked := &peekDirectory{File: f}
	fs.dirs = append(fs.dirs, tracked)
	return tracked, nil
}

type peekDirectory struct {
	legacyvfs.File
	closed bool
}

func (f *peekDirectory) Close() error {
	if f.closed {
		return nil
	}
	f.closed = true
	return f.File.Close()
}
