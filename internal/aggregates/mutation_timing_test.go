package aggregates

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

type heldSyncFS struct {
	vfs.FS
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (fs *heldSyncFS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	f, err := fs.FS.Create(name, category)
	if err == nil && strings.HasSuffix(name, ".log") {
		return &heldSyncFile{File: f, fs: fs}, nil
	}
	return f, err
}

type heldSyncFile struct {
	vfs.File
	fs *heldSyncFS
}

func (f *heldSyncFile) hold() {
	if f.fs.armed.Load() {
		select {
		case f.fs.entered <- struct{}{}:
		default:
		}
		<-f.fs.release
	}
}

func (f *heldSyncFile) Sync() error     { f.hold(); return f.File.Sync() }
func (f *heldSyncFile) SyncData() error { f.hold(); return f.File.SyncData() }

func TestMutationTimingWaitsForDurableCommit(t *testing.T) {
	fs := &heldSyncFS{FS: vfs.NewMem(), entered: make(chan struct{}, 1), release: make(chan struct{})}
	db, err := pebble.Open("db", &pebble.Options{FS: fs})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	defer fs.once.Do(func() { close(fs.release) })
	lb := New(db)
	fs.armed.Store(true)
	var timing MutationTiming
	done := make(chan error, 1)
	go func() {
		done <- lb.AtomicMutationManyTimed([]string{"m:p"}, func(batch *pebble.Batch) error {
			// Make staging measurable even on Windows clocks with coarse ticks.
			time.Sleep(15 * time.Millisecond)
			return lb.StageValueFromCurrent(batch, "m", "p", 0, 42, false)
		}, &timing)
	}()
	select {
	case <-fs.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("commit never reached WAL sync")
	}
	select {
	case err := <-done:
		t.Fatalf("acknowledged before durable sync: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	fs.once.Do(func() { close(fs.release) })
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("commit did not resume after sync")
	}
	if timing.Commit < 30*time.Millisecond {
		t.Fatalf("held WAL sync was not attributed to commit: %+v", timing)
	}
	if timing.Stage < 15*time.Millisecond || timing.LockWait < 0 {
		t.Fatalf("missing phase measurements: %+v", timing)
	}
	if score, err := lb.Get("m", "p"); err != nil || score != 42 {
		t.Fatal(score, err)
	}
	fs.armed.Store(false)
	wantErr := errors.New("staging failed")
	err = lb.AtomicMutationManyTimed([]string{"m:p"}, func(batch *pebble.Batch) error {
		if err := batch.Set([]byte("not-committed"), []byte("value"), nil); err != nil {
			return err
		}
		return wantErr
	}, &timing)
	if !errors.Is(err, wantErr) || timing.Commit != 0 {
		t.Fatalf("failed callback committed or leaked old timing: %v %+v", err, timing)
	}
	if _, closer, err := db.Get([]byte("not-committed")); !errors.Is(err, pebble.ErrNotFound) {
		if closer != nil {
			closer.Close()
		}
		t.Fatalf("failed callback changed storage: %v", err)
	}
}

func TestLookupStatsDistinguishesIndexedAndLegacyScores(t *testing.T) {
	db, err := pebble.Open("db", &pebble.Options{FS: vfs.NewMem()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	lb := New(db)
	if err := lb.Set("m", "indexed", 12); err != nil {
		t.Fatal(err)
	}
	value, err := json.Marshal(LeaderboardEntry{EntityID: "legacy", Value: 7})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Set(lbKey("m", "legacy", 7), value, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	var stats LookupStats
	if score, err := lb.GetWithLookupStats("m", "indexed", &stats); err != nil || score != 12 {
		t.Fatal(score, err)
	}
	if stats != (LookupStats{}) {
		t.Fatalf("indexed point read reported legacy scan: %+v", stats)
	}
	if score, err := lb.GetWithLookupStats("m", "legacy", &stats); err != nil || score != 7 {
		t.Fatal(score, err)
	}
	if stats.LegacyScans != 1 || stats.LegacyEntries < 1 {
		t.Fatalf("legacy scan missing: %+v", stats)
	}
	if score, err := lb.GetWithLookupStats("m", "missing", &stats); err != nil || score != 0 {
		t.Fatal(score, err)
	}
	if stats.LegacyScans != 2 || stats.LegacyEntries < 3 {
		t.Fatalf("missing entity full scan missing: %+v", stats)
	}
}
