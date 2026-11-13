package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPebbleOnlyConfiguration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yml")
	t.Setenv("FLAMEDB_POSTGRES_DSN", "")
	t.Setenv("PEBBLE_TEST_KEY", "quoted: value")
	load := func(storage string) (*Config, error) {
		t.Helper()
		if err := os.WriteFile(path, []byte("auth:\n  keys:\n    - name: test\n      key: '${PEBBLE_TEST_KEY}'\n      permissions: [read, write]\nserver:\n  data_path: existing-data\nstorage:\n  compression: zstd\n"+storage), 0600); err != nil {
			t.Fatal(err)
		}
		return Load(path)
	}
	for _, engine := range []string{"postgres", "postgresql", "timescaledb", "unknown"} {
		if _, err := load("  engine: " + engine + "\n"); err == nil || !strings.Contains(err.Error(), "only pebble") {
			t.Fatalf("invalid engine %q: %v", engine, err)
		}
	}
	for _, storage := range []string{"", "  engine: pebble\n"} {
		cfg, err := load(storage)
		if err != nil || cfg.Server.DataPath != "existing-data" || cfg.Storage.Compression != "zstd" || cfg.Auth.Keys[0].Key != "quoted: value" || cfg.Server.MaxConnections != 1024 {
			t.Fatalf("existing Pebble configuration: %v", err)
		}
	}
	t.Setenv("FLAMEDB_POSTGRES_DSN", "retired-selector")
	if _, err := load(""); err == nil || !strings.Contains(err.Error(), "no longer selects a backend") {
		t.Fatalf("retired environment selector silently fell back: %v", err)
	}
	if _, err := load("  engine: pebble\n"); err != nil {
		t.Fatalf("explicit Pebble with unrelated environment: %v", err)
	}
}
