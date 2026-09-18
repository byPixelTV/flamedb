package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEnvironmentInternalKeyNeverSaved(t *testing.T) {
	t.Setenv("FLAMEDB_INTERNAL_KEY", "temporary-test-secret")
	path := filepath.Join(t.TempDir(), "config.yml")
	if err := os.WriteFile(path, []byte("auth:\n  keys: []\ncluster:\n  replication_factor: 2\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Auth.EffectiveInternalKey() != "temporary-test-secret" {
		t.Fatal("key not loaded")
	}
	if err := Save(path, cfg); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "temporary-test-secret") {
		t.Fatal("environment secret persisted")
	}
}
