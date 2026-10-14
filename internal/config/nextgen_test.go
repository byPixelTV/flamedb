package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestExternalConfigurationAndSafeEnvironmentExpansion(t *testing.T) {
	t.Setenv("TEST_PG_DSN", "postgres://user:pass@127.0.0.1/db?sslmode=disable")
	t.Setenv("TEST_REDIS_SECRET", "quoted:\n  server: injected")
	path := filepath.Join(t.TempDir(), "config.yml")
	content := "auth:\n  enabled: false\nstorage:\n  engine: timescaledb\n  postgres:\n    dsn: '${TEST_PG_DSN}'\n  redis:\n    address: localhost:6379\n    password: '${TEST_REDIS_SECRET}'\ncluster:\n  enabled: true\n  replication_factor: 3\n"
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Storage.Postgres.DSN != os.Getenv("TEST_PG_DSN") || cfg.Storage.Redis.Password != os.Getenv("TEST_REDIS_SECRET") || cfg.Server.Port != 7777 || cfg.Server.MaxConnections != 1024 || cfg.Auth.IsEnabled() {
		t.Fatal(cfg)
	}
	for _, extra := range []string{"performance:\n  max_inflight_requests: -1\n", "server:\n  max_response_bytes: -1\n", "server:\n  tls_cert: missing\n"} {
		if err := os.WriteFile(path, []byte(content+extra), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(path); err == nil {
			t.Fatalf("invalid config accepted %s", extra)
		}
	}
}
