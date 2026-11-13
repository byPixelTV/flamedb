package config

import (
	"fmt"
	"os"
	"strings"

	"go.yaml.in/yaml/v3"
)

type APIKey struct {
	Name        string   `yaml:"name"`
	Key         string   `yaml:"key"`
	Permissions []string `yaml:"permissions"`
}

type AuthConfig struct {
	Enabled                *bool    `yaml:"enabled,omitempty"`
	EnvironmentInternalKey string   `yaml:"-"`
	InternalKey            string   `yaml:"internal_key,omitempty"`
	Keys                   []APIKey `yaml:"keys"`
}

type ServerConfig struct {
	MaxConnections  int    `yaml:"max_connections"`
	MaxRequestBytes int    `yaml:"max_request_bytes"`
	MaxBatchEntries int    `yaml:"max_batch_entries"`
	TLSCert         string `yaml:"tls_cert"`
	TLSKey          string `yaml:"tls_key"`
	Port            int    `yaml:"port"`
	Host            string `yaml:"host"`
	AdvertiseAddr   string `yaml:"advertise_addr"`
	NodeID          string `yaml:"node_id"`
	DataPath        string `yaml:"data_path"`
}

type StorageConfig struct {
	Engine      string `yaml:"engine,omitempty"` // optional, only pebble is supported
	Compression string `yaml:"compression"`      // none|snappy|zstd
}

type Config struct {
	Auth    AuthConfig    `yaml:"auth"`
	Server  ServerConfig  `yaml:"server"`
	Cluster ClusterConfig `yaml:"cluster"`
	Storage StorageConfig `yaml:"storage"`
}

type ClusterConfig struct {
	Enabled              bool     `yaml:"enabled"`
	Seeds                []string `yaml:"seeds"`
	ReplicationFactor    int      `yaml:"replication_factor"`
	ReadPolicy           string   `yaml:"read_policy,omitempty"`
	ReplicationQueueSize int      `yaml:"replication_queue_size,omitempty"`
	FanoutQueueSize      int      `yaml:"fanout_queue_size,omitempty"`
}

type NodeConfig struct {
	ID   string `yaml:"id"`
	Addr string `yaml:"addr"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	if cfg.Storage.Engine != "" && cfg.Storage.Engine != "pebble" {
		return nil, fmt.Errorf("unsupported storage engine: %q (only pebble is supported)", cfg.Storage.Engine)
	}
	// Refuse an old environment-only selection rather than silently starting an
	// unrelated, potentially empty Pebble database.
	if cfg.Storage.Engine == "" && os.Getenv("FLAMEDB_POSTGRES_DSN") != "" {
		return nil, fmt.Errorf("FLAMEDB_POSTGRES_DSN no longer selects a backend; configure an existing Pebble database explicitly")
	}
	// Expand auth secrets after decoding YAML so values cannot inject structure.
	for i := range cfg.Auth.Keys {
		cfg.Auth.Keys[i].Key = os.ExpandEnv(cfg.Auth.Keys[i].Key)
	}
	cfg.Auth.InternalKey = os.ExpandEnv(cfg.Auth.InternalKey)
	if cfg.Server.Port == 0 {
		cfg.Server.Port = 7777
	}
	if cfg.Server.Host == "" {
		cfg.Server.Host = "127.0.0.1"
	}
	if cfg.Server.MaxConnections == 0 {
		cfg.Server.MaxConnections = 1024
	}
	if cfg.Server.MaxRequestBytes == 0 {
		cfg.Server.MaxRequestBytes = 8 << 20
	}
	if cfg.Server.MaxBatchEntries == 0 {
		cfg.Server.MaxBatchEntries = 10000
	}
	if cfg.Server.MaxConnections < 1 || cfg.Server.MaxRequestBytes < 1024 || cfg.Server.MaxBatchEntries < 1 {
		return nil, fmt.Errorf("invalid resource limits")
	}
	if cfg.Server.Port < 1 || cfg.Server.Port > 65535 {
		return nil, fmt.Errorf("invalid server port")
	}
	if (cfg.Server.TLSCert == "") != (cfg.Server.TLSKey == "") {
		return nil, fmt.Errorf("TLS requires both tls_cert and tls_key")
	}
	if key := os.Getenv("FLAMEDB_INTERNAL_KEY"); key != "" {
		cfg.Auth.EnvironmentInternalKey = key
	}
	if cfg.Auth.IsEnabled() && len(cfg.Auth.Keys) == 0 && cfg.Auth.EffectiveInternalKey() == "" {
		return nil, fmt.Errorf("at least one auth key is required")
	}
	seen := map[string]bool{}
	for _, k := range cfg.Auth.Keys {
		if strings.TrimSpace(k.Key) == "" || strings.ContainsAny(k.Key, "\r\n") || seen[k.Key] || k.Key == cfg.Auth.EffectiveInternalKey() {
			return nil, fmt.Errorf("empty, duplicate or conflicting auth key")
		}
		seen[k.Key] = true
	}
	if strings.ContainsAny(cfg.Auth.EffectiveInternalKey(), "\r\n") {
		return nil, fmt.Errorf("invalid internal key")
	}
	if (len(cfg.Cluster.Seeds) > 0 || cfg.Cluster.ReplicationFactor > 1) && cfg.Auth.EffectiveInternalKey() == "" {
		return nil, fmt.Errorf("cluster mode requires auth.internal_key shared by all nodes")
	}
	return &cfg, nil
}

func (a AuthConfig) IsEnabled() bool { return a.Enabled == nil || *a.Enabled }

func Save(path string, cfg *Config) error {
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (a AuthConfig) EffectiveInternalKey() string {
	if a.EnvironmentInternalKey != "" {
		return a.EnvironmentInternalKey
	}
	return a.InternalKey
}
