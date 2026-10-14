package config

import (
	"fmt"
	"os"
	"strings"
	"time"

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
	MaxConnections   int           `yaml:"max_connections"`
	MaxRequestBytes  int           `yaml:"max_request_bytes"`
	MaxResponseBytes int           `yaml:"max_response_bytes"`
	MaxBatchEntries  int           `yaml:"max_batch_entries"`
	RequestTimeout   time.Duration `yaml:"request_timeout"`
	ShutdownTimeout  time.Duration `yaml:"shutdown_timeout"`
	MetricsAddress   string        `yaml:"metrics_address"`
	TLSCert          string        `yaml:"tls_cert"`
	TLSKey           string        `yaml:"tls_key"`
	Port             int           `yaml:"port"`
	Host             string        `yaml:"host"`
	AdvertiseAddr    string        `yaml:"advertise_addr"`
	NodeID           string        `yaml:"node_id"`
	DataPath         string        `yaml:"data_path"`
}

type StorageConfig struct {
	Engine         string         `yaml:"engine"`
	Postgres       PostgresConfig `yaml:"postgres"`
	Redis          RedisConfig    `yaml:"redis"`
	MaxQueryEvents int            `yaml:"max_query_events"`
	Compression    string         `yaml:"compression"` // none|snappy|zstd
}

type PostgresConfig struct {
	QuorumReplicationFactor int    `yaml:"quorum_replication_factor"`
	DSN                     string `yaml:"dsn"`
	MaxConnections          int32  `yaml:"max_connections"`
}
type RedisConfig struct {
	SentinelUsername string   `yaml:"sentinel_username"`
	SentinelPassword string   `yaml:"sentinel_password"`
	Mode             string   `yaml:"mode"`
	Address          string   `yaml:"address"`
	Addresses        []string `yaml:"addresses"`
	MasterName       string   `yaml:"master_name"`
	Username         string   `yaml:"username"`
	Password         string   `yaml:"password"`
	DB               int      `yaml:"db"`
	PoolSize         int      `yaml:"pool_size"`
	TLS              bool     `yaml:"tls"`
}
type PerformanceConfig struct {
	ProfileAddress      string `yaml:"profile_address"`
	BatchSize           int    `yaml:"batch_size"`
	MaxInflightRequests int    `yaml:"max_inflight_requests"`
}

type Config struct {
	Performance PerformanceConfig `yaml:"performance"`
	Auth        AuthConfig        `yaml:"auth"`
	Server      ServerConfig      `yaml:"server"`
	Cluster     ClusterConfig     `yaml:"cluster"`
	Storage     StorageConfig     `yaml:"storage"`
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
	// Existing YAML can opt into the successor without changing its shape.
	if cfg.Storage.Engine == "" {
		if dsn := os.Getenv("FLAMEDB_POSTGRES_DSN"); dsn != "" {
			cfg.Storage.Engine = "postgres"
			cfg.Storage.Postgres.DSN = dsn
			if cfg.Storage.Redis.Address == "" {
				cfg.Storage.Redis.Address = os.Getenv("FLAMEDB_REDIS_ADDRESS")
				if cfg.Storage.Redis.Address == "" {
					cfg.Storage.Redis.Address = "127.0.0.1:6379"
				}
			}
		}
	}
	// Expand only connection secrets, after YAML decoding, so environment values
	// cannot inject configuration structure.
	cfg.Storage.Postgres.DSN = os.ExpandEnv(cfg.Storage.Postgres.DSN)
	cfg.Storage.Redis.Address = os.ExpandEnv(cfg.Storage.Redis.Address)
	cfg.Storage.Redis.Password = os.ExpandEnv(cfg.Storage.Redis.Password)
	cfg.Storage.Redis.SentinelUsername = os.ExpandEnv(cfg.Storage.Redis.SentinelUsername)
	cfg.Storage.Redis.SentinelPassword = os.ExpandEnv(cfg.Storage.Redis.SentinelPassword)
	for i := range cfg.Storage.Redis.Addresses {
		cfg.Storage.Redis.Addresses[i] = os.ExpandEnv(cfg.Storage.Redis.Addresses[i])
	}
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
	if cfg.Server.MaxResponseBytes == 0 {
		cfg.Server.MaxResponseBytes = 64 << 20
	}
	if cfg.Server.MaxBatchEntries == 0 {
		cfg.Server.MaxBatchEntries = 10000
	}
	if cfg.Server.RequestTimeout == 0 {
		cfg.Server.RequestTimeout = 30 * time.Second
	}
	if cfg.Server.ShutdownTimeout == 0 {
		cfg.Server.ShutdownTimeout = 30 * time.Second
	}
	if cfg.Performance.MaxInflightRequests == 0 {
		cfg.Performance.MaxInflightRequests = 1024
	}
	if cfg.Performance.BatchSize == 0 {
		cfg.Performance.BatchSize = 1000
	}
	if cfg.Storage.Postgres.MaxConnections == 0 {
		cfg.Storage.Postgres.MaxConnections = 64
	}
	if cfg.Storage.MaxQueryEvents == 0 {
		cfg.Storage.MaxQueryEvents = 1000000
	}
	if cfg.Server.MaxConnections < 1 || cfg.Server.MaxRequestBytes < 1024 || cfg.Server.MaxResponseBytes < 1024 || cfg.Server.MaxBatchEntries < 1 || cfg.Server.RequestTimeout < time.Millisecond || cfg.Server.ShutdownTimeout < time.Millisecond || cfg.Performance.MaxInflightRequests < 1 || cfg.Performance.BatchSize < 1 || cfg.Storage.Postgres.MaxConnections < 1 || cfg.Storage.MaxQueryEvents < 1 {
		return nil, fmt.Errorf("invalid resource limits")
	}
	if cfg.Server.Port < 1 || cfg.Server.Port > 65535 || cfg.Storage.Postgres.QuorumReplicationFactor < 0 || cfg.Storage.Redis.DB < 0 || cfg.Storage.Redis.PoolSize < 0 {
		return nil, fmt.Errorf("invalid server port or database configuration")
	}
	if cfg.Storage.Redis.Mode == "cluster" && cfg.Storage.Redis.DB != 0 {
		return nil, fmt.Errorf("Redis Cluster requires db: 0")
	}
	if cfg.Storage.Postgres.QuorumReplicationFactor > 0 && cfg.Cluster.ReplicationFactor > 0 && cfg.Storage.Postgres.QuorumReplicationFactor/2 < cfg.Cluster.ReplicationFactor/2 {
		return nil, fmt.Errorf("PostgreSQL quorum cannot weaken the configured legacy cluster majority")
	}
	if (cfg.Server.TLSCert == "") != (cfg.Server.TLSKey == "") {
		return nil, fmt.Errorf("TLS requires both tls_cert and tls_key")
	}
	switch cfg.Storage.Engine {
	case "", "pebble":
	case "postgres", "postgresql", "timescaledb":
		if cfg.Storage.Postgres.DSN == "" {
			return nil, fmt.Errorf("postgres dsn is required")
		}
		if cfg.Storage.Redis.Mode == "" {
			cfg.Storage.Redis.Mode = "standalone"
		}
		switch cfg.Storage.Redis.Mode {
		case "standalone", "sentinel", "cluster":
		default:
			return nil, fmt.Errorf("unsupported redis mode")
		}
		if cfg.Storage.Redis.Mode == "sentinel" && cfg.Storage.Redis.MasterName == "" {
			return nil, fmt.Errorf("redis sentinel requires master_name")
		}
		if cfg.Storage.Redis.Address == "" && len(cfg.Storage.Redis.Addresses) == 0 {
			return nil, fmt.Errorf("redis address is required")
		}
	default:
		return nil, fmt.Errorf("unsupported storage engine: %s", cfg.Storage.Engine)
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
	if (cfg.Storage.Engine == "" || cfg.Storage.Engine == "pebble") && (len(cfg.Cluster.Seeds) > 0 || cfg.Cluster.ReplicationFactor > 1) && cfg.Auth.EffectiveInternalKey() == "" {
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
