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
	EnvironmentInternalKey string   `yaml:"-"`
	InternalKey            string   `yaml:"internal_key,omitempty"`
	Keys                   []APIKey `yaml:"keys"`
}

type ServerConfig struct {
	Port          int    `yaml:"port"`
	Host          string `yaml:"host"`
	AdvertiseAddr string `yaml:"advertise_addr"`
	NodeID        string `yaml:"node_id"`
	DataPath      string `yaml:"data_path"`
}

type StorageConfig struct {
	Compression string `yaml:"compression"` // none|snappy|zstd
}

type Config struct {
	Auth    AuthConfig    `yaml:"auth"`
	Server  ServerConfig  `yaml:"server"`
	Cluster ClusterConfig `yaml:"cluster"`
	Storage StorageConfig `yaml:"storage"`
}

type ClusterConfig struct {
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
	if key := os.Getenv("FLAMEDB_INTERNAL_KEY"); key != "" {
		cfg.Auth.EnvironmentInternalKey = key
	}
	if len(cfg.Auth.Keys) == 0 && cfg.Auth.EffectiveInternalKey() == "" {
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
