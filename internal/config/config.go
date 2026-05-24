package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type APIKey struct {
	Name        string   `yaml:"name"`
	Key         string   `yaml:"key"`
	Permissions []string `yaml:"permissions"`
}

type AuthConfig struct {
	Keys []APIKey `yaml:"keys"`
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
	return &cfg, nil
}

func Save(path string, cfg *Config) error {
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
