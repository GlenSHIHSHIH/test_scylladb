package config

import (
	"log"
	"os"

	"github.com/goccy/go-yaml"
)

type Config struct {
	Scylla ScyllaConfig `yaml:"scylla"`
}

type ScyllaConfig struct {
	Hosts    []string `yaml:"hosts"`
	Keyspace string   `yaml:"keyspace"`
	Username string   `yaml:"username"`
	Password string   `yaml:"password"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	log.Printf("Config loaded successfully from %s", path)
	return &cfg, nil
}
