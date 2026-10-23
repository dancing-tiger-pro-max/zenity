// Package config loads and validates config.yaml, the single source of settings.
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Jetstream Jetstream `yaml:"jetstream"`
}

type Jetstream struct {
	URL string `yaml:"url"`
}

// Load reads and validates the file at path. Invalid config fails loudly.
func Load(path string) (Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	var c Config
	if err := yaml.Unmarshal(raw, &c); err != nil {
		return Config{}, fmt.Errorf("parse config %s: %w", path, err)
	}
	if c.Jetstream.URL == "" {
		return Config{}, fmt.Errorf("config %s: jetstream.url is required", path)
	}
	return c, nil
}
