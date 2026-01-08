package config

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
)

type AppConfig struct {
	Scripts ScriptsConfig `toml:"scripts"`
	// Server   ServerConfig   `toml:"server"`
}

func Load(path string) (*AppConfig, error) {
	cfg := &AppConfig{}

	if _, err := os.Stat(path); os.IsNotExist(err) {
		cfg = createDefaults()
		if err := saveToDisk(path, cfg); err != nil {
			return nil, err
		}
	} else {
		if _, err := toml.DecodeFile(path, cfg); err != nil {
			return nil, fmt.Errorf("failed to decode config: %w", err)
		}
	}

	return cfg, nil
}

func createDefaults() *AppConfig {
	return &AppConfig{
		Scripts: ScriptsConfig{
			AllowedCommands: allowedCommands,
			AllowedPaths:    []string{"/dev/null", "/dev/urandom"},
		},
	}
}

func saveToDisk(path string, cfg *AppConfig) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return toml.NewEncoder(f).Encode(cfg)
}
