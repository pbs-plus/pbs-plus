package tape

import (
	"os"
	"strings"
)

func ResolveDevice(path string) string {
	if path == "" {
		return path
	}
	if before, ok := strings.CutSuffix(path, "-sg"); ok {
		nstPath := before + "-nst"
		if _, err := os.Stat(nstPath); err == nil {
			return nstPath
		}
	}
	return path
}

func ResolveChanger(nameOrPath string) string {
	return resolveConfiguredDevice(nameOrPath, func(c *Config) []string {
		out := make([]string, 0, len(c.Changers))
		for _, ch := range c.Changers {
			if ch.Name == nameOrPath {
				return []string{ch.Path}
			}
		}
		return out
	})
}

func ResolveDrive(nameOrPath string) string {
	resolved := resolveConfiguredDevice(nameOrPath, func(c *Config) []string {
		for _, d := range c.Drives {
			if d.Name == nameOrPath {
				return []string{d.Path}
			}
		}
		return nil
	})
	return ResolveDevice(resolved)
}

func resolveConfiguredDevice(nameOrPath string, lookup func(*Config) []string) string {
	if nameOrPath == "" {
		return nameOrPath
	}
	if _, err := os.Stat(nameOrPath); err == nil {
		return nameOrPath
	}
	cfg, err := ReadConfig()
	if err != nil {
		return nameOrPath
	}
	if hits := lookup(cfg); len(hits) > 0 {
		return hits[0]
	}
	return nameOrPath
}
