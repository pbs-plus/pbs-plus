//go:build linux

package conf

import (
	"os"
	"testing"
)

func TestEnvConfig_Defaults(t *testing.T) {
	for _, key := range []string{"DEBUG", "PBS_PLUS_HOSTNAME"} {
		t.Setenv(key, "")
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
	}

	cfg := loadEnvConfig()

	tests := []struct {
		name string
		got  bool
		want bool
	}{
		{"Debug defaults to false", cfg.Debug, false},
		{"StdoutOnly defaults to false", cfg.StdoutOnly, false},
		{"DisableAutoUpdate defaults to false", cfg.DisableAutoUpdate, false},
		{"InsideContainer defaults to false", cfg.InsideContainer, false},
		{"ForceDisableIRM defaults to false", cfg.ForceDisableIRM, false},
		{"PprofEnabled defaults to false", cfg.PprofEnabled, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("got %v, want %v", tt.got, tt.want)
			}
		})
	}

	if cfg.Hostname != "" {
		t.Errorf("Hostname should be empty by default, got %q", cfg.Hostname)
	}
}

func TestEnvConfig_DebugEnabled(t *testing.T) {
	t.Setenv("DEBUG", "true")
	cfg := loadEnvConfig()
	if !cfg.Debug {
		t.Error("expected Debug to be true")
	}
}

func TestEnvConfig_HostnameSet(t *testing.T) {
	t.Setenv("PBS_PLUS_HOSTNAME", "test-host")
	cfg := loadEnvConfig()
	if cfg.Hostname != "test-host" {
		t.Errorf("expected 'test-host', got %q", cfg.Hostname)
	}
}
