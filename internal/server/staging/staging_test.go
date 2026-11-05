//go:build linux

package staging

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/conf"
)

func TestRootPrefersOverride(t *testing.T) {
	previous := conf.Env.StagingDir
	t.Cleanup(func() { conf.Env.StagingDir = previous })

	override := filepath.Join(t.TempDir(), "nested", "staging")
	conf.Env.StagingDir = override

	if got := Root("any-store"); got != override {
		t.Fatalf("Root = %q, want %q", got, override)
	}
	info, err := os.Stat(override)
	if err != nil {
		t.Fatal(err)
	}
	if mode := info.Mode().Perm(); mode&0o111 != 0o111 {
		t.Fatalf("staging mode = %o, want it traversable", mode)
	}
}

func TestRootMakesExistingDirectoryTraversable(t *testing.T) {
	previous := conf.Env.StagingDir
	t.Cleanup(func() { conf.Env.StagingDir = previous })

	override := filepath.Join(t.TempDir(), "staging")
	if err := os.Mkdir(override, 0o700); err != nil {
		t.Fatal(err)
	}
	conf.Env.StagingDir = override

	if got := Root("any-store"); got != override {
		t.Fatalf("Root = %q, want %q", got, override)
	}
	info, err := os.Stat(override)
	if err != nil {
		t.Fatal(err)
	}
	if mode := info.Mode().Perm(); mode&0o111 != 0o111 {
		t.Fatalf("staging mode = %o, want it traversable", mode)
	}
}

func TestRootWithoutStore(t *testing.T) {
	previous := conf.Env.StagingDir
	t.Cleanup(func() { conf.Env.StagingDir = previous })
	conf.Env.StagingDir = ""

	if got := Root(""); got != "" {
		t.Fatalf("Root = %q, want the system temporary directory", got)
	}
}

func TestRootFallsBackWhenUnusable(t *testing.T) {
	previous := conf.Env.StagingDir
	t.Cleanup(func() { conf.Env.StagingDir = previous })

	blocker := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(blocker, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	conf.Env.StagingDir = filepath.Join(blocker, "staging")

	if got := Root("any-store"); got != "" {
		t.Fatalf("Root = %q, want the system temporary directory", got)
	}
}
