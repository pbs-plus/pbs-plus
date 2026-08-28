package proxmox

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEnsureGroupPathCreatesFullChain(t *testing.T) {
	base := t.TempDir()

	if err := EnsureGroupPath(base, "test", "host", "my-host"); err != nil {
		t.Fatalf("EnsureGroupPath: %v", err)
	}

	for _, dir := range []string{
		filepath.Join(base, "ns"),
		filepath.Join(base, "ns", "test"),
		filepath.Join(base, "ns", "test", "host"),
		filepath.Join(base, "ns", "test", "host", "my-host"),
	} {
		if err := checkDir(dir); err != nil {
			t.Fatalf("chain dir %s: %v", dir, err)
		}
	}
}

func TestEnsureGroupPathNestedNamespace(t *testing.T) {
	base := t.TempDir()

	if err := EnsureGroupPath(base, "a/b", "host", ""); err != nil {
		t.Fatalf("EnsureGroupPath: %v", err)
	}

	for _, dir := range []string{
		filepath.Join(base, "ns", "a"),
		filepath.Join(base, "ns", "a", "ns", "b"),
		filepath.Join(base, "ns", "a", "ns", "b", "host"),
	} {
		if err := checkDir(dir); err != nil {
			t.Fatalf("chain dir %s: %v", dir, err)
		}
	}
}

func TestEnsureGroupPathRequiresTypeForID(t *testing.T) {
	if err := EnsureGroupPath(t.TempDir(), "test", "", "some-id"); err == nil {
		t.Fatal("expected error when backup id is set without backup type")
	}
}

func checkDir(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return os.ErrInvalid
	}
	return nil
}
