//go:build linux

package pxarmount

import (
	"path/filepath"
	"testing"
)

func TestBuildStackInitWithoutReader(t *testing.T) {
	stack, err := BuildStack(MountConfig{
		InitMode:   true,
		MountPoint: filepath.Join(t.TempDir(), "mount"),
	})
	if err != nil {
		t.Fatalf("BuildStack() error = %v", err)
	}
	if stack.MFS == nil {
		t.Fatal("BuildStack() did not create a mutable filesystem")
	}
	stack.Close()
}
