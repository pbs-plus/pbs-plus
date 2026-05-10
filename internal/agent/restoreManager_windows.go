//go:build windows

package agent

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/gofrs/flock"
)

func NewRestoreStore() (*RestoreStore, error) {
	execPath, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("NewRestoreStore: %w", err)
	}
	dir := filepath.Dir(execPath)
	filePath := filepath.Join(dir, "restore_sessions.json")
	lockPath := filepath.Join(dir, "restore_sessions.lock")

	fl := flock.New(lockPath)

	return &RestoreStore{
		filePath: filePath,
		fileLock: fl,
	}, nil
}
