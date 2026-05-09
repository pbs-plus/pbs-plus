//go:build windows

package agent

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/gofrs/flock"
)

func NewBackupStore() (*BackupStore, error) {
	execPath, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("NewBackupStore: %w", err)
	}
	dir := filepath.Dir(execPath)
	filePath := filepath.Join(dir, "backup_sessions.json")
	lockPath := filepath.Join(dir, "backup_sessions.lock")

	fl := flock.New(lockPath)

	return &BackupStore{
		filePath: filePath,
		fileLock: fl,
	}, nil
}
