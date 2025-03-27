//go:build windows

package agent

import (
	"os"
	"path/filepath"

	"github.com/gofrs/flock"
)

func NewBackupStore() (*BackupStore, error) {
	execPath, err := os.Executable()
	if err != nil {
		panic(err)
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
