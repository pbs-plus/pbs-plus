//go:build windows

package agent

import (
	"os"
	"path/filepath"

	"github.com/alexflint/go-filemutex"
)

func NewBackupStore() (*BackupStore, error) {
	execPath, err := os.Executable()
	if err != nil {
		panic(err)
	}
	dir := filepath.Dir(execPath)
	filePath := filepath.Join(dir, "backup_sessions.json")
	lockPath := filepath.Join(dir, "backup_sessions.lock")

	fl, err := filemutex.New(lockPath)
	if err != nil {
		return nil, err
	}

	return &BackupStore{
		filePath: filePath,
		fileLock: fl,
	}, nil
}
