//go:build linux

package agent

import (
	"path/filepath"

	"github.com/alexflint/go-filemutex"
)

func NewBackupStore() (*BackupStore, error) {
	dir := "/etc/pbs-plus-agent"
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
