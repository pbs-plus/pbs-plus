//go:build unix

package agent

import (
	"path/filepath"

	"github.com/gofrs/flock"
)

func NewBackupStore() (*BackupStore, error) {
	dir := "/etc/pbs-plus-agent"
	filePath := filepath.Join(dir, "backup_sessions.json")
	lockPath := filepath.Join(dir, "backup_sessions.lock")

	fl := flock.New(lockPath)

	return &BackupStore{
		filePath: filePath,
		fileLock: fl,
	}, nil
}
