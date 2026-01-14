//go:build unix

package agent

import (
	"path/filepath"

	"github.com/gofrs/flock"
)

func NewRestoreStore() (*RestoreStore, error) {
	dir := "/etc/pbs-plus-agent"
	filePath := filepath.Join(dir, "restore_sessions.json")
	lockPath := filepath.Join(dir, "restore_sessions.lock")

	fl := flock.New(lockPath)

	return &RestoreStore{
		filePath: filePath,
		fileLock: fl,
	}, nil
}
