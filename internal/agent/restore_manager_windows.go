//go:build windows

package agent

import (
	"os"
	"path/filepath"

	"github.com/gofrs/flock"
)

func NewRestoreStore() (*RestoreStore, error) {
	execPath, err := os.Executable()
	if err != nil {
		panic(err)
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
