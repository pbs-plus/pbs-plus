//go:build windows

package agent

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/alexflint/go-filemutex"
)

type BackupSessionData struct {
	Drive        string    `json:"drive"`
	StartTime    time.Time `json:"start_time"`
	NFSStartTime time.Time `json:"nfs_start_time"`
}

type BackupStore struct {
	filePath string
	fileLock *filemutex.FileMutex
}

func NewBackupStore() *BackupStore {
	execPath, err := os.Executable()
	if err != nil {
		panic(err)
	}
	dir := filepath.Dir(execPath)
	filePath := filepath.Join(dir, "backup_sessions.json")
	lockPath := filepath.Join(dir, "backup_sessions.lock")

	fl, err := filemutex.New(lockPath)
	if err != nil {
		panic(err)
	}

	return &BackupStore{
		filePath: filePath,
		fileLock: fl,
	}
}

func (bs *BackupStore) updateSessions(fn func(map[string]*BackupSessionData)) error {
	if err := bs.fileLock.Lock(); err != nil {
		return err
	}
	defer bs.fileLock.Unlock()

	sessions := make(map[string]*BackupSessionData)
	data, err := os.ReadFile(bs.filePath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err == nil {
		if err := json.Unmarshal(data, &sessions); err != nil {
			sessions = make(map[string]*BackupSessionData)
		}
	}

	fn(sessions)

	newData, err := json.MarshalIndent(sessions, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(bs.filePath, newData, 0644)
}

func (bs *BackupStore) StartBackup(drive string) error {
	return bs.updateSessions(func(sessions map[string]*BackupSessionData) {
		sessions[drive] = &BackupSessionData{
			Drive:     drive,
			StartTime: time.Now(),
		}
	})
}

func (bs *BackupStore) StartNFS(drive string) error {
	return bs.updateSessions(func(sessions map[string]*BackupSessionData) {
		if session, ok := sessions[drive]; ok {
			session.NFSStartTime = time.Now()
		}
	})
}

func (bs *BackupStore) EndBackup(drive string) error {
	return bs.updateSessions(func(sessions map[string]*BackupSessionData) {
		delete(sessions, drive)
	})
}

func (bs *BackupStore) HasActiveBackups() (bool, error) {
	if err := bs.fileLock.Lock(); err != nil {
		return false, err
	}
	defer bs.fileLock.Unlock()

	sessions := make(map[string]*BackupSessionData)
	data, err := os.ReadFile(bs.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	if err := json.Unmarshal(data, &sessions); err != nil {
		return false, err
	}
	return len(sessions) > 0, nil
}

func (bs *BackupStore) HasActiveBackupForDrive(drive string) (bool, error) {
	if err := bs.fileLock.Lock(); err != nil {
		return false, err
	}
	defer bs.fileLock.Unlock()

	sessions := make(map[string]*BackupSessionData)
	data, err := os.ReadFile(bs.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	if err := json.Unmarshal(data, &sessions); err != nil {
		return false, err
	}

	_, exists := sessions[drive]
	return exists, nil
}
