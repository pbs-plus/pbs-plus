package agent

import (
	"encoding/json"
	"os"
	"time"

	"github.com/gofrs/flock"
)

type BackupSessionData struct {
	JobId     string    `json:"job_id"`
	StartTime time.Time `json:"start_time"`
}

type BackupStore struct {
	filePath string
	fileLock *flock.Flock
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

func (bs *BackupStore) StartBackup(jobId string) error {
	enableWakeLockSystem()

	return bs.updateSessions(func(sessions map[string]*BackupSessionData) {
		sessions[jobId] = &BackupSessionData{
			JobId:     jobId,
			StartTime: time.Now(),
		}
	})
}

func (bs *BackupStore) EndBackup(jobId string) error {
	hasActive, err := bs.HasActiveBackups()
	if err == nil && !hasActive {
		disableWakeLock()
	}

	return bs.updateSessions(func(sessions map[string]*BackupSessionData) {
		delete(sessions, jobId)
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

func (bs *BackupStore) HasActiveBackupForJob(job string) (bool, error) {
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

	_, exists := sessions[job]
	return exists, nil
}

func (bs *BackupStore) ClearAll() error {
	disableWakeLock()

	return bs.updateSessions(func(sessions map[string]*BackupSessionData) {
		for job := range sessions {
			delete(sessions, job)
		}
	})
}
