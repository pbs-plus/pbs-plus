package agent

import (
	"encoding/json"
	"os"
	"time"

	"github.com/gofrs/flock"
)

type RestoreSessionData struct {
	JobId     string    `json:"job_id"`
	StartTime time.Time `json:"start_time"`
}

type RestoreStore struct {
	filePath string
	fileLock *flock.Flock
}

func (bs *RestoreStore) updateSessions(fn func(map[string]*RestoreSessionData)) error {
	if err := bs.fileLock.Lock(); err != nil {
		return err
	}
	defer bs.fileLock.Unlock()

	sessions := make(map[string]*RestoreSessionData)
	data, err := os.ReadFile(bs.filePath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err == nil {
		if err := json.Unmarshal(data, &sessions); err != nil {
			sessions = make(map[string]*RestoreSessionData)
		}
	}

	fn(sessions)

	newData, err := json.MarshalIndent(sessions, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(bs.filePath, newData, 0644)
}

func (bs *RestoreStore) StartRestore(jobId string) error {
	enableWakeLockSystem()

	return bs.updateSessions(func(sessions map[string]*RestoreSessionData) {
		sessions[jobId] = &RestoreSessionData{
			JobId:     jobId,
			StartTime: time.Now(),
		}
	})
}

func (bs *RestoreStore) EndRestore(jobId string) error {
	hasActive, err := bs.HasActiveRestores()
	if err == nil && !hasActive {
		disableWakeLock()
	}

	return bs.updateSessions(func(sessions map[string]*RestoreSessionData) {
		delete(sessions, jobId)
	})
}

func (bs *RestoreStore) HasActiveRestores() (bool, error) {
	if err := bs.fileLock.Lock(); err != nil {
		return false, err
	}
	defer bs.fileLock.Unlock()

	sessions := make(map[string]*RestoreSessionData)
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

func (bs *RestoreStore) HasActiveRestoreForJob(job string) (bool, error) {
	if err := bs.fileLock.Lock(); err != nil {
		return false, err
	}
	defer bs.fileLock.Unlock()

	sessions := make(map[string]*RestoreSessionData)
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

func (bs *RestoreStore) ClearAll() error {
	disableWakeLock()

	return bs.updateSessions(func(sessions map[string]*RestoreSessionData) {
		for job := range sessions {
			delete(sessions, job)
		}
	})
}
