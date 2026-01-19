package snapshots

import (
	"errors"
	"time"
)

type Snapshot struct {
	Path         string          `json:"path"`
	Id           string          `json:"id"`
	TimeStarted  time.Time       `json:"time_started"`
	SourcePath   string          `json:"source_path"`
	Direct       bool            `json:"direct"`
	RelativePath string          `json:"rel_path"`
	Handler      SnapshotHandler `json:"-"`
}

type SnapshotHandler interface {
	CreateSnapshot(jobId string, sourcePath string) (Snapshot, error)
	DeleteSnapshot(snapshot Snapshot) error
	IsSupported(sourcePath string) bool
}

var (
	ErrSnapshotTimeout  = errors.New("timeout waiting for in-progress snapshot")
	ErrSnapshotCreation = errors.New("failed to create snapshot")
	ErrInvalidSnapshot  = errors.New("invalid snapshot")
)
