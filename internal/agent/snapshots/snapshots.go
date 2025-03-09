package snapshots

import (
	"errors"
	"time"
)

// Snapshot represents a generic snapshot
type Snapshot struct {
	Path        string          `json:"path"`
	TimeStarted time.Time       `json:"time_started"`
	SourcePath  string          `json:"source_path"`
	Direct      bool            `json:"direct"`
	Handler     SnapshotHandler `json:"-"`
}

// SnapshotHandler defines the interface for snapshot operations
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
