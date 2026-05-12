package snapshots

import (
	"errors"
	"time"
)

// Snapshot represents a generic snapshot.
type Snapshot struct {
	Path        string          `json:"path"`
	TimeStarted time.Time       `json:"time_started"`
	SourcePath  string          `json:"source_path"`
	Direct      bool            `json:"direct"`
	Handler     SnapshotHandler `json:"-"`
}

// SnapshotHandler defines the interface for snapshot lifecycle operations.
type SnapshotHandler interface {
	CreateSnapshot(jobID, sourcePath string) (Snapshot, error)
	DeleteSnapshot(snapshot Snapshot) error
	IsSupported(sourcePath string) bool
}

// SnapshotProvider is the new interface for filesystem-specific snapshot
// providers. Each provider handles exactly one snapshot mechanism.
type SnapshotProvider interface {
	SnapshotHandler
	// Name returns a human-readable name for this provider (e.g. "blksnap", "btrfs-ioctl").
	Name() string
}

var (
	ErrSnapshotTimeout   = errors.New("timeout waiting for in-progress snapshot")
	ErrSnapshotCreation  = errors.New("failed to create snapshot")
	ErrInvalidSnapshot   = errors.New("invalid snapshot")
	ErrNoSnapshotSupport = errors.New("filesystem does not support snapshots, use file-level (direct) mode")
)
