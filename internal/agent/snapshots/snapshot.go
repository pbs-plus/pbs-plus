package snapshots

import (
	"errors"
	"time"
)

type Snapshot struct {
	Path        string          `json:"path"`
	TimeStarted time.Time       `json:"time_started"`
	SourcePath  string          `json:"source_path"`
	Direct      bool            `json:"direct"`
	Handler     SnapshotHandler `json:"-"`

	MountPoint string `json:"mount_point,omitempty"`
	MountDir   string `json:"mount_dir,omitempty"`
	Device     string `json:"device,omitempty"`
	SnapDir    string `json:"snap_dir,omitempty"`
	Ref        string `json:"ref,omitempty"`
	FSType     string `json:"fs_type,omitempty"`
	Mounted    bool   `json:"mounted,omitempty"`
}

type SnapshotHandler interface {
	CreateSnapshot(jobID string, sourcePath string) (Snapshot, error)
	DeleteSnapshot(snapshot Snapshot) error
	IsSupported(sourcePath string) bool
}

var (
	ErrSnapshotTimeout  = errors.New("timeout waiting for in-progress snapshot")
	ErrSnapshotCreation = errors.New("failed to create snapshot")
	ErrInvalidSnapshot  = errors.New("invalid snapshot")
)
