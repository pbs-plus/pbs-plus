package snapshots

import (
	"fmt"
	"runtime"
	"strings"
)

type SnapshotManager struct {
	handlerMap map[string]SnapshotHandler
}

var Manager = &SnapshotManager{
	handlerMap: map[string]SnapshotHandler{
		"ntfs":  &NtfsSnapshotHandler{},
		"refs":  &NtfsSnapshotHandler{},
		"fat32": nil,
		"exfat": nil,
		"hfs+":  nil,
	},
}

func (m *SnapshotManager) CreateSnapshot(jobID string, sourcePath string) (Snapshot, error) {
	if runtime.GOOS == "linux" {
		return Snapshot{}, fmt.Errorf("snapshots are unavailable on Linux")
	}

	fsType, err := detectFilesystem(sourcePath)
	if err != nil {
		return Snapshot{}, fmt.Errorf("failed to detect filesystem: %w", err)
	}

	handler, exists := m.handlerMap[strings.ToLower(fsType)]
	if !exists || handler == nil {
		return Snapshot{}, fmt.Errorf("no snapshot handler available for filesystem type: %s", fsType)
	}

	return handler.CreateSnapshot(jobID, sourcePath)
}
