package snapshots

import (
	"errors"
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

var blockProviders = []SnapshotHandler{
	&LVMSnapshotHandler{},
}

func (m *SnapshotManager) CreateSnapshot(jobID string, sourcePath string) (Snapshot, error) {
	if runtime.GOOS == "linux" {
		return m.createLinux(jobID, sourcePath)
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

func (m *SnapshotManager) createLinux(jobID string, sourcePath string) (Snapshot, error) {
	mount, err := FindMount(sourcePath)
	if err != nil {
		return Snapshot{}, err
	}

	var failures []error
	for _, handler := range providersFor(mount.FSType) {
		snapshot, err := handler.CreateSnapshot(jobID, mount.MountPoint)
		if err == nil {
			return snapshot, nil
		}
		failures = append(failures, err)
	}

	return Snapshot{}, fmt.Errorf("no snapshot provider succeeded for %s (%s): %w",
		mount.MountPoint, mount.FSType, errors.Join(failures...))
}

func providersFor(fsType string) []SnapshotHandler {
	switch fsType {
	case "btrfs":
		return []SnapshotHandler{&BtrfsSnapshotHandler{}}
	case "zfs":
		return []SnapshotHandler{&ZFSSnapshotHandler{}}
	default:
		return blockProviders
	}
}
