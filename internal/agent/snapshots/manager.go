package snapshots

import (
	"fmt"
	"path/filepath"
)

type SnapshotManager struct {
	handlerMap map[string]SnapshotHandler
}

var Manager = &SnapshotManager{
	handlerMap: map[string]SnapshotHandler{
		"btrfs": &BtrfsSnapshotHandler{},
		"zfs":   &ZFSSnapshotHandler{},
		"lvm":   &LVMSnapshotHandler{},
		"ext2":  NewKernelSnapshotHandler(),
		"ext3":  NewKernelSnapshotHandler(),
		"ext4":  NewKernelSnapshotHandler(),
		"xfs":   NewKernelSnapshotHandler(),
		"ntfs":  &NtfsSnapshotHandler{},
		"refs":  &NtfsSnapshotHandler{},
		"fat32": NewKernelSnapshotHandler(),
		"exfat": NewKernelSnapshotHandler(),
		"hfs+":  nil,
	},
}

func getRelativePath(sourcePath, mountPointPath string) (string, error) {
	source := filepath.Clean(sourcePath)
	mount := filepath.Clean(mountPointPath)

	rel, err := filepath.Rel(mount, source)
	if err != nil {
		return "", fmt.Errorf("failed to get relative path: %w", err)
	}

	if rel == "." {
		return "", nil
	}

	return rel, nil
}

func (m *SnapshotManager) CreateSnapshot(jobId string, sourcePath string) (Snapshot, error) {
	fsType, mountPointPath, err := detectFilesystem(sourcePath)
	if err != nil {
		return Snapshot{}, fmt.Errorf("failed to detect filesystem: %w", err)
	}

	handler, exists := m.handlerMap[fsType]
	if !exists || handler == nil {
		return Snapshot{}, fmt.Errorf("no snapshot handler available for filesystem type: %s", fsType)
	}

	relPath, err := getRelativePath(sourcePath, mountPointPath)
	if err != nil {
		return Snapshot{}, fmt.Errorf("failed to detect relative path to mount point: %w", err)
	}

	snap, err := handler.CreateSnapshot(jobId, mountPointPath)
	if err != nil {
		return Snapshot{}, err
	}

	snap.RelativePath = relPath

	return snap, nil
}

func (m *SnapshotManager) DeleteSnapshot(snapshot Snapshot) error {
	fsType, _, err := detectFilesystem(snapshot.SourcePath)
	if err != nil {
		return fmt.Errorf("failed to detect filesystem: %w", err)
	}

	handler, exists := m.handlerMap[fsType]
	if !exists || handler == nil {
		return fmt.Errorf("no snapshot handler available for filesystem type: %s", fsType)
	}

	return handler.DeleteSnapshot(snapshot)
}
