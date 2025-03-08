package snapshots

import (
	"context"
	"fmt"
)

// SnapshotManager manages snapshot operations based on filesystem and OS detection
type SnapshotManager struct {
	handlerMap map[string]SnapshotHandler
}

// NewSnapshotManager initializes the manager with all available handlers
func NewSnapshotManager() *SnapshotManager {
	return &SnapshotManager{
		handlerMap: map[string]SnapshotHandler{
			"cephfs": &CephfsSnapshotHandler{},
			"btrfs":  &BtrfsSnapshotHandler{},
			"zfs":    &ZFSSnapshotHandler{},
			"lvm":    &LVMSnapshotHandler{},
			"ext4":   &EXT4XFSHandler{}, // EXT4 delegates to LVM
			"xfs":    &EXT4XFSHandler{}, // XFS delegates to LVM
			"ntfs":   &NtfsSnapshotHandler{},
			"refs":   &NtfsSnapshotHandler{},
			"fat32":  nil, // FAT32 does not support snapshots
			"exfat":  nil, // exFAT does not support snapshots
			"hfs+":   nil, // HFS+ does not support snapshots
		},
	}
}

// CreateSnapshot detects the filesystem and delegates to the appropriate handler
func (m *SnapshotManager) CreateSnapshot(ctx context.Context, jobId string, sourcePath string) (Snapshot, error) {
	fsType, err := detectFilesystem(sourcePath)
	if err != nil {
		return Snapshot{}, fmt.Errorf("failed to detect filesystem: %w", err)
	}

	handler, exists := m.handlerMap[fsType]
	if !exists || handler == nil {
		return Snapshot{}, fmt.Errorf("no snapshot handler available for filesystem type: %s", fsType)
	}

	return handler.CreateSnapshot(ctx, jobId, sourcePath)
}

// DeleteSnapshot delegates the deletion to the appropriate handler
func (m *SnapshotManager) DeleteSnapshot(snapshot Snapshot) error {
	fsType, err := detectFilesystem(snapshot.SourcePath)
	if err != nil {
		return fmt.Errorf("failed to detect filesystem: %w", err)
	}

	handler, exists := m.handlerMap[fsType]
	if !exists || handler == nil {
		return fmt.Errorf("no snapshot handler available for filesystem type: %s", fsType)
	}

	return handler.DeleteSnapshot(snapshot)
}
