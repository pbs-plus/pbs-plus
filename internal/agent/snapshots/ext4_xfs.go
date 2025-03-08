package snapshots

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
)

type EXT4XFSHandler struct{}

func (e *EXT4XFSHandler) CreateSnapshot(ctx context.Context, jobId string, sourcePath string) (Snapshot, error) {
	if !e.IsSupported(sourcePath) {
		return Snapshot{}, fmt.Errorf("source path %q is not on an EXT4 or XFS filesystem", sourcePath)
	}

	// Delegate to LVM handler
	lvmHandler := &LVMSnapshotHandler{}
	if !lvmHandler.IsSupported(sourcePath) {
		return Snapshot{}, fmt.Errorf("EXT4/XFS snapshot requires LVM, but LVM is not supported for %q", sourcePath)
	}

	return lvmHandler.CreateSnapshot(ctx, jobId, sourcePath)
}

func (e *EXT4XFSHandler) DeleteSnapshot(snapshot Snapshot) error {
	// Delegate to LVM handler
	lvmHandler := &LVMSnapshotHandler{}
	return lvmHandler.DeleteSnapshot(snapshot)
}

func (e *EXT4XFSHandler) IsSupported(sourcePath string) bool {
	cmd := exec.Command("lsblk", "-no", "FSTYPE", sourcePath)
	output, err := cmd.Output()
	if err != nil {
		return false
	}
	fsType := strings.TrimSpace(string(output))
	return fsType == "ext4" || fsType == "xfs"
}
