package snapshots

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type LVMSnapshotHandler struct{}

func (l *LVMSnapshotHandler) CreateSnapshot(ctx context.Context, jobId string, sourcePath string) (Snapshot, error) {
	if !l.IsSupported(sourcePath) {
		return Snapshot{}, fmt.Errorf("source path %q is not on an LVM volume", sourcePath)
	}

	// Extract volume group and logical volume from the source path
	vgName, lvName, err := l.getVolumeGroupAndLogicalVolume(sourcePath)
	if err != nil {
		return Snapshot{}, fmt.Errorf("failed to get volume group and logical volume: %w", err)
	}

	snapshotName := fmt.Sprintf("%s-snap-%s", lvName, jobId)
	timeStarted := time.Now()

	cmd := exec.CommandContext(ctx, "lvcreate", "--snapshot", "--name", snapshotName, "--size", "1G", fmt.Sprintf("/dev/%s/%s", vgName, lvName))
	if output, err := cmd.CombinedOutput(); err != nil {
		return Snapshot{}, fmt.Errorf("failed to create LVM snapshot: %s, %w", string(output), err)
	}

	snapshotPath := filepath.Join("/dev", vgName, snapshotName)
	return Snapshot{
		Path:        snapshotPath,
		TimeStarted: timeStarted,
		SourcePath:  sourcePath,
		Handler:     l,
	}, nil
}

func (l *LVMSnapshotHandler) DeleteSnapshot(snapshot Snapshot) error {
	cmd := exec.Command("lvremove", "-f", snapshot.Path)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to delete LVM snapshot: %s, %w", string(output), err)
	}
	return nil
}

func (l *LVMSnapshotHandler) IsSupported(sourcePath string) bool {
	cmd := exec.Command("lsblk", "-no", "TYPE", sourcePath)
	output, err := cmd.Output()
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(output)) == "lvm"
}

func (l *LVMSnapshotHandler) getVolumeGroupAndLogicalVolume(sourcePath string) (string, string, error) {
	cmd := exec.Command("lvs", "--noheadings", "-o", "vg_name,lv_name", sourcePath)
	output, err := cmd.Output()
	if err != nil {
		return "", "", fmt.Errorf("failed to get LVM details: %w", err)
	}

	parts := strings.Fields(string(output))
	if len(parts) < 2 {
		return "", "", fmt.Errorf("unexpected output from lvs command: %s", string(output))
	}

	return parts[0], parts[1], nil
}
