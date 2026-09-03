package snapshots

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

const btrfsSnapshotDir = ".pbs-plus-snapshots"

type BtrfsSnapshotHandler struct{}

func (b *BtrfsSnapshotHandler) CreateSnapshot(jobID string, sourcePath string) (Snapshot, error) {
	mount, err := FindMount(sourcePath)
	if err != nil {
		return Snapshot{}, err
	}
	if mount.FSType != "btrfs" {
		return Snapshot{}, fmt.Errorf("%q is on %s, not btrfs", sourcePath, mount.FSType)
	}

	parent := filepath.Join(mount.MountPoint, btrfsSnapshotDir)
	if err := os.MkdirAll(parent, 0700); err != nil {
		return Snapshot{}, fmt.Errorf("failed to create %s: %w", parent, err)
	}

	snapshotPath := filepath.Join(parent, sanitizeVolumeName(jobID))
	timeStarted := time.Now()

	if _, err := os.Stat(snapshotPath); err == nil {
		if err := b.remove(snapshotPath); err != nil {
			return Snapshot{}, err
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, "btrfs", "subvolume", "snapshot", "-r", mount.MountPoint, snapshotPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		return Snapshot{}, fmt.Errorf("failed to create btrfs snapshot of %s: %s: %w", mount.MountPoint, strings.TrimSpace(string(output)), err)
	}

	return Snapshot{
		Path:        snapshotPath,
		TimeStarted: timeStarted,
		SourcePath:  sourcePath,
		MountPoint:  mount.MountPoint,
		SnapDir:     snapshotPath,
		Ref:         snapshotPath,
		FSType:      mount.FSType,
		Handler:     b,
	}, nil
}

func (b *BtrfsSnapshotHandler) DeleteSnapshot(snapshot Snapshot) error {
	if err := Unmaterialize(&snapshot); err != nil {
		return fmt.Errorf("failed to unmount btrfs snapshot at %s: %w", snapshot.MountDir, err)
	}
	return b.remove(snapshot.Ref)
}

func (b *BtrfsSnapshotHandler) remove(path string) error {
	if path == "" {
		return nil
	}
	if output, err := exec.Command("btrfs", "subvolume", "delete", path).CombinedOutput(); err != nil {
		return fmt.Errorf("failed to delete btrfs snapshot %s: %s: %w", path, strings.TrimSpace(string(output)), err)
	}
	return nil
}

func (b *BtrfsSnapshotHandler) IsSupported(sourcePath string) bool {
	mount, err := FindMount(sourcePath)
	return err == nil && mount.FSType == "btrfs"
}
