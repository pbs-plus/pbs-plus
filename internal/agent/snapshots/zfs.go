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

type ZFSSnapshotHandler struct{}

func (z *ZFSSnapshotHandler) CreateSnapshot(jobID string, sourcePath string) (Snapshot, error) {
	mount, err := FindMount(sourcePath)
	if err != nil {
		return Snapshot{}, err
	}
	if mount.FSType != "zfs" {
		return Snapshot{}, fmt.Errorf("%q is on %s, not zfs", sourcePath, mount.FSType)
	}

	dataset := mount.Source
	snapName := sanitizeVolumeName(jobID)
	ref := fmt.Sprintf("%s@%s", dataset, snapName)
	timeStarted := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if output, err := exec.CommandContext(ctx, "zfs", "snapshot", ref).CombinedOutput(); err != nil {
		return Snapshot{}, fmt.Errorf("failed to create zfs snapshot %s: %s: %w", ref, strings.TrimSpace(string(output)), err)
	}

	snapDir := filepath.Join(mount.MountPoint, ".zfs", "snapshot", snapName)
	if _, err := os.Stat(snapDir); err != nil {
		_ = z.remove(ref)
		return Snapshot{}, fmt.Errorf("zfs snapshot %s is not reachable at %s: %w", ref, snapDir, err)
	}

	return Snapshot{
		Path:        snapDir,
		TimeStarted: timeStarted,
		SourcePath:  sourcePath,
		MountPoint:  mount.MountPoint,
		SnapDir:     snapDir,
		Ref:         ref,
		FSType:      mount.FSType,
		Handler:     z,
	}, nil
}

func (z *ZFSSnapshotHandler) DeleteSnapshot(snapshot Snapshot) error {
	if err := Unmaterialize(&snapshot); err != nil {
		return fmt.Errorf("failed to unmount zfs snapshot at %s: %w", snapshot.MountPoint, err)
	}
	return z.remove(snapshot.Ref)
}

func (z *ZFSSnapshotHandler) remove(ref string) error {
	if ref == "" {
		return nil
	}
	if output, err := exec.Command("zfs", "destroy", ref).CombinedOutput(); err != nil {
		return fmt.Errorf("failed to delete zfs snapshot %s: %s: %w", ref, strings.TrimSpace(string(output)), err)
	}
	return nil
}

func (z *ZFSSnapshotHandler) IsSupported(sourcePath string) bool {
	mount, err := FindMount(sourcePath)
	return err == nil && mount.FSType == "zfs"
}
