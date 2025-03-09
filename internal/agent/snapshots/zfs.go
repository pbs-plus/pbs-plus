package snapshots

import (
	"fmt"
	"os/exec"
	"runtime"
	"strings"
	"time"
)

type ZFSSnapshotHandler struct{}

func (z *ZFSSnapshotHandler) CreateSnapshot(jobId string, sourcePath string) (Snapshot, error) {
	if !z.IsSupported(sourcePath) {
		return Snapshot{}, fmt.Errorf("source path %q is not on a ZFS filesystem", sourcePath)
	}

	// ZFS snapshots are named as <dataset>@<snapshot_name>
	snapshotName := fmt.Sprintf("%s@%s", sourcePath, jobId)
	timeStarted := time.Now()

	cmd := exec.Command("zfs", "snapshot", snapshotName)
	if output, err := cmd.CombinedOutput(); err != nil {
		return Snapshot{}, fmt.Errorf("failed to create ZFS snapshot: %s, %w", string(output), err)
	}

	return Snapshot{
		Path:        snapshotName,
		TimeStarted: timeStarted,
		SourcePath:  sourcePath,
	}, nil
}

func (z *ZFSSnapshotHandler) DeleteSnapshot(snapshot Snapshot) error {
	cmd := exec.Command("zfs", "destroy", snapshot.Path)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to delete ZFS snapshot: %s, %w", string(output), err)
	}
	return nil
}

func (z *ZFSSnapshotHandler) IsSupported(sourcePath string) bool {
	if runtime.GOOS == "windows" {
		return false
	}

	cmd := exec.Command("zfs", "list", "-H", "-o", "name", sourcePath)
	output, err := cmd.Output()
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(output)) != ""
}
