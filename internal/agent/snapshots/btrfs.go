package snapshots

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

type BtrfsSnapshotHandler struct{}

func (b *BtrfsSnapshotHandler) CreateSnapshot(jobId string, sourcePath string) (Snapshot, error) {
	if !b.IsSupported(sourcePath) {
		return Snapshot{}, fmt.Errorf("source path %q is not on a Btrfs volume", sourcePath)
	}

	tmpDir := os.TempDir()
	snapshotPath := filepath.Join(tmpDir, "pbs-plus-btrfs", jobId)
	timeStarted := time.Now()

	_ = b.DeleteSnapshot(Snapshot{Path: snapshotPath})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, "btrfs", "subvolume", "snapshot", sourcePath, snapshotPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		return Snapshot{}, fmt.Errorf("failed to create Btrfs snapshot: %s, %w", string(output), err)
	}

	return Snapshot{
		Path:        snapshotPath,
		TimeStarted: timeStarted,
		SourcePath:  sourcePath,
		Handler:     b,
	}, nil
}

func (b *BtrfsSnapshotHandler) DeleteSnapshot(snapshot Snapshot) error {
	cmd := exec.Command("btrfs", "subvolume", "delete", snapshot.Path)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to delete Btrfs snapshot: %s, %w", string(output), err)
	}
	return nil
}

func (b *BtrfsSnapshotHandler) IsSupported(sourcePath string) bool {
	if runtime.GOOS == "windows" {
		return false
	}

	cmd := exec.Command("stat", "-f", "-c", "%T", sourcePath)
	output, err := cmd.Output()
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(output)) == "btrfs"
}
