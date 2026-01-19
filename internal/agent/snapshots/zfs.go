package snapshots

import (
	"context"
	"fmt"
	"os/exec"
	"runtime"
	"strings"
	"time"
)

type ZFSSnapshotHandler struct{}

func (z *ZFSSnapshotHandler) CreateSnapshot(jobId string, sourcePath string) (Snapshot, error) {
	dataset, err := z.getDatasetName(sourcePath)
	if err != nil {
		return Snapshot{}, fmt.Errorf("source path %q is not on a ZFS volume: %w", sourcePath, err)
	}

	snapshotTag := fmt.Sprintf("pbs-plus-%s", jobId)
	snapshotFullIdentifier := fmt.Sprintf("%s@%s", dataset, snapshotTag)

	snapshotMountPath := fmt.Sprintf("%s/.zfs/snapshot/%s", strings.TrimSuffix(sourcePath, "/"), snapshotTag)

	timeStarted := time.Now()

	_ = z.DeleteSnapshot(Snapshot{Path: snapshotFullIdentifier})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, "zfs", "snapshot", snapshotFullIdentifier)
	if output, err := cmd.CombinedOutput(); err != nil {
		return Snapshot{}, fmt.Errorf("failed to create ZFS snapshot: %s, %w", string(output), err)
	}

	return Snapshot{
		Id:          snapshotFullIdentifier,
		Path:        snapshotMountPath,
		TimeStarted: timeStarted,
		JobId:       jobId,
		SourcePath:  sourcePath,
		Handler:     z,
	}, nil
}

func (z *ZFSSnapshotHandler) DeleteSnapshot(snapshot Snapshot) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	checkCmd := exec.CommandContext(ctx, "zfs", "list", "-H", "-o", "name", snapshot.Id)
	if err := checkCmd.Run(); err != nil {
		return nil
	}

	cmd := exec.CommandContext(ctx, "zfs", "destroy", snapshot.Id)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to delete ZFS snapshot %q: %s, %w", snapshot.Path, string(output), err)
	}
	return nil
}

func (z *ZFSSnapshotHandler) IsSupported(sourcePath string) bool {
	_, err := z.getDatasetName(sourcePath)
	return err == nil
}

func (z *ZFSSnapshotHandler) getDatasetName(sourcePath string) (string, error) {
	if runtime.GOOS == "windows" {
		return "", fmt.Errorf("ZFS not supported on Windows")
	}

	cmd := exec.Command("zfs", "list", "-H", "-o", "name", sourcePath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", err
	}

	dataset := strings.TrimSpace(string(output))
	if dataset == "" {
		return "", fmt.Errorf("not a ZFS dataset")
	}
	return dataset, nil
}
