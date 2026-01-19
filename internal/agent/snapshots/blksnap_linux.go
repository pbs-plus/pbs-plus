//go:build linux

package snapshots

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/agent/snapshots/blksnap"
)

type BlksnapSnapshotHandler struct {
	DiffStorageDir string
}

func (b *BlksnapSnapshotHandler) CreateSnapshot(jobId string, sourcePath string) (Snapshot, error) {
	devicePath, err := b.getDeviceForPath(sourcePath)
	if err != nil {
		return Snapshot{}, fmt.Errorf("failed to find device for %s: %w", sourcePath, err)
	}

	ctrl, err := blksnap.OpenControl()
	if err != nil {
		return Snapshot{}, fmt.Errorf("failed to open blksnap control: %w", err)
	}
	defer ctrl.Close()

	if b.DiffStorageDir == "" {
		b.DiffStorageDir = "/var/lib/pbs-plus-agent/blksnap"
	}
	_ = os.MkdirAll(b.DiffStorageDir, 0700)
	diffPath := filepath.Join(b.DiffStorageDir, jobId+".diff")

	limitSectors := uint64(10 * 1024 * 1024 * 1024 / 512)

	snap, err := ctrl.CreateSnapshot(diffPath, limitSectors)
	if err != nil {
		return Snapshot{}, fmt.Errorf("failed to create blksnap session: %w", err)
	}

	tracker, err := blksnap.NewTracker(devicePath)
	if err != nil {
		snap.Destroy()
		return Snapshot{}, fmt.Errorf("failed to create tracker for %s: %w", devicePath, err)
	}
	defer tracker.Close()

	if err := tracker.Attach(); err != nil {
		snap.Destroy()
		return Snapshot{}, fmt.Errorf("failed to attach blksnap filter: %w", err)
	}

	if err := tracker.AddToSnapshot(snap.ID); err != nil {
		snap.Destroy()
		return Snapshot{}, fmt.Errorf("failed to add device to snapshot group: %w", err)
	}

	if err := snap.Take(); err != nil {
		snap.Destroy()
		return Snapshot{}, fmt.Errorf("failed to take blksnap: %w", err)
	}

	snapInfo, err := tracker.GetSnapshotInfo()
	if err != nil {
		snap.Destroy()
		return Snapshot{}, fmt.Errorf("failed to get snapshot info: %w", err)
	}
	snapshotDevice := snapInfo.ImageName()

	mountPath := filepath.Join(os.TempDir(), "pbs-plus-blksnap", jobId)
	if err := os.MkdirAll(mountPath, 0750); err != nil {
		snap.Destroy()
		return Snapshot{}, err
	}

	mountCmd := exec.Command("mount", "-o", "ro,nouuid", snapshotDevice, mountPath)
	if output, err := mountCmd.CombinedOutput(); err != nil {
		snap.Destroy()
		return Snapshot{}, fmt.Errorf("mount failed: %s, %w", string(output), err)
	}

	return Snapshot{
		Id:          snap.ID.String(),
		SnapBlock:   snapshotDevice,
		Path:        mountPath,
		SourcePath:  sourcePath,
		TimeStarted: time.Now(),
		JobId:       jobId,
		Handler:     b,
	}, nil
}

func (b *BlksnapSnapshotHandler) DeleteSnapshot(snapshot Snapshot) error {
	if snapshot.Path != "" {
		_ = exec.Command("umount", "-l", snapshot.Path).Run()
		_ = os.Remove(snapshot.Path)
	}

	ctrl, err := blksnap.OpenControl()
	if err != nil {
		return fmt.Errorf("failed to open blksnap control for deletion: %w", err)
	}
	defer ctrl.Close()

	uuid, err := blksnap.ParseUUID(snapshot.Id)
	if err == nil {
		snapshotBlk := ctrl.OpenSnapshot(uuid)
		_ = snapshotBlk.Destroy()
	}

	diffPath := filepath.Join(b.DiffStorageDir, snapshot.JobId+".diff")
	_ = os.Remove(diffPath)

	return nil
}

func (b *BlksnapSnapshotHandler) IsSupported(sourcePath string) bool {
	ctrl, err := blksnap.OpenControl()
	if err != nil {
		return false
	}
	ctrl.Close()

	dev, err := b.getDeviceForPath(sourcePath)
	if err != nil {
		return false
	}

	return strings.HasPrefix(dev, "/dev/")
}

func (b *BlksnapSnapshotHandler) getDeviceForPath(path string) (string, error) {
	cmd := exec.Command("df", "--output=source", path)
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) < 2 {
		return "", fmt.Errorf("could not find device for path")
	}
	return strings.TrimSpace(lines[1]), nil
}
