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

type LVMSnapshotHandler struct{}

func (l *LVMSnapshotHandler) CreateSnapshot(jobId string, sourcePath string) (Snapshot, error) {
	if !l.IsSupported(sourcePath) {
		return Snapshot{}, fmt.Errorf("source path %q is not on an LVM volume", sourcePath)
	}

	vgName, lvName, err := l.getVolumeGroupAndLogicalVolume(sourcePath)
	if err != nil {
		return Snapshot{}, err
	}

	snapshotLVName := fmt.Sprintf("%s-snap-%s", lvName, jobId)
	snapshotDev := fmt.Sprintf("/dev/%s/%s", vgName, snapshotLVName)

	mountPath := filepath.Join(os.TempDir(), "pbs-plus-lvm", jobId)

	timeStarted := time.Now()

	_ = l.DeleteSnapshot(Snapshot{Id: snapshotDev, Path: mountPath})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, "lvcreate",
		"--snapshot",
		"--name", snapshotLVName,
		"-l", "10%ORIGIN",
		fmt.Sprintf("/dev/%s/%s", vgName, lvName))

	if output, err := cmd.CombinedOutput(); err != nil {
		return Snapshot{}, fmt.Errorf("lvm lvcreate failed: %s, %w", string(output), err)
	}

	if err := os.MkdirAll(mountPath, 0750); err != nil {
		return Snapshot{}, fmt.Errorf("failed to create mount dir: %w", err)
	}

	mountCmd := exec.CommandContext(ctx, "mount", "-o", "ro,nouuid", snapshotDev, mountPath)
	if output, err := mountCmd.CombinedOutput(); err != nil {
		_ = exec.Command("lvremove", "-f", snapshotDev).Run()
		return Snapshot{}, fmt.Errorf("failed to mount LVM snapshot: %s, %w", string(output), err)
	}

	return Snapshot{
		Id:          snapshotDev,
		Path:        mountPath,
		TimeStarted: timeStarted,
		SourcePath:  sourcePath,
		Handler:     l,
	}, nil
}

func (l *LVMSnapshotHandler) DeleteSnapshot(snapshot Snapshot) error {
	if snapshot.Path != "" {
		_ = exec.Command("umount", "-l", snapshot.Path).Run()
		_ = os.Remove(snapshot.Path)
	}

	if snapshot.Id != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		cmd := exec.CommandContext(ctx, "lvremove", "-f", snapshot.Id)
		if output, err := cmd.CombinedOutput(); err != nil {
			if !strings.Contains(string(output), "not found") {
				return fmt.Errorf("failed to delete LVM LV: %s, %w", string(output), err)
			}
		}
	}
	return nil
}

func (l *LVMSnapshotHandler) IsSupported(sourcePath string) bool {
	cmd := exec.Command("lsblk", "-no", "TYPE", sourcePath)
	output, err := cmd.Output()
	if err != nil {
		return false
	}
	return strings.Contains(string(output), "lvm")
}

func (l *LVMSnapshotHandler) getVolumeGroupAndLogicalVolume(path string) (string, string, error) {
	cmd := exec.Command("lvs", "--noheadings", "-o", "vg_name,lv_name", "--select", fmt.Sprintf("path=%s", path))

	output, err := cmd.Output()
	if err != nil || len(strings.Fields(string(output))) < 2 {
		findDev := exec.Command("df", "--output=source", path)
		devOut, _ := findDev.Output()
		lines := strings.Split(strings.TrimSpace(string(devOut)), "\n")
		if len(lines) < 2 {
			return "", "", fmt.Errorf("could not determine LVM device for path %s", path)
		}
		devPath := strings.TrimSpace(lines[1])

		cmd = exec.Command("lvs", "--noheadings", "-o", "vg_name,lv_name", devPath)
		output, err = cmd.Output()
	}

	parts := strings.Fields(string(output))
	if len(parts) < 2 {
		return "", "", fmt.Errorf("path %s does not appear to be an LVM volume", path)
	}

	return parts[0], parts[1], nil
}
