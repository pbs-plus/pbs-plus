//go:build linux

package snapshots

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// LVMProvider creates snapshots using LVM (Logical Volume Manager).
// This is a fallback for ext4/XFS filesystems when blksnap is not available.
type LVMProvider struct{}

func (p *LVMProvider) Name() string { return "lvm" }

func (p *LVMProvider) CreateSnapshot(jobID, sourcePath string) (Snapshot, error) {
	if !p.IsSupported(sourcePath) {
		return Snapshot{}, fmt.Errorf("lvm: source path %q is not on an LVM volume", sourcePath)
	}

	vgName, lvName, err := p.getVolumeGroupAndLogicalVolume(sourcePath)
	if err != nil {
		return Snapshot{}, fmt.Errorf("lvm: %w", err)
	}

	snapshotName := fmt.Sprintf("%s-snap-%s", lvName, jobID)
	timeStarted := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, "lvcreate",
		"--snapshot", "--name", snapshotName,
		"--size", "1G",
		fmt.Sprintf("/dev/%s/%s", vgName, lvName),
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		return Snapshot{}, fmt.Errorf("lvm: lvcreate failed: %s, %w", string(output), err)
	}

	snapshotPath := filepath.Join("/dev", vgName, snapshotName)
	return Snapshot{
		Path:        snapshotPath,
		TimeStarted: timeStarted,
		SourcePath:  sourcePath,
		Handler:     p,
	}, nil
}

func (p *LVMProvider) DeleteSnapshot(snapshot Snapshot) error {
	cmd := exec.Command("lvremove", "-f", snapshot.Path)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("lvm: lvremove failed: %s, %w", string(output), err)
	}
	return nil
}

func (p *LVMProvider) IsSupported(sourcePath string) bool {
	cmd := exec.Command("lsblk", "-no", "TYPE", sourcePath)
	output, err := cmd.Output()
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(output)) == "lvm"
}

func (p *LVMProvider) getVolumeGroupAndLogicalVolume(sourcePath string) (string, string, error) {
	cmd := exec.Command("lvs", "--noheadings", "-o", "vg_name,lv_name", sourcePath)
	output, err := cmd.Output()
	if err != nil {
		return "", "", fmt.Errorf("get LVM details: %w", err)
	}

	parts := strings.Fields(string(output))
	if len(parts) < 2 {
		return "", "", fmt.Errorf("unexpected output from lvs: %s", string(output))
	}

	return parts[0], parts[1], nil
}
