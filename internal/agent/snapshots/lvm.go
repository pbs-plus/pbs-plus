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

type lvmVolume struct {
	vg     string
	lv     string
	layout string
}

func (l *LVMSnapshotHandler) CreateSnapshot(jobID string, sourcePath string) (Snapshot, error) {
	mount, err := FindMount(sourcePath)
	if err != nil {
		return Snapshot{}, err
	}

	vol, err := l.lookupVolume(mount.Source)
	if err != nil {
		return Snapshot{}, fmt.Errorf("%q is not backed by an LVM logical volume: %w", sourcePath, err)
	}

	snapshotName := fmt.Sprintf("%s_snap_%s", vol.lv, sanitizeVolumeName(jobID))
	origin := filepath.Join("/dev", vol.vg, vol.lv)
	snapshotDev := filepath.Join("/dev", vol.vg, snapshotName)
	timeStarted := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	args := []string{"--snapshot", "--name", snapshotName}
	thin := strings.Contains(vol.layout, "thin")
	if !thin {
		args = append(args, "--extents", "20%ORIGIN")
	}
	args = append(args, origin)

	if output, err := exec.CommandContext(ctx, "lvcreate", args...).CombinedOutput(); err != nil {
		return Snapshot{}, fmt.Errorf("failed to create LVM snapshot of %s: %s: %w", origin, strings.TrimSpace(string(output)), err)
	}

	if thin {
		if output, err := exec.CommandContext(ctx, "lvchange", "--activate", "y", "--ignoreactivationskip", snapshotDev).CombinedOutput(); err != nil {
			_ = l.remove(snapshotDev)
			return Snapshot{}, fmt.Errorf("failed to activate thin snapshot %s: %s: %w", snapshotDev, strings.TrimSpace(string(output)), err)
		}
	}

	return Snapshot{
		Path:        mount.MountPoint,
		TimeStarted: timeStarted,
		SourcePath:  sourcePath,
		MountPoint:  mount.MountPoint,
		Device:      snapshotDev,
		Ref:         snapshotDev,
		FSType:      mount.FSType,
		Handler:     l,
	}, nil
}

func (l *LVMSnapshotHandler) DeleteSnapshot(snapshot Snapshot) error {
	if err := Unmaterialize(&snapshot); err != nil {
		return fmt.Errorf("failed to unmount LVM snapshot at %s: %w", snapshot.MountPoint, err)
	}
	return l.remove(snapshot.Ref)
}

func (l *LVMSnapshotHandler) remove(dev string) error {
	if dev == "" {
		return nil
	}
	if output, err := exec.Command("lvremove", "--force", dev).CombinedOutput(); err != nil {
		return fmt.Errorf("failed to delete LVM snapshot %s: %s: %w", dev, strings.TrimSpace(string(output)), err)
	}
	return nil
}

func (l *LVMSnapshotHandler) IsSupported(sourcePath string) bool {
	mount, err := FindMount(sourcePath)
	if err != nil {
		return false
	}
	_, err = l.lookupVolume(mount.Source)
	return err == nil
}

func (l *LVMSnapshotHandler) lookupVolume(device string) (lvmVolume, error) {
	if device == "" {
		return lvmVolume{}, fmt.Errorf("mount has no backing device")
	}

	cmd := exec.Command("lvs", "--noheadings", "--separator", "|", "-o", "vg_name,lv_name,lv_layout", device)
	output, err := cmd.Output()
	if err != nil {
		return lvmVolume{}, fmt.Errorf("lvs failed for %s: %w", device, err)
	}

	fields := strings.Split(strings.TrimSpace(string(output)), "|")
	if len(fields) < 3 {
		return lvmVolume{}, fmt.Errorf("unexpected lvs output for %s: %q", device, string(output))
	}

	return lvmVolume{
		vg:     strings.TrimSpace(fields[0]),
		lv:     strings.TrimSpace(fields[1]),
		layout: strings.TrimSpace(fields[2]),
	}, nil
}

func sanitizeVolumeName(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '.', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}
