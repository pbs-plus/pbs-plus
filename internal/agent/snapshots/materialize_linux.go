//go:build linux

package snapshots

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

const scratchRoot = "/run/pbs-plus/snapshots"

// Materialize exposes the snapshot as a readable directory tree and sets Path to its root.
// Directory snapshots are already readable; block snapshots get mounted on a scratch directory.
func Materialize(snap *Snapshot) error {
	if snap.Device == "" {
		if snap.SnapDir == "" {
			return errors.New("snapshot exposes neither a device nor a directory")
		}
		snap.Path = snap.SnapDir
		return nil
	}

	if err := os.MkdirAll(scratchRoot, 0700); err != nil {
		return fmt.Errorf("failed to create %s: %w", scratchRoot, err)
	}
	dir, err := os.MkdirTemp(scratchRoot, "snap-")
	if err != nil {
		return fmt.Errorf("failed to create snapshot mount directory: %w", err)
	}

	data := ""
	if snap.FSType == "xfs" {
		data = "nouuid"
	}
	if err := unix.Mount(snap.Device, dir, snap.FSType, unix.MS_RDONLY|unix.MS_NOATIME, data); err != nil {
		_ = os.Remove(dir)
		return fmt.Errorf("mount %s on %s: %w", snap.Device, dir, err)
	}

	snap.MountDir = dir
	snap.Path = dir
	snap.Mounted = true
	return nil
}

// Unmaterialize releases the snapshot mount so the provider can drop the snapshot itself.
func Unmaterialize(snap *Snapshot) error {
	if !snap.Mounted {
		return nil
	}
	err := unix.Unmount(snap.MountDir, 0)
	if err != nil {
		err = unix.Unmount(snap.MountDir, unix.MNT_DETACH)
	}
	snap.Mounted = false
	if err == nil {
		_ = os.Remove(snap.MountDir)
	}
	return err
}

// PrivateMounts stops mount propagation so snapshot mounts never escape this namespace.
func PrivateMounts() error {
	return unix.Mount("", "/", "", unix.MS_REC|unix.MS_PRIVATE, "")
}
