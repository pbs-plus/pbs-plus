//go:build linux

package snapshots

import (
	"fmt"

	"golang.org/x/sys/unix"
)

func Materialize(snap *Snapshot) error {
	if snap.MountPoint == "" {
		return fmt.Errorf("snapshot has no mount point to stand in for")
	}

	switch {
	case snap.Device != "":
		data := ""
		if snap.FSType == "xfs" {
			data = "nouuid"
		}
		if err := unix.Mount(snap.Device, snap.MountPoint, snap.FSType, unix.MS_RDONLY|unix.MS_NOATIME, data); err != nil {
			return fmt.Errorf("mount %s on %s: %w", snap.Device, snap.MountPoint, err)
		}
	case snap.SnapDir != "":
		if err := unix.Mount(snap.SnapDir, snap.MountPoint, "", unix.MS_BIND, ""); err != nil {
			return fmt.Errorf("bind %s on %s: %w", snap.SnapDir, snap.MountPoint, err)
		}
		if err := unix.Mount("", snap.MountPoint, "", unix.MS_BIND|unix.MS_REMOUNT|unix.MS_RDONLY, ""); err != nil {
			_ = unix.Unmount(snap.MountPoint, unix.MNT_DETACH)
			return fmt.Errorf("remount %s read-only: %w", snap.MountPoint, err)
		}
	default:
		return fmt.Errorf("snapshot exposes neither a device nor a directory")
	}

	snap.Mounted = true
	return nil
}

func Unmaterialize(snap *Snapshot) error {
	if !snap.Mounted {
		return nil
	}
	err := unix.Unmount(snap.MountPoint, 0)
	if err != nil {
		err = unix.Unmount(snap.MountPoint, unix.MNT_DETACH)
	}
	snap.Mounted = false
	return err
}

func PrivateMounts() error {
	return unix.Mount("", "/", "", unix.MS_REC|unix.MS_PRIVATE, "")
}
