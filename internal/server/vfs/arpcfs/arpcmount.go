//go:build linux

package arpcfs

import (
	"github.com/pbs-plus/pbs-plus/internal/server/vfs"
)

func MountARPC(f *ARPCFS, mountpoint string) error {
	fsName := "pbs-plus://" + f.Backup.ID

	// Nothing is normally mounted here, so a failure is the expected case and not worth logging.
	if err := vfs.ForceUnmount(f.Ctx, mountpoint); err != nil {
		return err
	}

	server, err := MountFuse(mountpoint, fsName, f)
	if err != nil {
		return err
	}

	f.Fuse = server
	f.mountpoint = mountpoint

	return f.Fuse.WaitMount()
}
