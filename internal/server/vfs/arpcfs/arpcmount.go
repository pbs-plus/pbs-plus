//go:build linux

package arpcfs

import (
	"os"
	"os/exec"

	"github.com/pbs-plus/pbs-plus/internal/log"
)

// forceUnmount lazily detaches a mountpoint whose FUSE server refused to go
// away. A plain umount on a mount whose agent is gone hangs forever and takes
// every later statfs on the host with it.
func forceUnmount(mountpoint string) {
	if mountpoint == "" {
		return
	}
	if err := exec.Command("fusermount", "-uz", mountpoint).Run(); err == nil {
		return
	}
	umount := exec.Command("umount", "-lf", mountpoint)
	umount.Env = os.Environ()
	if err := umount.Run(); err != nil {
		log.Error(err, "lazy unmount failed", "mountpoint", mountpoint)
	}
}

func MountARPC(f *ARPCFS, mountpoint string) error {
	fsName := "pbs-plus://" + f.Backup.ID

	umount := exec.Command("umount", "-lf", mountpoint)
	umount.Env = os.Environ()
	if err := umount.Run(); err != nil {
		log.Error(err, "")
	}

	server, err := MountFuse(mountpoint, fsName, f)
	if err != nil {
		return err
	}

	f.Fuse = server
	f.mountpoint = mountpoint

	if err := f.Fuse.WaitMount(); err != nil {
		log.Error(err, "")
	}
	return nil
}
