//go:build linux

package arpcfs

import (
	"os"
	"os/exec"

	"github.com/pbs-plus/pbs-plus/internal/log"
)

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

	if err := f.Fuse.WaitMount(); err != nil {
		log.Error(err, "")
	}
	return nil
}
