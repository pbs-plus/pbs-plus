//go:build linux

package arpcfs

import (
	"os"
	"os/exec"

)

func MountARPC(f *ARPCFS, mountpoint string) error {
	fsName := "pbs-plus://" + f.Backup.ID

	umount := exec.Command("umount", "-lf", mountpoint)
	umount.Env = os.Environ()
	_ = umount.Run()

	server, err := MountFuse(mountpoint, fsName, f)
	if err != nil {
		return err
	}

	f.Fuse = server

	f.Fuse.WaitMount()
	return nil
}
