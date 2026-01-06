//go:build linux

package arpcmount

import (
	"os"
	"os/exec"

	arpcfs "github.com/pbs-plus/pbs-plus/internal/backend/vfs/arpc"
	arpcfuse "github.com/pbs-plus/pbs-plus/internal/backend/vfs/arpc/fuse"
)

func Mount(f *arpcfs.ARPCFS, mountpoint string) error {
	fsName := "pbs-plus://" + f.Backup.ID

	umount := exec.Command("umount", "-lf", mountpoint)
	umount.Env = os.Environ()
	_ = umount.Run()

	server, err := arpcfuse.Mount(mountpoint, fsName, f)
	if err != nil {
		return err
	}

	f.Fuse = server

	f.Fuse.WaitMount()
	return nil
}
