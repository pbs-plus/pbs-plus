//go:build linux

package s3mount

import (
	"os"
	"os/exec"

	"github.com/pbs-plus/pbs-plus/internal/backend/vfs/s3fs"
	"github.com/pbs-plus/pbs-plus/internal/backend/vfs/s3fs/fuse"
)

func Mount(f *s3fs.S3FS, mountpoint string) error {
	fsName := "pbs-plus://" + f.Backup.ID

	umount := exec.Command("umount", "-lf", mountpoint)
	umount.Env = os.Environ()
	_ = umount.Run()

	server, err := fuse.Mount(mountpoint, fsName, f)
	if err != nil {
		return err
	}

	f.Fuse = server

	f.Fuse.WaitMount()
	return nil
}
