//go:build linux

package s3fs

import (
	"os"
	"os/exec"

	"github.com/pbs-plus/pbs-plus/internal/backend/vfs/fuse"
)

func (f *S3FS) Mount(mountpoint string) error {
	fsName := "pbs-plus://" + f.Job.ID

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

func (fs *S3FS) Unmount() {
	if fs.Fuse != nil {
		_ = fs.Fuse.Unmount()
	}
	fs.cancel()
}

