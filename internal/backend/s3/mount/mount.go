//go:build linux

package mount

import (
	"os"
	"os/exec"

	s3fs "github.com/pbs-plus/pbs-plus/internal/backend/s3"
	"github.com/pbs-plus/pbs-plus/internal/backend/s3/fuse"
)

func Mount(f *s3fs.S3FS, mountpoint string) error {
	fsName := "pbs-plus://" + f.Job.ID

	umount := exec.Command("umount", "-lf", mountpoint)
	umount.Env = os.Environ()
	_ = umount.Run()

	server, err := fuse.Mount(mountpoint, fsName, f)
	if err != nil {
		return err
	}

	f.Mount = server

	f.Mount.WaitMount()
	return nil
}
