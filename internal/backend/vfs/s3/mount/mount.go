package s3mount

import (
	"os"
	"os/exec"

	s3fs "github.com/pbs-plus/pbs-plus/internal/backend/vfs/s3"
	s3fuse "github.com/pbs-plus/pbs-plus/internal/backend/vfs/s3/fuse"
)

func Mount(f *s3fs.S3FS, mountpoint string) error {
	fsName := "pbs-plus://" + f.Job.ID

	umount := exec.Command("umount", "-lf", mountpoint)
	umount.Env = os.Environ()
	_ = umount.Run()

	server, err := s3fuse.Mount(mountpoint, fsName, f)
	if err != nil {
		return err
	}

	f.Fuse = server

	f.Fuse.WaitMount()
	return nil
}
