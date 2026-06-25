//go:build linux

package s3fs

import (
	"github.com/pbs-plus/pbs-plus/internal/log"
	"os"
	"os/exec"
)

func MountS3(f *S3FS, mountpoint string) error {
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
