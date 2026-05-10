//go:build linux

package s3fs

import (
	"os"
	"os/exec"

)

func MountS3(f *S3FS, mountpoint string) error {
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
