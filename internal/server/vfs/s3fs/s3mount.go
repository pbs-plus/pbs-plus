//go:build linux

package s3fs

import (
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/vfs"
)

func MountS3(f *S3FS, mountpoint string) error {
	fsName := "pbs-plus://" + f.Backup.ID

	// Nothing is normally mounted here, so a failure is the expected case and not worth logging.
	_ = vfs.ForceUnmount(mountpoint)

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
