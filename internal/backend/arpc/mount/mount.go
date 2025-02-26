//go:build linux

package mount

import (
	"os"
	"os/exec"

	arpcfs "github.com/sonroyaalmerol/pbs-plus/internal/backend/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/backend/arpc/fuse"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

func Mount(f *arpcfs.ARPCFS, mountpoint string) error {
	fsName := "agent://" + utils.Slugify(f.Hostname) + "/" + f.JobId

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
