//go:build linux

package mount

import (
	arpcfs "github.com/sonroyaalmerol/pbs-plus/internal/backend/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/backend/arpc/fuse"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

func Mount(f *arpcfs.ARPCFS, mountpoint string) error {
	fsName := "agent://" + utils.Slugify(f.Hostname) + "/" + f.Drive
	server, err := fuse.Mount(mountpoint, fsName, f)
	if err != nil {
		return err
	}

	f.Mount = server

	f.Mount.WaitMount()
	return nil
}
