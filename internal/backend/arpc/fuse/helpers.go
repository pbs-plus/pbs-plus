package fuse

import (
	"os"

	"github.com/hanwen/go-fuse/v2/fuse"
)

func getMode(node os.FileInfo) uint32 {
	vfsMode := node.Mode()
	Mode := vfsMode.Perm()
	if vfsMode&os.ModeDir != 0 {
		Mode |= fuse.S_IFDIR
	} else if vfsMode&os.ModeSymlink != 0 {
		Mode |= fuse.S_IFLNK
	} else if vfsMode&os.ModeNamedPipe != 0 {
		Mode |= fuse.S_IFIFO
	} else {
		Mode |= fuse.S_IFREG
	}
	return uint32(Mode)
}
