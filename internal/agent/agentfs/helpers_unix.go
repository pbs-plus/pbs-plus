//go:build unix

package agentfs

import (
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

func lastPathElem(p string) string {
	if i := strings.LastIndexByte(p, '/'); i >= 0 {
		return p[i+1:]
	}
	return p
}

func modeFromUnix(m uint32) uint32 {
	mode := m & 0o777
	switch m & unix.S_IFMT {
	case unix.S_IFDIR:
		mode |= uint32(os.ModeDir)
	case unix.S_IFLNK:
		mode |= uint32(os.ModeSymlink)
	case unix.S_IFCHR:
		mode |= uint32(os.ModeDevice | os.ModeCharDevice)
	case unix.S_IFBLK:
		mode |= uint32(os.ModeDevice)
	case unix.S_IFIFO:
		mode |= uint32(os.ModeNamedPipe)
	case unix.S_IFSOCK:
		mode |= uint32(os.ModeSocket)
	}
	return mode
}
