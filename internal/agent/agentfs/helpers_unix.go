//go:build unix

package agentfs

import (
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

func lastPathElem(p string) string {
	// Avoid allocations vs. filepath.Base? Base is fine and cross-platform.
	return filepath.Base(p)
}

func modeFromUnix(m uint32) uint32 {
	// Convert unix mode to os.FileMode-like bits stored in uint32
	// Preserve permission bits and set type bits approximated to Go's FileMode
	const (
		sIFMT   = unix.S_IFMT
		sIFDIR  = unix.S_IFDIR
		sIFLNK  = unix.S_IFLNK
		sIFCHR  = unix.S_IFCHR
		sIFBLK  = unix.S_IFBLK
		sIFIFO  = unix.S_IFIFO
		sIFSOCK = unix.S_IFSOCK
	)
	mode := m & 0o777
	switch m & sIFMT {
	case sIFDIR:
		mode |= uint32(os.ModeDir)
	case sIFLNK:
		mode |= uint32(os.ModeSymlink)
	case sIFCHR:
		mode |= uint32(os.ModeDevice | os.ModeCharDevice)
	case sIFBLK:
		mode |= uint32(os.ModeDevice)
	case sIFIFO:
		mode |= uint32(os.ModeNamedPipe)
	case sIFSOCK:
		mode |= uint32(os.ModeSocket)
	}
	return mode
}

func isDot(b []byte) bool {
	return len(b) == 1 && b[0] == '.'
}

func isDotDot(b []byte) bool {
	return len(b) == 2 && b[0] == '.' && b[1] == '.'
}
