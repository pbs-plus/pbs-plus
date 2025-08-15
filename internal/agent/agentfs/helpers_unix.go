//go:build unix

package agentfs

import (
	"os"
	"path/filepath"
	"syscall"

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

// unixTypeToFileMode maps a DT_* byte to an os.FileMode.
func unixTypeToFileMode(t byte) (m os.FileMode) {
	switch t {
	case syscall.DT_DIR:
		return os.ModeDir | 0o755
	case syscall.DT_REG:
		return 0o644
	case syscall.DT_LNK:
		return os.ModeSymlink | 0o777
	case syscall.DT_CHR:
		return os.ModeDevice | os.ModeCharDevice | 0o666
	case syscall.DT_BLK:
		return os.ModeDevice | 0o666
	case syscall.DT_FIFO:
		return os.ModeNamedPipe | 0o666
	case syscall.DT_SOCK:
		return os.ModeSocket | 0o666
	default:
		return 0o644
	}
}

func isDot(b []byte) bool {
	return len(b) == 1 && b[0] == '.'
}

func isDotDot(b []byte) bool {
	return len(b) == 2 && b[0] == '.' && b[1] == '.'
}
