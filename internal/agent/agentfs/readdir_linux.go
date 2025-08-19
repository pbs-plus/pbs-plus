//go:build linux

package agentfs

import (
	"time"
	"unsafe"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"golang.org/x/sys/unix"
)

type linuxDirent64 struct {
	Ino     uint64
	Off     int64
	Reclen  uint16
	Type    byte
	NameBuf [0]byte
}

var linuxHdrSize = int(unsafe.Offsetof(linuxDirent64{}.NameBuf))

func (r *DirReaderUnix) getdents() (int, error) {
	for {
		n, err := unix.Getdents(r.fd, r.buf)
		if err == unix.EINTR || err == unix.EAGAIN {
			continue
		}
		return n, err
	}
}

func (r *DirReaderUnix) parseDirent() ([]byte, byte, int, bool, error) {
	if r.bufPos+linuxHdrSize > r.bufEnd {
		return nil, 0, 0, false, nil
	}

	dirent := (*linuxDirent64)(unsafe.Pointer(&r.buf[r.bufPos]))
	reclen := int(dirent.Reclen)
	if reclen < linuxHdrSize || r.bufPos+reclen > r.bufEnd {
		return nil, 0, 0, false, unix.EBADF
	}

	nameStart := r.bufPos + linuxHdrSize
	nameEnd := r.bufPos + reclen

	b := r.buf[nameStart:nameEnd]
	i := 0
	for i < len(b) && b[i] != 0 {
		i++
	}
	return b[:i], dirent.Type, reclen, true, nil
}

// fillAttrs fetches full attributes using statx on Linux.
// If FetchFullAttrs is false but called due to DT_UNKNOWN or similar,
// we still request a minimal mask to get mode/isdir/mtime/size as needed.
func (r *DirReaderUnix) fillAttrs(info *types.AgentFileInfo) error {
	var stx unix.Statx_t

	// Minimal mask: mode always needed; size+mtime if full attrs requested.
	mask := uint32(unix.STATX_MODE)
	if r.FetchFullAttrs {
		mask |= unix.STATX_SIZE | unix.STATX_MTIME | unix.STATX_BLOCKS
	}

	// Do not follow symlinks, consistent with your handleAttr.
	flags := int(unix.AT_SYMLINK_NOFOLLOW)

	if err := unix.Statx(r.fd, info.Name, flags, int(mask), &stx); err != nil {
		return err
	}

	// Mode/IsDir
	info.Mode = uint32(modeFromUnix(uint32(stx.Mode)))
	info.IsDir = (stx.Mode & unix.S_IFMT) == unix.S_IFDIR

	if r.FetchFullAttrs {
		info.Size = int64(stx.Size)
		info.ModTime = time.Unix(int64(stx.Mtime.Sec), int64(stx.Mtime.Nsec))
		if info.IsDir {
			info.Blocks = 0
		} else {
			info.Blocks = uint64(stx.Blocks)
		}
	}

	return nil
}
