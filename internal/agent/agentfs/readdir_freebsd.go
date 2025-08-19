//go:build freebsd

package agentfs

import (
	"syscall"
	"time"
	"unsafe"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"golang.org/x/sys/unix"
)

func (r *DirReaderUnix) getdents() (int, error) {
	for {
		n, err := syscall.Getdirentries(r.fd, r.buf, &r.basep)
		if err == syscall.EINTR || err == syscall.EAGAIN {
			continue
		}
		return n, err
	}
}

func (r *DirReaderUnix) parseDirent() ([]byte, byte, int, bool, error) {
	nameOff := int(unsafe.Offsetof(syscall.Dirent{}.Name))
	if r.bufPos+nameOff > r.bufEnd {
		return nil, 0, 0, false, nil
	}
	de := (*syscall.Dirent)(unsafe.Pointer(&r.buf[r.bufPos]))
	reclen := int(de.Reclen)
	if reclen < nameOff || r.bufPos+reclen > r.bufEnd {
		return nil, 0, 0, false, syscall.EBADF
	}

	namelen := int(de.Namlen)
	if namelen < 0 {
		return nil, 0, 0, false, syscall.EBADF
	}
	if namelen > len(de.Name) {
		namelen = len(de.Name)
	}
	raw := (*[256]byte)(unsafe.Pointer(&de.Name[0]))[:namelen:namelen]
	return raw, byte(de.Type), reclen, true, nil
}

// fillAttrs fetches attributes using fstatat on FreeBSD.
func (r *DirReaderUnix) fillAttrs(info *types.AgentFileInfo) error {
	var st syscall.Stat_t
	if err := syscall.Fstatat(r.fd, info.Name, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return err
	}

	info.Mode = uint32(modeFromUnix(uint32(st.Mode)))
	info.IsDir = (st.Mode & syscall.S_IFMT) == syscall.S_IFDIR

	if r.FetchFullAttrs {
		info.Size = st.Size
		// FreeBSD Stat_t has Timespec fields
		info.ModTime = time.Unix(int64(st.Mtimespec.Sec), int64(st.Mtimespec.Nsec))
		if info.IsDir {
			info.Blocks = 0
		} else {
			// st.Blocks in 512B units (POSIX)
			info.Blocks = uint64(st.Blocks)
		}
	}
	return nil
}
