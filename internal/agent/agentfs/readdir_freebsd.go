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

	// FreeBSD d_name is a flexible array; safe slicing logic:
	nameStart := r.bufPos + nameOff
	raw := r.buf[nameStart : nameStart+namelen]

	return raw, byte(de.Type), reclen, true, nil
}

func (r *DirReaderUnix) fillAttrs(info *types.AgentFileInfo) error {
	var st syscall.Stat_t
	if err := syscall.Fstatat(r.fd, info.Name, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return err
	}

	info.Mode = uint32(modeFromUnix(uint32(st.Mode)))
	info.IsDir = (st.Mode & syscall.S_IFMT) == syscall.S_IFDIR

	if r.FetchFullAttrs {
		info.Size = st.Size
		info.ModTime = time.Unix(int64(st.Mtimespec.Sec), int64(st.Mtimespec.Nsec)).UnixNano()

		if info.IsDir {
			info.Blocks = 0
		} else {
			info.Blocks = uint64(st.Blocks)
		}

		info.CreationTime = time.Unix(int64(st.Birthtimespec.Sec), int64(st.Birthtimespec.Nsec)).Unix()
		info.LastAccessTime = time.Unix(int64(st.Atimespec.Sec), int64(st.Atimespec.Nsec)).Unix()
		info.LastWriteTime = time.Unix(int64(st.Mtimespec.Sec), int64(st.Mtimespec.Nsec)).Unix()

		info.Owner = getIDString(st.Uid)
		info.Group = getIDString(st.Gid)

		info.FileAttributes = make(map[string]bool)
	}
	return nil
}
