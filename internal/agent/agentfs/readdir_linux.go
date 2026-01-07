//go:build linux

package agentfs

import (
	"strconv"
	"sync"
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

var (
	linuxHdrSize = int(unsafe.Offsetof(linuxDirent64{}.NameBuf))
	idCache      = make(map[uint32]string)
	idCacheLock  sync.RWMutex
)

func getIDString(id uint32) string {
	idCacheLock.RLock()
	if s, ok := idCache[id]; ok {
		idCacheLock.RUnlock()
		return s
	}
	idCacheLock.RUnlock()
	s := strconv.Itoa(int(id))
	idCacheLock.Lock()
	idCache[id] = s
	idCacheLock.Unlock()
	return s
}

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
	b := r.buf[nameStart : r.bufPos+reclen]
	i := 0
	for i < len(b) && b[i] != 0 {
		i++
	}
	return b[:i], dirent.Type, reclen, true, nil
}

func (r *DirReaderUnix) fillAttrs(info *types.AgentFileInfo) error {
	var stx unix.Statx_t
	mask := uint32(unix.STATX_MODE)
	if r.FetchFullAttrs {
		mask |= unix.STATX_SIZE | unix.STATX_MTIME | unix.STATX_BLOCKS | unix.STATX_UID | unix.STATX_GID
	}
	// Use AT_STATX_DONT_SYNC to avoid network latency on distributed filesystems
	if err := unix.Statx(r.fd, info.Name, unix.AT_SYMLINK_NOFOLLOW|unix.AT_STATX_DONT_SYNC, int(mask), &stx); err != nil {
		return err
	}
	info.Mode = uint32(modeFromUnix(uint32(stx.Mode)))
	info.IsDir = (stx.Mode & unix.S_IFMT) == unix.S_IFDIR
	if r.FetchFullAttrs {
		info.Size = int64(stx.Size)
		info.ModTime = time.Unix(int64(stx.Mtime.Sec), int64(stx.Mtime.Nsec)).UnixNano()
		info.Blocks = uint64(stx.Blocks)
		info.LastAccessTime = time.Unix(int64(stx.Atime.Sec), int64(stx.Atime.Nsec)).Unix()
		info.LastWriteTime = time.Unix(int64(stx.Mtime.Sec), int64(stx.Mtime.Nsec)).Unix()
		info.Owner = getIDString(stx.Uid)
		info.Group = getIDString(stx.Gid)
		info.FileAttributes = make(map[string]bool)
	}
	return nil
}
