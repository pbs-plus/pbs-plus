//go:build !windows && !linux

package agentfs

import (
	"errors"
	"io"
	"os"
	"unsafe"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"golang.org/x/sys/unix"
)

func (r *DirReader) readdir(n int, blockSize uint64) ([]types.AgentFileInfo, error) {
	if r.closed {
		return nil, os.ErrClosed
	}

	wantAll := n <= 0
	limit := n
	if wantAll {
		limit = int(^uint(0) >> 1)
	}

	fd := int(r.file.Fd())
	out := make([]types.AgentFileInfo, 0, min(limit, 128))

	fullByteBuf := unsafe.Slice((*byte)(unsafe.Pointer(&r.buf[0])), len(r.buf)*8)

	for len(out) < limit {
		if r.bufp >= r.nbuf {
			nread, err := unix.Getdents(int(r.file.Fd()), fullByteBuf)
			if err != nil {
				if errors.Is(err, unix.EBADF) {
					return nil, os.ErrClosed
				}
				return nil, err
			}
			r.nbuf = nread
			r.bufp = 0
			if nread <= 0 {
				r.noMoreFiles = true
				break
			}
		}

		remaining := fullByteBuf[r.bufp:r.nbuf]
		nb, _, names := unix.ParseDirent(remaining, limit-len(out), nil)

		if nb <= 0 {
			break
		}

		r.bufp += nb

		for _, name := range names {
			if name == "." || name == ".." {
				continue
			}

			var st unix.Stat_t
			err := unix.Fstatat(fd, name, &st, unix.AT_SYMLINK_NOFOLLOW)
			if err != nil {
				if errors.Is(err, unix.ENOENT) {
					continue
				}
				return nil, err
			}

			isDir := (st.Mode & unix.S_IFMT) == unix.S_IFDIR

			mode := uint32(st.Mode & 0777)
			if isDir {
				mode |= 0x80000000
			}

			info := types.AgentFileInfo{
				Name:           name,
				Size:           st.Size,
				Mode:           mode,
				IsDir:          isDir,
				ModTime:        timespecToNano(st.Mtim),
				CreationTime:   getBirthTimeNano(&st),
				LastAccessTime: timespecToNano(st.Atim),
				LastWriteTime:  timespecToNano(st.Mtim),
			}

			if !isDir && blockSize > 0 {
				if st.Blocks > 0 {
					bytes := uint64(st.Blocks) * 512
					info.Blocks = (bytes + blockSize - 1) / blockSize
				} else {
					sz := uint64(max(0, st.Size))
					info.Blocks = (sz + blockSize - 1) / blockSize
				}
			}
			out = append(out, info)
		}
	}

	if len(out) == 0 && r.noMoreFiles && n > 0 {
		return nil, io.EOF
	}
	return out, nil
}

func timespecToNano(ts unix.Timespec) int64 {
	return int64(ts.Sec)*1e9 + int64(ts.Nsec)
}

func getBirthTimeNano(st *unix.Stat_t) int64 {
	// FreeBSD and some BSDs have Birthtimespec
	if st.Btim.Sec != 0 || st.Btim.Nsec != 0 {
		return timespecToNano(st.Btim)
	}
	// Fallback to ctime
	return timespecToNano(st.Ctim)
}
