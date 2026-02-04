//go:build !windows

package agentfs

import (
	"errors"
	"io"
	"os"

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

	if r.buf == nil {
		r.buf = make([]byte, 64*1024)
	}

	fd := int(r.file.Fd())
	out := make([]types.AgentFileInfo, 0, min(limit, 128))

	const statxMask = unix.STATX_TYPE | unix.STATX_MODE | unix.STATX_SIZE |
		unix.STATX_BLOCKS | unix.STATX_ATIME | unix.STATX_MTIME | unix.STATX_CTIME

	for len(out) < limit {
		if r.bufp >= r.nbuf {
			r.bufp = 0
			nread, err := unix.Getdents(fd, r.buf)
			if err != nil {
				if errors.Is(err, unix.EBADF) {
					return nil, os.ErrClosed
				}
				return nil, err
			}
			r.nbuf = nread
			if nread <= 0 {
				r.noMoreFiles = true
				break
			}
		}

		remaining := r.buf[r.bufp:r.nbuf]
		nb, _, names := unix.ParseDirent(remaining, limit-len(out), nil)

		if nb == 0 {
			break
		}

		r.bufp += nb

		for _, name := range names {
			if name == "." || name == ".." {
				continue
			}

			var sx unix.Statx_t
			err := unix.Statx(fd, name, unix.AT_SYMLINK_NOFOLLOW|unix.AT_STATX_DONT_SYNC, statxMask, &sx)
			if err != nil {
				if errors.Is(err, unix.ENOENT) {
					continue
				}
				return nil, err
			}

			isDir := (sx.Mode & unix.S_IFMT) == unix.S_IFDIR

			mode := uint32(sx.Mode & 0777)
			if isDir {
				mode |= 0x80000000
			}

			info := types.AgentFileInfo{
				Name:           name,
				Size:           int64(sx.Size),
				Mode:           mode,
				IsDir:          isDir,
				ModTime:        statxTimestampToNano(sx.Mtime),
				CreationTime:   statxBirthTimeNano(&sx),
				LastAccessTime: statxTimestampToNano(sx.Atime),
				LastWriteTime:  statxTimestampToNano(sx.Mtime),
			}

			if !isDir && blockSize > 0 {
				if sx.Blocks > 0 {
					bytes := uint64(sx.Blocks) * 512
					info.Blocks = (bytes + blockSize - 1) / blockSize
				} else {
					sz := uint64(max(0, int64(sx.Size)))
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

func statxTimestampToNano(ts unix.StatxTimestamp) int64 {
	return int64(ts.Sec)*1e9 + int64(ts.Nsec)
}

func statxBirthTimeNano(sx *unix.Statx_t) int64 {
	if sx.Mask&unix.STATX_BTIME != 0 {
		return statxTimestampToNano(sx.Btime)
	}
	return statxTimestampToNano(sx.Ctime)
}
