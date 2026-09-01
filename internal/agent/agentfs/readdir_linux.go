//go:build linux

package agentfs

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/fswire"
	"golang.org/x/sys/unix"
)

const (
	excludedAttrs = unix.STATX_ATTR_ENCRYPTED |
		unix.STATX_ATTR_VERITY |
		unix.STATX_ATTR_AUTOMOUNT

	statxMask = unix.STATX_TYPE | unix.STATX_MODE | unix.STATX_SIZE |
		unix.STATX_BLOCKS | unix.STATX_ATIME | unix.STATX_MTIME | unix.STATX_CTIME

	direntHeaderLen = 19

	statParallelThreshold = 32
)

// statWorkerLimit caps goroutines overlapping statx calls; tests set it to 1 for the serial path.
var statWorkerLimit = 16

type dirent struct {
	ino  uint64
	name string
	typ  uint8
}

func (r *DirReader) readdir(n int, blockSize uint64) ([]fswire.AgentFileInfo, error) {
	if r.closed {
		return nil, os.ErrClosed
	}

	wantAll := n <= 0
	limit := n
	if wantAll {
		limit = int(^uint(0) >> 1)
	}

	fd := int(r.file.Fd())
	out := make([]fswire.AgentFileInfo, 0, min(limit, defaultBatchSize))
	ents := make([]dirent, 0, min(limit, defaultBatchSize))

	fullByteBuf := unsafe.Slice((*byte)(unsafe.Pointer(&r.buf[0])), len(r.buf)*8)

	for len(out) < limit {
		if r.bufp >= r.nbuf {
			nread, err := unix.Getdents(fd, fullByteBuf)
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

		nb, parsed := parseDirents(fullByteBuf[r.bufp:r.nbuf], limit-len(out), ents[:0])
		if nb <= 0 {
			break
		}
		r.bufp += nb
		ents = parsed
		if len(ents) == 0 {
			continue
		}

		slices.SortFunc(ents, func(a, b dirent) int {
			return cmp.Compare(a.ino, b.ino)
		})

		var err error
		out, err = statDirents(out, fd, ents, blockSize)
		if err != nil {
			return nil, err
		}
	}

	if len(out) == 0 && r.noMoreFiles && n > 0 {
		return nil, io.EOF
	}
	return out, nil
}

// parseDirents decodes linux_dirent64 records, keeping the d_ino that unix.ParseDirent discards.
func parseDirents(buf []byte, max int, dst []dirent) (int, []dirent) {
	consumed := 0

	for len(buf) >= direntHeaderLen && len(dst) < max {
		reclen := int(binary.NativeEndian.Uint16(buf[16:18]))
		if reclen < direntHeaderLen || reclen > len(buf) {
			break
		}

		ino := binary.NativeEndian.Uint64(buf[0:8])
		typ := buf[18]
		name := buf[direntHeaderLen:reclen]
		if i := bytes.IndexByte(name, 0); i >= 0 {
			name = name[:i]
		}

		buf = buf[reclen:]
		consumed += reclen

		if ino == 0 || len(name) == 0 {
			continue
		}
		if string(name) == "." || string(name) == ".." {
			continue
		}

		dst = append(dst, dirent{ino: ino, name: string(name), typ: typ})
	}

	return consumed, dst
}

func statWorkers(n int) int {
	if n < statParallelThreshold || statWorkerLimit <= 1 {
		return 1
	}
	workers := max(min(n/statParallelThreshold, statWorkerLimit), 2)
	return workers
}

// statDirents stats ents across a worker pool, dropping vanished and excluded entries.
func statDirents(out []fswire.AgentFileInfo, fd int, ents []dirent, blockSize uint64) ([]fswire.AgentFileInfo, error) {
	start := len(out)
	out = slices.Grow(out, len(ents))
	out = out[:start+len(ents)]
	infos := out[start:]
	clear(infos)

	workers := statWorkers(len(ents))
	if workers <= 1 {
		for i := range ents {
			_, err := statDirent(fd, ents[i], blockSize, &infos[i])
			if err != nil {
				clear(infos)
				return out[:start], err
			}
		}
	} else {
		var next atomic.Int64
		var wg sync.WaitGroup
		errs := make([]error, workers)

		for w := range workers {
			wg.Go(func() {
				for {
					i := int(next.Add(1)) - 1
					if i >= len(ents) {
						return
					}
					_, err := statDirent(fd, ents[i], blockSize, &infos[i])
					if err != nil {
						errs[w] = err
						return
					}
				}
			})
		}
		wg.Wait()

		for _, err := range errs {
			if err != nil {
				clear(infos)
				return out[:start], err
			}
		}
	}

	write := start
	for i := start; i < len(out); i++ {
		if out[i].Name != "" {
			out[write] = out[i]
			write++
		}
	}
	clear(out[write:])
	return out[:write], nil
}

func statDirent(fd int, ent dirent, blockSize uint64, info *fswire.AgentFileInfo) (bool, error) {
	if shouldExcludeDirent(ent.typ) {
		return false, nil
	}

	var sx unix.Statx_t
	err := statxDirent(fd, ent.name, &sx)
	if err != nil {
		if errors.Is(err, unix.ENOENT) {
			return false, nil
		}
		return false, err
	}

	if shouldExcludeStatx(&sx) {
		return false, nil
	}

	isDir := (sx.Mode & unix.S_IFMT) == unix.S_IFDIR

	mode := uint32(sx.Mode & 0777)
	if isDir {
		mode |= 0x80000000
	}

	*info = fswire.AgentFileInfo{
		Name:           ent.name,
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

	return true, nil
}

func shouldExcludeDirent(fileType uint8) bool {
	switch fileType {
	case unix.DT_SOCK, unix.DT_BLK, unix.DT_CHR, unix.DT_LNK:
		return true
	default:
		return false
	}
}

// statxDirent bypasses unix.Statx so the pathname remains stack-backed instead of allocating per entry.
func statxDirent(fd int, name string, sx *unix.Statx_t) error {
	if len(name) > unix.NAME_MAX {
		return unix.ENAMETOOLONG
	}

	var path [unix.NAME_MAX + 1]byte
	copy(path[:], name)
	_, _, errno := unix.Syscall6(
		unix.SYS_STATX,
		uintptr(fd),
		uintptr(unsafe.Pointer(&path[0])),
		uintptr(unix.AT_SYMLINK_NOFOLLOW|unix.AT_STATX_DONT_SYNC),
		uintptr(statxMask),
		uintptr(unsafe.Pointer(sx)),
		0,
	)
	runtime.KeepAlive(&path)
	if errno != 0 {
		return errno
	}
	return nil
}

func statxBirthTimeNano(sx *unix.Statx_t) int64 {
	if sx.Mask&unix.STATX_BTIME != 0 {
		return statxTimestampToNano(sx.Btime)
	}
	return statxTimestampToNano(sx.Ctime)
}

func shouldExcludeStatx(sx *unix.Statx_t) bool {
	fileType := sx.Mode & unix.S_IFMT

	if fileType == unix.S_IFSOCK || fileType == unix.S_IFBLK || fileType == unix.S_IFCHR || fileType == unix.S_IFLNK {
		return true
	}

	if sx.Attributes_mask&excludedAttrs != 0 && sx.Attributes&excludedAttrs != 0 {
		return true
	}

	return false
}

func statxTimestampToNano(ts unix.StatxTimestamp) int64 {
	return int64(ts.Sec)*1e9 + int64(ts.Nsec)
}
