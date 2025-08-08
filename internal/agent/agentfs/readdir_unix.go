//go:build unix

package agentfs

import (
	"fmt"
	"os"
	"sync"
	"syscall"
	"unsafe"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"golang.org/x/sys/unix"
)

const BUF_SIZE = 64 * 1024 // 64K buffer

type DirReaderUnix struct {
	fd          int
	buf         []byte
	bufPos      int
	bufEnd      int
	noMoreFiles bool
	path        string
}

var bufferPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, BUF_SIZE)
	},
}

func NewDirReaderUnix(path string) (*DirReaderUnix, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open directory '%s': %w", path, err)
	}

	return &DirReaderUnix{
		fd:   fd,
		buf:  bufferPool.Get().([]byte),
		path: path,
	}, nil
}

// Cross-platform getdents implementation
func (r *DirReaderUnix) getdents() (int, error) {
	return getdents(r.fd, r.buf)
}

func (r *DirReaderUnix) NextBatch() ([]byte, error) {
	if r.noMoreFiles {
		return nil, os.ErrProcessDone
	}

	entries := make(types.ReadDirEntries, 0, 256)

	for {
		if r.bufPos >= r.bufEnd {
			n, err := r.getdents()
			if err != nil {
				return nil, fmt.Errorf("getdents failed: %w", err)
			}
			if n == 0 {
				r.noMoreFiles = true
				break
			}
			r.bufPos = 0
			r.bufEnd = n
		}

		// Parse entries from buffer
		for r.bufPos < r.bufEnd {
			name, typ, reclen, err := r.parseDirent()
			if err != nil {
				return nil, err
			}
			if reclen == 0 {
				break
			}

			if name != "." && name != ".." {
				mode := unixTypeToFileMode(typ)
				entries = append(entries, types.AgentDirEntry{
					Name: name,
					Mode: uint32(mode),
				})
			}

			r.bufPos += reclen
		}

		if len(entries) > 0 {
			break
		}
	}

	if len(entries) == 0 {
		return nil, os.ErrProcessDone
	}

	encodedBatch, err := entries.Encode()
	if err != nil {
		return nil, fmt.Errorf("failed to encode batch for path '%s': %w", r.path, err)
	}

	return encodedBatch, nil
}

func (r *DirReaderUnix) Close() error {
	if err := syscall.Close(r.fd); err != nil {
		return fmt.Errorf("failed to close directory '%s': %w", r.path, err)
	}

	if r.buf != nil {
		bufferPool.Put(r.buf)
		r.buf = nil
	}

	return nil
}

func unixTypeToFileMode(t byte) os.FileMode {
	switch t {
	case syscall.DT_DIR:
		return os.ModeDir | 0755
	case syscall.DT_REG:
		return 0644
	case syscall.DT_LNK:
		return os.ModeSymlink | 0777
	case syscall.DT_CHR:
		return os.ModeDevice | os.ModeCharDevice | 0666
	case syscall.DT_BLK:
		return os.ModeDevice | 0666
	case syscall.DT_FIFO:
		return os.ModeNamedPipe | 0666
	case syscall.DT_SOCK:
		return os.ModeSocket | 0666
	default:
		return 0644
	}
}

// unixDirent64 matches the Linux getdents64 struct
type unixDirent64 struct {
	Ino     uint64
	Off     int64
	Reclen  uint16
	Type    byte
	NameBuf [0]byte
}

func getdents(fd int, buf []byte) (int, error) {
	return unix.Getdents(fd, buf)
}

func (r *DirReaderUnix) parseDirent() (name string, typ byte, reclen int, err error) {
	if r.bufPos+19 > r.bufEnd { // Minimum dirent64 size
		return "", 0, 0, nil
	}

	dirent := (*unixDirent64)(unsafe.Pointer(&r.buf[r.bufPos]))
	if dirent.Reclen == 0 || r.bufPos+int(dirent.Reclen) > r.bufEnd {
		return "", 0, 0, nil
	}

	nameBytes := unsafe.Slice(
		(*byte)(unsafe.Add(unsafe.Pointer(dirent), unsafe.Offsetof(dirent.NameBuf))),
		int(dirent.Reclen)-int(unsafe.Offsetof(dirent.NameBuf)),
	)

	// Find null terminator
	nameLen := 0
	for nameLen < len(nameBytes) && nameBytes[nameLen] != 0 {
		nameLen++
	}

	return string(nameBytes[:nameLen]), dirent.Type, int(dirent.Reclen), nil
}
