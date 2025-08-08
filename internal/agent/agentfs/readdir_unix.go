//go:build unix

package agentfs

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
)

const BUF_SIZE = 1024 * 1024 // 1MB buffer, same as Windows version

type DirReaderUnix struct {
	fd          int
	buf         []byte
	bufPos      int
	bufEnd      int
	noMoreFiles bool
	path        string
}

func NewDirReaderUnix(path string) (*DirReaderUnix, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_DIRECTORY, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open directory '%s': %w", path, err)
	}

	return &DirReaderUnix{
		fd:   fd,
		buf:  make([]byte, BUF_SIZE),
		path: path,
	}, nil
}

// linuxDirent64 matches the Linux getdents64 struct
type linuxDirent64 struct {
	Ino     uint64
	Off     int64
	Reclen  uint16
	Type    byte
	NameBuf [0]byte // flexible array member
}

func (r *DirReaderUnix) NextBatch() ([]byte, error) {
	if r.noMoreFiles {
		return nil, os.ErrProcessDone
	}

	var entries types.ReadDirEntries

	for {
		// If buffer is exhausted, refill
		if r.bufPos >= r.bufEnd {
			n, err := syscall.Getdents(r.fd, r.buf)
			if err != nil {
				return nil, fmt.Errorf("getdents failed: %w", err)
			}
			if n == 0 {
				r.noMoreFiles = true
				return nil, os.ErrProcessDone
			}
			r.bufPos = 0
			r.bufEnd = n
		}

		// Parse one entry
		dirent := (*linuxDirent64)(unsafe.Pointer(&r.buf[r.bufPos]))
		if dirent.Reclen == 0 {
			return nil, fmt.Errorf("invalid dirent record length")
		}

		nameBytes := unsafe.Slice(
			(*byte)(unsafe.Add(unsafe.Pointer(dirent), unsafe.Offsetof(dirent.NameBuf))),
			int(dirent.Reclen)-int(unsafe.Offsetof(dirent.NameBuf)),
		)

		// Null-terminated string
		nameLen := 0
		for nameLen < len(nameBytes) && nameBytes[nameLen] != 0 {
			nameLen++
		}
		name := string(nameBytes[:nameLen])

		if name != "." && name != ".." {
			mode := unixTypeToFileMode(dirent.Type)
			entries = append(entries, types.AgentDirEntry{
				Name: name,
				Mode: uint32(mode),
			})
		}

		r.bufPos += int(dirent.Reclen)

		// Return batch if buffer is consumed or enough entries collected
		if r.bufPos >= r.bufEnd {
			break
		}
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
	default:
		return 0
	}
}
