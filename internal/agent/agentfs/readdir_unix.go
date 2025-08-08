//go:build unix

package agentfs

import (
	"fmt"
	"os"
	"syscall"

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
	fd, err := syscall.Open(path, syscall.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open directory '%s': %w", path, err)
	}

	return &DirReaderUnix{
		fd:   fd,
		buf:  make([]byte, BUF_SIZE),
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

	var entries types.ReadDirEntries

	for {
		// If buffer is exhausted, refill
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

		// If we have entries or reached end of buffer, return batch
		if len(entries) > 0 || r.bufPos >= r.bufEnd {
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
