//go:build unix

package agentfs

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"golang.org/x/sys/unix"
)

const (
	// Larger pooled buffer to reduce getdents syscalls.
	// Tuneable via env/constructor if needed.
	defaultBufSize          = 1024 * 1024 // 1 MiB
	defaultTargetEncodedLen = 1024 * 1024 // 1 MiB of encoded payload per batch
)

// DirReaderUnix reads directory entries in batches.
type DirReaderUnix struct {
	fd          int
	buf         []byte
	bufPos      int
	bufEnd      int
	noMoreFiles bool
	path        string
	basep       uintptr // used on FreeBSD

	// targetEncoded is the target size for encoded batches to reduce round-trips.
	targetEncoded int
}

// Large buffer pool reused across readers.
var bufferPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, defaultBufSize)
	},
}

// NewDirReaderUnix opens a directory for reading.
// Uses unix.Open to obtain a raw fd with directory-safe flags.
func NewDirReaderUnix(path string) (*DirReaderUnix, error) {
	// Explicit directory flags improve safety on Linux and are fine elsewhere via x/sys/unix.
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open directory '%s': %w", path, err)
	}
	return &DirReaderUnix{
		fd:            fd,
		buf:           bufferPool.Get().([]byte),
		path:          path,
		basep:         0,
		targetEncoded: defaultTargetEncodedLen,
	}, nil
}

// NextBatch returns the next encoded batch of directory entries.
// - Retries EINTR/EAGAIN around getdents/getdirentries
// - Accumulates until reaching targetEncoded or buffer exhaustion
// - Encodes directly while parsing to avoid large intermediate allocations
func (r *DirReaderUnix) NextBatch() ([]byte, error) {
	if r.noMoreFiles {
		return nil, os.ErrProcessDone
	}

	entries := make(types.ReadDirEntries, 0, 512)

	target := r.targetEncoded
	if target <= 0 {
		target = defaultTargetEncodedLen
	}

	encodedSizeEstimate := 4 // u32 count header

	for {
		// Refill buffer if consumed
		if r.bufPos >= r.bufEnd {
			n, err := r.getdents()
			if err != nil {
				return nil, fmt.Errorf("getdents failed: %w", err)
			}
			if n == 0 {
				r.noMoreFiles = true
				// Reached EOF from kernel
				break
			}
			r.bufPos = 0
			r.bufEnd = n
		}

		// Parse entries from current buffer window
		for r.bufPos < r.bufEnd {
			nameBytes, typ, reclen, ok, perr := r.parseDirent()
			if perr != nil {
				// Malformed entry - abort for safety
				return nil, fmt.Errorf("failed to parse dirent for '%s': %w", r.path, perr)
			}
			if !ok || reclen == 0 {
				// Not enough data or padding; force a refill next loop
				r.bufPos = r.bufEnd
				break
			}
			r.bufPos += reclen

			// Skip "." and ".." without allocating strings
			if isDot(nameBytes) || isDotDot(nameBytes) {
				continue
			}

			if typ == unix.DT_LNK {
				continue
			}

			if typ == unix.DT_UNKNOWN {
				continue
			}

			// Convert to string only for entries we keep
			name := string(nameBytes)
			mode := uint32(unixTypeToFileMode(typ))
			entries = append(entries, types.AgentFileInfo{
				Name: name,
				Mode: mode,
			})

			// Roughly estimate size added to encoded buffer:
			// entry.Encode() typically: length-prefix + content. Assume ~fixed + name.
			encodedSizeEstimate += 8 + len(name) // coarse but effective

			// If we hit the target, encode and return immediately
			if encodedSizeEstimate >= target {
				enc, err := entries.Encode()
				if err != nil {
					return nil, fmt.Errorf("failed to encode batch for path '%s': %w", r.path, err)
				}
				return enc, nil
			}
		}

		// If we produced any entries in this iteration but haven't met the target,
		// we can return to reduce latency rather than looping for more kernel reads.
		if len(entries) > 0 {
			enc, err := entries.Encode()
			if err != nil {
				return nil, fmt.Errorf("failed to encode batch for path '%s': %w", r.path, err)
			}
			return enc, nil
		}

		// Otherwise, loop back to refill and continue.
	}

	// EOF path: if we have any remaining entries, send them; else signal done.
	if len(entries) == 0 {
		return nil, os.ErrProcessDone
	}

	enc, err := entries.Encode()
	if err != nil {
		return nil, fmt.Errorf("failed to encode batch for path '%s': %w", r.path, err)
	}
	return enc, nil
}

// Close releases the directory fd and buffer.
func (r *DirReaderUnix) Close() error {
	var firstErr error
	if r.fd != 0 {
		if err := unix.Close(r.fd); err != nil && !errors.Is(err, syscall.EBADF) {
			firstErr = fmt.Errorf("failed to close directory '%s': %w", r.path, err)
		}
		r.fd = 0
	}
	if r.buf != nil {
		bufferPool.Put(r.buf)
		r.buf = nil
	}
	return firstErr
}
