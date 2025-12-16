//go:build unix

package agentfs

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"golang.org/x/sys/unix"
)

const (
	defaultBufSize          = 1024 * 1024 // 1 MiB
	defaultTargetEncodedLen = 1024 * 1024
)

type DirReaderUnix struct {
	fd            int
	buf           []byte
	bufPos        int
	bufEnd        int
	noMoreFiles   bool
	path          string
	basep         uintptr // FreeBSD only
	targetEncoded int

	// Controls whether to fetch full attrs (size, mtime, blocks) for each entry.
	// If false, only Name, Mode, and IsDir are filled (IsDir inferred from d_type
	// or via a selective stat/fstatat only for DT_UNKNOWN).
	FetchFullAttrs bool
}

var bufferPool = sync.Pool{
	New: func() any {
		return make([]byte, defaultBufSize)
	},
}

func NewDirReaderUnix(path string) (*DirReaderUnix, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open directory '%s': %w", path, err)
	}
	return &DirReaderUnix{
		fd:             fd,
		buf:            bufferPool.Get().([]byte),
		path:           path,
		basep:          0,
		targetEncoded:  defaultTargetEncodedLen,
		FetchFullAttrs: true,
	}, nil
}

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

		for r.bufPos < r.bufEnd {
			nameBytes, typ, reclen, ok, perr := r.parseDirent()
			if perr != nil {
				return nil, fmt.Errorf("failed to parse dirent for '%s': %w", r.path, perr)
			}
			if !ok || reclen == 0 {
				r.bufPos = r.bufEnd
				break
			}
			r.bufPos += reclen

			if isDot(nameBytes) || isDotDot(nameBytes) {
				continue
			}

			if typ == unix.DT_LNK {
				continue
			}

			if typ == unix.DT_UNKNOWN {
				continue
			}

			name := string(nameBytes)

			info := types.AgentFileInfo{
				Name: name,
			}

			// If the caller wants full attrs, or we could not trust d_type, fetch attrs.
			if r.FetchFullAttrs {
				if err := r.fillAttrs(&info); err != nil {
					// If file vanished or permission denied, skip cleanly rather than failing the whole batch.
					if err == unix.ENOENT || err == unix.EACCES || err == unix.EPERM {
						continue
					}
					return nil, fmt.Errorf("stat attrs failed for '%s/%s': %w", r.path, name, err)
				}
			}

			entries = append(entries, info)

			encodedSizeEstimate += 8 + len(name)
			if encodedSizeEstimate >= target {
				enc, err := cbor.Marshal(entries)
				if err != nil {
					return nil, fmt.Errorf("failed to encode batch for path '%s': %w", r.path, err)
				}
				return enc, nil
			}
		}

		if len(entries) > 0 {
			enc, err := cbor.Marshal(entries)
			if err != nil {
				return nil, fmt.Errorf("failed to encode batch for path '%s': %w", r.path, err)
			}
			return enc, nil
		}
	}

	if len(entries) == 0 {
		return nil, os.ErrProcessDone
	}

	enc, err := cbor.Marshal(entries)
	if err != nil {
		return nil, fmt.Errorf("failed to encode batch for path '%s': %w", r.path, err)
	}
	return enc, nil
}

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
