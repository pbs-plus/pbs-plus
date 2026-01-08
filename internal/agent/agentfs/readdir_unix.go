//go:build unix

package agentfs

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"syscall"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/utils/safemap"
	"golang.org/x/sys/unix"
)

const (
	defaultBufSize          = 1024 * 1024 // 1 MiB
	defaultTargetEncodedLen = 1024 * 1024
)

var (
	idCache = safemap.New[uint32, string]()
)

func getIDString(id uint32) string {
	if s, ok := idCache.Get(id); ok {
		return s
	}
	s := strconv.Itoa(int(id))
	idCache.Set(id, s)
	return s
}

type DirReaderUnix struct {
	fd              int
	buf             []byte
	bufPos          int
	bufEnd          int
	noMoreFiles     bool
	path            string
	basep           uintptr // FreeBSD only
	targetEncoded   int
	reusableEntries types.ReadDirEntries

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

func NewDirReaderUnix(fd int, path string) (*DirReaderUnix, error) {
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
	if r.reusableEntries == nil {
		r.reusableEntries = make(types.ReadDirEntries, 0, 512)
	}
	r.reusableEntries = r.reusableEntries[:0]
	target := r.targetEncoded
	if target <= 0 {
		target = defaultTargetEncodedLen
	}
	encodedSizeEstimate := 4
	for {
		if r.bufPos >= r.bufEnd {
			n, err := r.getdents()
			if err != nil || n == 0 {
				r.noMoreFiles = true
				break
			}
			r.bufPos, r.bufEnd = 0, n
		}
		for r.bufPos < r.bufEnd {
			nameBytes, typ, reclen, ok, perr := r.parseDirent()
			if perr != nil {
				return nil, perr
			}
			if !ok || reclen == 0 {
				r.bufPos = r.bufEnd
				break
			}
			r.bufPos += reclen
			if isDot(nameBytes) || isDotDot(nameBytes) || typ == unix.DT_LNK || typ == unix.DT_UNKNOWN {
				continue
			}
			info := types.AgentFileInfo{Name: string(nameBytes)}
			if r.FetchFullAttrs {
				if err := r.fillAttrs(&info); err != nil {
					if err == unix.ENOENT || err == unix.EACCES {
						continue
					}
					return nil, err
				}
			}
			r.reusableEntries = append(r.reusableEntries, info)
			encodedSizeEstimate += 12 + len(info.Name)
			if encodedSizeEstimate >= target {
				return cbor.Marshal(r.reusableEntries)
			}
		}
		if len(r.reusableEntries) > 0 {
			break
		}
	}
	if len(r.reusableEntries) == 0 {
		return nil, os.ErrProcessDone
	}
	return cbor.Marshal(r.reusableEntries)
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
