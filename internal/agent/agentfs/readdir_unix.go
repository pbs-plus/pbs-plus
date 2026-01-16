//go:build unix

package agentfs

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strconv"
	"syscall"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"golang.org/x/sys/unix"
)

type DirReaderUnix struct {
	fd             int
	buf            []byte
	bufPos         int
	bufEnd         int
	noMoreFiles    bool
	path           string
	basep          uintptr
	targetEncoded  int
	encodeBuf      bytes.Buffer
	idBuf          []byte
	FetchFullAttrs bool
}

func NewDirReaderUnix(fd int, path string) (*DirReaderUnix, error) {
	return &DirReaderUnix{
		fd:             fd,
		buf:            bufferPool.Get().([]byte),
		path:           path,
		basep:          0,
		targetEncoded:  defaultTargetEncodedLen,
		idBuf:          make([]byte, 0, 16),
		FetchFullAttrs: true,
	}, nil
}

func (r *DirReaderUnix) writeIDString(dst *string, id uint32) {
	r.idBuf = strconv.AppendUint(r.idBuf[:0], uint64(id), 10)
	*dst = string(r.idBuf)
}

func (r *DirReaderUnix) NextBatch() ([]byte, error) {
	if r.noMoreFiles {
		return nil, os.ErrProcessDone
	}

	r.encodeBuf.Reset()
	enc := cbor.NewEncoder(&r.encodeBuf)
	if err := enc.StartIndefiniteArray(); err != nil {
		return nil, err
	}

	target := r.targetEncoded
	if target <= 0 {
		target = defaultTargetEncodedLen
	}

	hasEntries := false
	batchFull := false

	for !batchFull {
		if r.bufPos >= r.bufEnd {
			n, err := r.getdents()
			if err != nil || n == 0 {
				r.noMoreFiles = true
				break // Exit outer loop
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
				break // Exit inner loop to fetch more dents
			}
			r.bufPos += reclen

			if isDot(nameBytes) || isDotDot(nameBytes) || typ == unix.DT_LNK || typ == unix.DT_UNKNOWN {
				continue
			}

			info := types.AgentFileInfo{
				Name: bytesToString(nameBytes),
			}

			if r.FetchFullAttrs {
				if err := r.fillAttrs(&info); err != nil {
					if err == unix.ENOENT || err == unix.EACCES {
						continue
					}
					return nil, err
				}
			}

			if err := enc.Encode(info); err != nil {
				return nil, err
			}
			hasEntries = true

			if r.encodeBuf.Len() >= target {
				batchFull = true
				break
			}
		}
	}

	if !hasEntries && r.noMoreFiles {
		return nil, os.ErrProcessDone
	}

	if err := enc.EndIndefinite(); err != nil {
		return nil, err
	}

	return r.encodeBuf.Bytes(), nil
}

func (r *DirReaderUnix) Close() error {
	var firstErr error
	if r.fd != -1 {
		if err := unix.Close(r.fd); err != nil && !errors.Is(err, syscall.EBADF) {
			firstErr = fmt.Errorf("failed to close directory '%s': %w", r.path, err)
		}
		r.fd = -1
	}
	if r.buf != nil {
		bufferPool.Put(r.buf)
		r.buf = nil
	}
	return firstErr
}
