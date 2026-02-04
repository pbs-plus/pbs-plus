package agentfs

import (
	"bytes"
	"context"
	"io"
	"os"
	"sync"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

const (
	defaultBatchSize = 1024
	defaultBufSize   = 1024 * 1024
)

var bufferPool = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(make([]byte, 0, defaultBufSize))
	},
}

type DirReader struct {
	file         *os.File
	path         string
	encodeBuf    *bytes.Buffer
	winFirstCall bool
	buf          [8192]uint64 // The raw buffer from the kernel
	bufp         int          // The current position in the buffer
	nbuf         int
	noMoreFiles  bool
	mu           sync.Mutex
	closed       bool
}

func NewDirReader(handle *os.File, path string) (*DirReader, error) {
	syslog.L.Debug().WithMessage("NewDirReader: initializing directory reader").
		WithField("path", path).Write()

	reader := &DirReader{
		file:         handle,
		path:         path,
		winFirstCall: true,
		encodeBuf:    bufferPool.Get().(*bytes.Buffer),
	}

	return reader, nil
}

func (r *DirReader) NextBatch(ctx context.Context, blockSize uint64) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.noMoreFiles {
		syslog.L.Debug().WithMessage("DirReader.NextBatch: no more files (cached)").
			WithField("path", r.path).Write()
		return nil, os.ErrProcessDone
	}

	if blockSize == 0 {
		blockSize = 4096
	}

	r.encodeBuf.Reset()
	enc := cbor.NewEncoder(r.encodeBuf)
	if err := enc.StartIndefiniteArray(); err != nil {
		return nil, err
	}

	hasEntries := false
	entryCount := 0

	for r.encodeBuf.Len() < defaultBufSize-1024 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		entries, err := r.readdir(defaultBatchSize, blockSize)
		if err == io.EOF {
			r.noMoreFiles = true
			break
		}
		if err != nil {
			syslog.L.Error(err).WithMessage("DirReader.NextBatch: read failed").
				WithField("path", r.path).Write()
			return nil, err
		}

		if len(entries) == 0 {
			r.noMoreFiles = true
			break
		}

		for _, info := range entries {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if info.Name == "" {
				continue
			}

			if err := enc.Encode(info); err != nil {
				syslog.L.Error(err).WithMessage("DirReader.NextBatch: encode failed").
					WithField("path", r.path).Write()
				return nil, err
			}
			hasEntries = true
			entryCount++
		}
	}

	if err := enc.EndIndefinite(); err != nil {
		return nil, err
	}

	if !hasEntries && r.noMoreFiles {
		return nil, os.ErrProcessDone
	}

	encodedResult := r.encodeBuf.Bytes()

	syslog.L.Debug().WithMessage("DirReader.NextBatch: batch encoded").
		WithField("path", r.path).
		WithField("bytes", len(encodedResult)).
		WithField("entries_count", entryCount).
		Write()

	return encodedResult, nil
}

func (r *DirReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}

	syslog.L.Debug().WithMessage("DirReader.Close: closing file").
		WithField("path", r.path).Write()

	r.encodeBuf.Reset()
	bufferPool.Put(r.encodeBuf)

	r.closed = true
	return r.file.Close()
}
