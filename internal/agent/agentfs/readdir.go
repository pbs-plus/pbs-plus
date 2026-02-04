package agentfs

import (
	"bytes"
	"context"
	"io"
	"os"
	"sync"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

const (
	defaultBatchSize = 1024
	defaultBufSize   = 1024 * 1024
)

type DirReader struct {
	file         *os.File
	path         string
	pending      []types.AgentFileInfo
	encodeBuf    [defaultBufSize]byte
	encodeWriter *bytes.Buffer
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
		pending:      make([]types.AgentFileInfo, 0, 8),
		path:         path,
		winFirstCall: true,
	}

	return reader, nil
}

func (r *DirReader) NextBatch(ctx context.Context, blockSize uint64) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.noMoreFiles && len(r.pending) == 0 {
		syslog.L.Debug().WithMessage("DirReader.NextBatch: no more files (cached)").
			WithField("path", r.path).Write()
		return nil, os.ErrProcessDone
	}

	if blockSize == 0 {
		blockSize = 4096
	}

	if r.encodeWriter == nil {
		r.encodeWriter = bytes.NewBuffer(r.encodeBuf[:])
	}

	r.encodeWriter.Reset()

	enc := cbor.NewEncoder(r.encodeWriter)
	if err := enc.StartIndefiniteArray(); err != nil {
		return nil, err
	}

	hasEntries := false
	entryCount := 0

	for len(r.pending) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		info := r.pending[0]

		testBuf := bytes.NewBuffer(nil)
		testEnc := cbor.NewEncoder(testBuf)
		if err := testEnc.Encode(info); err != nil {
			return nil, err
		}

		encodedSize := testBuf.Len()
		if r.encodeWriter.Len()+encodedSize > cap(r.encodeBuf) {
			break
		}

		if err := enc.Encode(info); err != nil {
			syslog.L.Error(err).WithMessage("DirReader.NextBatch: encode failed").
				WithField("path", r.path).Write()
			return nil, err
		}

		r.pending = r.pending[1:]
		hasEntries = true
		entryCount++
	}

	for len(r.pending) == 0 && !r.noMoreFiles {
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

		for i, info := range entries {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if info.Name == "" {
				continue
			}

			testBuf := bytes.NewBuffer(nil)
			testEnc := cbor.NewEncoder(testBuf)
			if err := testEnc.Encode(info); err != nil {
				return nil, err
			}

			encodedSize := testBuf.Len()
			if r.encodeWriter.Len()+encodedSize > cap(r.encodeBuf) {
				r.pending = append(r.pending, entries[i:]...)
				break
			}

			if err := enc.Encode(info); err != nil {
				syslog.L.Error(err).WithMessage("DirReader.NextBatch: encode failed").
					WithField("path", r.path).Write()
				return nil, err
			}
			hasEntries = true
			entryCount++
		}

		if len(r.pending) > 0 {
			break
		}
	}

	if err := enc.EndIndefinite(); err != nil {
		return nil, err
	}

	if !hasEntries && r.noMoreFiles && len(r.pending) == 0 {
		return nil, os.ErrProcessDone
	}

	encodedResult := r.encodeWriter.Bytes()

	syslog.L.Debug().WithMessage("DirReader.NextBatch: batch encoded").
		WithField("path", r.path).
		WithField("bytes", len(encodedResult)).
		WithField("entries_count", entryCount).
		WithField("pending_count", len(r.pending)).
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

	if r.encodeWriter != nil {
		r.encodeWriter.Reset()
	}

	r.closed = true
	return r.file.Close()
}
