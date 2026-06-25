package agentfs

import (
	"bytes"
	"context"
	"io"
	"os"
	"sync"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/log"
)

const (
	defaultBatchSize = 1024
	defaultBufSize   = 1024 * 1024
)

type DirReader struct {
	file         *os.File
	path         string
	pending      []types.AgentFileInfo
	encodeWriter *bytes.Buffer
	scratch      bytes.Buffer
	winFirstCall bool
	buf          [8192]uint64
	bufp         int
	nbuf         int
	noMoreFiles  bool
	mu           sync.Mutex
	closed       bool
}

func NewDirReader(handle *os.File, path string) (*DirReader, error) {
	log.Debug("newDirReader: initializing directory reader",
		"path", path)

	reader := &DirReader{
		file:         handle,
		pending:      make([]types.AgentFileInfo, 0, defaultBatchSize),
		path:         path,
		winFirstCall: true,
		encodeWriter: bytes.NewBuffer(make([]byte, 0, defaultBufSize)),
	}

	return reader, nil
}

func (r *DirReader) tryEncode(enc *cbor.Encoder, info types.AgentFileInfo) (bool, error) {
	r.scratch.Reset()
	scratchEnc := cbor.NewEncoder(&r.scratch)
	if err := scratchEnc.Encode(info); err != nil {
		return false, err
	}

	if r.encodeWriter.Len()+r.scratch.Len() > defaultBufSize {
		return false, nil
	}

	if err := enc.Encode(info); err != nil {
		return false, err
	}

	return true, nil
}

func (r *DirReader) NextBatch(ctx context.Context, blockSize uint64) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.noMoreFiles && len(r.pending) == 0 {
		log.Debug("dirReader.NextBatch: no more files (cached)",
			"path", r.path)
		return nil, os.ErrProcessDone
	}

	if blockSize == 0 {
		blockSize = 4096
	}

	r.encodeWriter.Reset()

	enc := cbor.NewEncoder(r.encodeWriter)
	if err := enc.StartIndefiniteArray(); err != nil {
		return nil, err
	}

	hasEntries := false
	entryCount := 0

	i := 0
	for i < len(r.pending) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		ok, err := r.tryEncode(enc, r.pending[i])
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}

		hasEntries = true
		entryCount++
		i++
	}

	if i > 0 {
		copy(r.pending, r.pending[i:])
		r.pending = r.pending[:len(r.pending)-i]
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
			log.Error(err, "DirReader.NextBatch: read failed",
				"path", r.path)
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

			ok, err := r.tryEncode(enc, info)
			if err != nil {
				return nil, err
			}
			if !ok {
				r.pending = append(r.pending, entries[i:]...)
				break
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

	result := make([]byte, r.encodeWriter.Len())
	copy(result, r.encodeWriter.Bytes())
	log.Debug("dirReader.NextBatch: batch encoded",

		"pending_count", len(r.pending), "entries_count", entryCount, "bytes", len(result), "path", r.path)

	return result, nil
}

func (r *DirReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}
	log.Debug("dirReader.Close: closing file",
		"path", r.path)

	r.encodeWriter.Reset()
	r.pending = r.pending[:0]
	r.closed = true
	return r.file.Close()
}
