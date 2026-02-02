//go:build !windows

package agentfs

import (
	"context"
	"io"
	"os"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"golang.org/x/sys/unix"
)

func NewDirReader(handle *os.File, path string) (*DirReader, error) {
	syslog.L.Debug().WithMessage("NewDirReader: initializing directory reader").
		WithField("path", path).Write()

	return &DirReader{
		file:          handle,
		path:          path,
		targetEncoded: defaultTargetEncodedLen,
	}, nil
}

func (r *DirReader) NextBatch(ctx context.Context, blockSize uint64) ([]byte, error) {
	if r.noMoreFiles {
		syslog.L.Debug().WithMessage("DirReader.NextBatch: no more files").
			WithField("path", r.path).Write()
		return nil, os.ErrProcessDone
	}

	if blockSize == 0 {
		blockSize = 4096
	}

	r.encodeBuf.Reset()
	enc := cbor.NewEncoder(&r.encodeBuf)
	if err := enc.StartIndefiniteArray(); err != nil {
		return nil, err
	}

	hasEntries := false
	entryCount := 0

	for r.encodeBuf.Len() < r.targetEncoded {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		entries, err := r.file.Readdir(defaultBatchSize)
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

		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return nil, err
			}

			info := buildFileInfo(entry, blockSize)

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

	encodedResult := make([]byte, r.encodeBuf.Len())
	copy(encodedResult, r.encodeBuf.Bytes())

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

	r.closed = true
	return r.file.Close()
}

func buildFileInfo(entry os.FileInfo, blockSize uint64) types.AgentFileInfo {
	info := types.AgentFileInfo{
		Name:    entry.Name(),
		Mode:    uint32(entry.Mode()),
		IsDir:   entry.IsDir(),
		Size:    entry.Size(),
		ModTime: entry.ModTime().UnixNano(),
	}

	if stat, ok := entry.Sys().(*unix.Stat_t); ok {
		info.CreationTime = stat.Ctim.Sec
		info.LastAccessTime = stat.Atim.Sec
		info.LastWriteTime = stat.Mtim.Sec

		if !info.IsDir && info.Size > 0 {
			info.Blocks = uint64(stat.Blocks)
		}
	} else {
		modTime := entry.ModTime().Unix()
		info.CreationTime = modTime
		info.LastAccessTime = modTime
		info.LastWriteTime = modTime

		if !info.IsDir && info.Size > 0 {
			info.Blocks = uint64((info.Size + int64(blockSize) - 1) / int64(blockSize))
		}
	}

	info.FileAttributes = make(map[string]bool)

	return info
}
