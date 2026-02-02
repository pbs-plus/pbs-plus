//go:build windows

package agentfs

import (
	"context"
	"io"
	"os"
	"syscall"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
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

			sys := entry.Sys().(*syscall.Win32FileAttributeData)

			if sys.FileAttributes&excludedAttrs != 0 {
				continue
			}

			fileSize := int64(sys.FileSizeHigh)<<32 | int64(sys.FileSizeLow)

			info := types.AgentFileInfo{
				Name:           entry.Name(),
				Mode:           uint32(entry.Mode()),
				IsDir:          entry.IsDir(),
				Size:           fileSize,
				ModTime:        entry.ModTime().UnixNano(),
				CreationTime:   filetimeSyscallToUnix(sys.CreationTime),
				LastAccessTime: filetimeSyscallToUnix(sys.LastAccessTime),
				LastWriteTime:  filetimeSyscallToUnix(sys.LastWriteTime),
				FileAttributes: parseFileAttributes(sys.FileAttributes),
			}

			if !info.IsDir && fileSize > 0 {
				info.Blocks = uint64((fileSize + int64(blockSize) - 1) / int64(blockSize))
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
