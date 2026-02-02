//go:build windows

package agentfs

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"golang.org/x/sys/windows"
)

func NewDirReader(path string) (*DirReader, error) {
	syslog.L.Debug().WithMessage("NewDirReader: initializing directory reader").
		WithField("path", path).Write()

	extPath := toExtendedLengthPath(path)

	f, err := os.Open(extPath)
	if err != nil {
		syslog.L.Error(err).WithMessage("NewDirReader: failed to open directory").
			WithField("path", path).Write()
		return nil, err
	}

	syslog.L.Debug().WithMessage("NewDirReader: directory opened").
		WithField("path", path).Write()

	return &DirReader{
		file:          f,
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

		entries, err := r.file.Readdir(128)
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

			sys := entry.Sys().(*windows.Win32FileAttributeData)

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
				CreationTime:   filetimeToUnix(sys.CreationTime),
				LastAccessTime: filetimeToUnix(sys.LastAccessTime),
				LastWriteTime:  filetimeToUnix(sys.LastWriteTime),
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

func toExtendedLengthPath(path string) string {
	if strings.HasPrefix(path, `\\?\`) || strings.HasPrefix(path, `\??\`) {
		return path
	}

	absPath, err := filepath.Abs(path)
	if err != nil {
		absPath = path
	}

	if len(absPath) >= 2 && absPath[1] == ':' {
		return `\\?\` + absPath
	}

	if strings.HasPrefix(absPath, `\\`) {
		return `\\?\UNC\` + absPath[2:]
	}

	return `\\?\` + absPath
}
