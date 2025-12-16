//go:build windows

package agentfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"unicode/utf16"
	"unsafe"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"golang.org/x/sys/windows"
)

// 1 MiB reusable buffer size. Tunable based on directory size / network latency.
const BUF_SIZE = 1024 * 1024

var dirBufPoolNT = sync.Pool{
	New: func() any { return make([]byte, BUF_SIZE) },
}

// NewDirReaderNT opens the directory handle and prepares a reusable buffer pool.
func NewDirReaderNT(path string) (*DirReaderNT, error) {
	syslog.L.Debug().WithMessage("NewDirReaderNT: initializing NT directory reader").WithField("path", path).Write()
	ntPath := convertToNTPath(path)
	if !strings.HasSuffix(ntPath, "\\") {
		ntPath += "\\"
	}

	pathUTF16 := utf16.Encode([]rune(ntPath))
	if len(pathUTF16) == 0 || pathUTF16[len(pathUTF16)-1] != 0 {
		pathUTF16 = append(pathUTF16, 0)
	}

	var unicodeString UnicodeString
	rtlInitUnicodeString.Call(
		uintptr(unsafe.Pointer(&unicodeString)),
		uintptr(unsafe.Pointer(&pathUTF16[0])),
	)

	var objectAttributes ObjectAttributes
	objectAttributes.Length = uint32(unsafe.Sizeof(objectAttributes))
	objectAttributes.ObjectName = &unicodeString
	objectAttributes.Attributes = OBJ_CASE_INSENSITIVE

	var handle uintptr
	var ioStatusBlock IoStatusBlock

	if err := ntCreateFileCall(&handle, &objectAttributes, &ioStatusBlock); err != nil {
		syslog.L.Error(err).WithMessage("NewDirReaderNT: ntCreateFileCall failed").WithField("path", path).Write()
		return nil, err
	}

	syslog.L.Debug().WithMessage("NewDirReaderNT: directory handle opened").WithField("path", path).WithField("handle", handle).Write()
	return &DirReaderNT{
		handle:      handle,
		ioStatus:    ioStatusBlock,
		restartScan: true,
		noMoreFiles: false,
		path:        path,
	}, nil
}

// NextBatch retrieves the next batch of directory entries.
// It returns the encoded batch bytes or os.ErrProcessDone when enumeration is finished.
func (r *DirReaderNT) NextBatch(ctx context.Context, blockSize uint64) ([]byte, error) {
	if r.noMoreFiles {
		syslog.L.Debug().WithMessage("DirReaderNT.NextBatch: no more files (cached)").WithField("path", r.path).Write()
		return nil, os.ErrProcessDone
	}

	bufAny := dirBufPoolNT.Get()
	buffer := bufAny.([]byte)
	defer dirBufPoolNT.Put(buffer)

	syslog.L.Debug().WithMessage("DirReaderNT.NextBatch: querying directory batch").
		WithField("path", r.path).
		WithField("restart_scan", r.restartScan).
		WithField("buffer_len", len(buffer)).
		Write()

	err := ntDirectoryCall(ctx, r.handle, &r.ioStatus, buffer, r.restartScan)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			syslog.L.Warn().WithMessage("DirReaderNT.NextBatch: STATUS_PENDING, retry later").WithField("path", r.path).Write()
			return nil, nil
		}
		if errors.Is(err, os.ErrProcessDone) {
			syslog.L.Debug().WithMessage("DirReaderNT.NextBatch: enumeration completed").WithField("path", r.path).Write()
			r.noMoreFiles = true
			return nil, err
		}
		syslog.L.Error(err).WithMessage("DirReaderNT.NextBatch: ntDirectoryCall failed").WithField("path", r.path).Write()
		return nil, err
	}

	r.restartScan = false

	var entries types.ReadDirEntries

	offset := 0
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		if offset+int(unsafe.Sizeof(FileDirectoryInformation{})) > len(buffer) {
			err := fmt.Errorf("offset exceeded buffer length")
			syslog.L.Error(err).WithMessage("DirReaderNT.NextBatch: buffer overflow guard").WithField("path", r.path).Write()
			return nil, err
		}

		entry := (*FileDirectoryInformation)(unsafe.Pointer(&buffer[offset]))

		if entry.FileAttributes&excludedAttrs == 0 {
			fileNameLen := entry.FileNameLength / 2
			fileNamePtr := unsafe.Pointer(
				uintptr(unsafe.Pointer(entry)) + unsafe.Offsetof(entry.FileName),
			)

			if uintptr(fileNamePtr)+uintptr(entry.FileNameLength) >
				uintptr(unsafe.Pointer(&buffer[0]))+uintptr(len(buffer)) {
				err := fmt.Errorf("filename data exceeds buffer bounds")
				syslog.L.Error(err).WithMessage("DirReaderNT.NextBatch: filename bounds check failed").WithField("path", r.path).Write()
				return nil, err
			}

			fileNameSlice := unsafe.Slice((*uint16)(fileNamePtr), fileNameLen)
			name := string(utf16.Decode(fileNameSlice))

			if name != "." && name != ".." {
				mode := windowsFileModeFromHandle(0, entry.FileAttributes)
				isDir := (entry.FileAttributes & windows.FILE_ATTRIBUTE_DIRECTORY) != 0
				size := entry.EndOfFile

				modTime := filetimeToTime(windows.Filetime{
					LowDateTime:  uint32(uint64(entry.LastWriteTime) & 0xFFFFFFFF),
					HighDateTime: uint32(uint64(entry.LastWriteTime) >> 32),
				})

				if blockSize == 0 {
					blockSize = 4096
				}

				var blocks uint64
				if !isDir {
					alloc := entry.AllocationSize
					if alloc < 0 {
						alloc = 0
					}
					blocks = uint64((alloc + int64(blockSize) - 1) / int64(blockSize))
				}

				creationTime := filetimeToUnix(windows.Filetime{
					LowDateTime:  uint32(uint64(entry.CreationTime) & 0xFFFFFFFF),
					HighDateTime: uint32(uint64(entry.CreationTime) >> 32),
				})
				lastAccessTime := filetimeToUnix(windows.Filetime{
					LowDateTime:  uint32(uint64(entry.LastAccessTime) & 0xFFFFFFFF),
					HighDateTime: uint32(uint64(entry.LastAccessTime) >> 32),
				})
				lastWriteTime := filetimeToUnix(windows.Filetime{
					LowDateTime:  uint32(uint64(entry.LastWriteTime) & 0xFFFFFFFF),
					HighDateTime: uint32(uint64(entry.LastWriteTime) >> 32),
				})

				fileAttributes := parseFileAttributes(entry.FileAttributes)

				entries = append(entries, types.AgentFileInfo{
					Name:           name,
					Mode:           mode,
					IsDir:          isDir,
					Size:           size,
					ModTime:        modTime,
					Blocks:         blocks,
					CreationTime:   creationTime,
					LastAccessTime: lastAccessTime,
					LastWriteTime:  lastWriteTime,
					FileAttributes: fileAttributes,
				})
			}
		}

		if entry.NextEntryOffset == 0 {
			break
		}
		nextOffset := offset + int(entry.NextEntryOffset)
		if nextOffset <= offset || nextOffset > len(buffer) {
			err := fmt.Errorf("invalid NextEntryOffset: %d from offset %d", entry.NextEntryOffset, offset)
			syslog.L.Error(err).WithMessage("DirReaderNT.NextBatch: invalid NextEntryOffset").WithField("path", r.path).Write()
			return nil, err
		}
		offset = nextOffset
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	encodedBatch, err := cbor.Marshal(entries)
	if err != nil {
		syslog.L.Error(err).WithMessage("DirReaderNT.NextBatch: encode failed").WithField("path", r.path).Write()
		return nil, fmt.Errorf("failed to encode batch for path '%s': %w", r.path, err)
	}
	syslog.L.Debug().WithMessage("DirReaderNT.NextBatch: batch encoded").
		WithField("path", r.path).
		WithField("bytes", len(encodedBatch)).
		WithField("entries", entries).
		Write()
	return encodedBatch, nil
}

// Close releases the resources used by the directory reader.
func (r *DirReaderNT) Close() error {
	syslog.L.Debug().WithMessage("DirReaderNT.Close: closing handle").WithField("path", r.path).WithField("handle", r.handle).Write()
	status, _, _ := ntClose.Call(r.handle)
	if status != 0 {
		err := fmt.Errorf("NtClose failed for path '%s' with status: 0x%x", r.path, status)
		syslog.L.Error(err).WithMessage("DirReaderNT.Close: NtClose failed").WithField("path", r.path).WithField("status_hex", fmt.Sprintf("0x%X", status)).Write()
		return err
	}
	syslog.L.Debug().WithMessage("DirReaderNT.Close: success").WithField("path", r.path).Write()
	return nil
}
