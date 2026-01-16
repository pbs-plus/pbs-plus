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

const BUF_SIZE = 1024 * 1024

var dirBufPoolNT = sync.Pool{
	New: func() any { return make([]byte, BUF_SIZE) },
}

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

func (r *DirReaderNT) NextBatch(ctx context.Context, blockSize uint64) ([]byte, error) {
	if r.noMoreFiles {
		syslog.L.Debug().WithMessage("DirReaderNT.NextBatch: no more files (cached)").
			WithField("path", r.path).Write()
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

	err := ntDirectoryCall(r.handle, &r.ioStatus, buffer, r.restartScan)
	r.restartScan = false
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, nil
		}
		if errors.Is(err, os.ErrProcessDone) {
			syslog.L.Debug().WithMessage("DirReaderNT.NextBatch: enumeration completed").
				WithField("path", r.path).Write()
			r.noMoreFiles = true
			return nil, err
		}
		syslog.L.Error(err).WithMessage("DirReaderNT.NextBatch: ntDirectoryCall failed").
			WithField("path", r.path).Write()
		return nil, err
	}

	r.encodeBuf.Reset()
	enc := cbor.NewEncoder(&r.encodeBuf)
	if err := enc.StartIndefiniteArray(); err != nil {
		return nil, err
	}

	hasEntries := false
	entryCount := 0
	offset := 0
	if blockSize == 0 {
		blockSize = 4096
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		if offset+int(unsafe.Sizeof(FileDirectoryInformation{})) > len(buffer) {
			return nil, fmt.Errorf("offset exceeded buffer length")
		}

		entry := (*FileDirectoryInformation)(unsafe.Pointer(&buffer[offset]))

		if entry.FileAttributes&excludedAttrs == 0 {
			fileNameLen := entry.FileNameLength / 2
			fileNamePtr := unsafe.Pointer(uintptr(unsafe.Pointer(entry)) + unsafe.Offsetof(entry.FileName))

			if uintptr(fileNamePtr)+uintptr(entry.FileNameLength) >
				uintptr(unsafe.Pointer(&buffer[0]))+uintptr(len(buffer)) {
				return nil, fmt.Errorf("filename data exceeds buffer bounds")
			}

			fileNameSlice := unsafe.Slice((*uint16)(fileNamePtr), fileNameLen)
			name := string(utf16.Decode(fileNameSlice))

			if name != "." && name != ".." {
				info := types.AgentFileInfo{
					Name:           name,
					Mode:           windowsFileModeFromHandle(0, entry.FileAttributes),
					IsDir:          (entry.FileAttributes & windows.FILE_ATTRIBUTE_DIRECTORY) != 0,
					Size:           entry.EndOfFile,
					ModTime:        unixNanoFromWin(entry.LastWriteTime),
					CreationTime:   unixFromWin(entry.CreationTime),
					LastAccessTime: unixFromWin(entry.LastAccessTime),
					LastWriteTime:  unixFromWin(entry.LastWriteTime),
					FileAttributes: parseFileAttributes(entry.FileAttributes),
				}

				if !info.IsDir {
					alloc := entry.AllocationSize
					if alloc < 0 {
						alloc = 0
					}
					info.Blocks = uint64((alloc + int64(blockSize) - 1) / int64(blockSize))
				}

				if err := enc.Encode(info); err != nil {
					syslog.L.Error(err).WithMessage("DirReaderNT.NextBatch: encode failed").
						WithField("path", r.path).Write()
					return nil, err
				}
				hasEntries = true
				entryCount++
			}
		}

		if entry.NextEntryOffset == 0 {
			break
		}
		nextOffset := offset + int(entry.NextEntryOffset)
		if nextOffset <= offset || nextOffset > len(buffer) {
			return nil, fmt.Errorf("invalid NextEntryOffset: %d from offset %d",
				entry.NextEntryOffset, offset)
		}
		offset = nextOffset
	}

	if !hasEntries {
		return nil, os.ErrProcessDone
	}

	if err := enc.EndIndefinite(); err != nil {
		return nil, err
	}

	encodedResult := r.encodeBuf.Bytes()

	syslog.L.Debug().WithMessage("DirReaderNT.NextBatch: batch encoded").
		WithField("path", r.path).
		WithField("bytes", len(encodedResult)).
		WithField("entries_count", entryCount).
		Write()

	return encodedResult, nil
}

func (r *DirReaderNT) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}

	syslog.L.Debug().WithMessage("DirReaderNT.Close: closing handle").
		WithField("path", r.path).WithField("handle", r.handle).Write()

	status, _, _ := ntClose.Call(r.handle)
	r.closed = true

	if status != 0 {
		err := fmt.Errorf("NtClose failed for path '%s' with status: 0x%x", r.path, status)
		syslog.L.Error(err).WithMessage("DirReaderNT.Close: NtClose failed").
			WithField("path", r.path).WithField("status_hex", fmt.Sprintf("0x%X", status)).Write()
		return err
	}

	syslog.L.Debug().WithMessage("DirReaderNT.Close: success").WithField("path", r.path).Write()
	return nil
}
