//go:build windows

package agentfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"unicode/utf16"
	"unsafe"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"golang.org/x/sys/windows"
)

func NewDirReaderNT(path string) (*DirReaderNT, error) {
	syslog.L.Debug().WithMessage("NewDirReaderNT: initializing NT directory reader").WithField("path", path).Write()
	ntPath := convertToNTPath(path)
	if !strings.HasSuffix(ntPath, "\\") {
		ntPath += "\\"
	}

	pathUTF16, err := windows.UTF16PtrFromString(ntPath)
	if err != nil {
		return nil, err
	}

	var unicodeString UnicodeString
	rtlInitUnicodeString.Call(
		uintptr(unsafe.Pointer(&unicodeString)),
		uintptr(unsafe.Pointer(pathUTF16)),
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
		handle:        handle,
		ioStatus:      ioStatusBlock,
		restartScan:   true,
		noMoreFiles:   false,
		path:          path,
		buf:           bufferPool.Get().([]byte),
		bufPos:        0,
		bufEnd:        0,
		targetEncoded: defaultTargetEncodedLen,
	}, nil
}

func (r *DirReaderNT) NextBatch(ctx context.Context, blockSize uint64) ([]byte, error) {
	if r.noMoreFiles {
		syslog.L.Debug().WithMessage("DirReaderNT.NextBatch: no more files (cached)").
			WithField("path", r.path).Write()
		return nil, os.ErrProcessDone
	}

	r.encodeBuf.Reset()
	enc := cbor.NewEncoder(&r.encodeBuf)
	if err := enc.StartIndefiniteArray(); err != nil {
		return nil, err
	}

	if blockSize == 0 {
		blockSize = 4096
	}

	hasEntries := false
	batchFull := false

	for !batchFull {
		if r.bufPos >= r.bufEnd {
			syslog.L.Debug().WithMessage("DirReaderNT.NextBatch: querying directory batch").
				WithField("path", r.path).
				WithField("restart_scan", r.restartScan).
				WithField("buffer_len", len(r.buf)).
				Write()

			ntErr := ntDirectoryCall(r.handle, &r.ioStatus, r.buf, r.restartScan)
			r.restartScan = false

			if ntErr != nil {
				if errors.Is(ntErr, os.ErrExist) {
					r.noMoreFiles = true
					break
				}
				if errors.Is(ntErr, os.ErrProcessDone) {
					syslog.L.Debug().WithMessage("DirReaderNT.NextBatch: enumeration completed").
						WithField("path", r.path).Write()
					r.noMoreFiles = true
					break
				}
				syslog.L.Error(ntErr).WithMessage("DirReaderNT.NextBatch: ntDirectoryCall failed").
					WithField("path", r.path).Write()
				return nil, ntErr
			}

			r.bufPos = 0
			r.bufEnd = len(r.buf)
		}

		for r.bufPos < r.bufEnd {
			if err := ctx.Err(); err != nil {
				return nil, err
			}

			if r.bufPos+int(unsafe.Sizeof(FileDirectoryInformation{})) > r.bufEnd {
				r.bufPos = r.bufEnd
				break
			}

			entry := (*FileDirectoryInformation)(unsafe.Pointer(&r.buf[r.bufPos]))

			if entry.FileAttributes&excludedAttrs == 0 {
				fileNameLen := entry.FileNameLength / 2
				fileNamePtr := unsafe.Pointer(uintptr(unsafe.Pointer(entry)) + unsafe.Offsetof(entry.FileName))

				if uintptr(fileNamePtr)+uintptr(entry.FileNameLength) >
					uintptr(unsafe.Pointer(&r.buf[0]))+uintptr(len(r.buf)) {
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

					if r.encodeBuf.Len() >= r.targetEncoded {
						batchFull = true
					}
				}
			}

			if entry.NextEntryOffset == 0 {
				r.bufPos = r.bufEnd
				break
			}

			nextPos := r.bufPos + int(entry.NextEntryOffset)
			if nextPos <= r.bufPos || nextPos > r.bufEnd {
				return nil, fmt.Errorf("invalid NextEntryOffset: %d from offset %d",
					entry.NextEntryOffset, r.bufPos)
			}
			r.bufPos = nextPos

			if batchFull {
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

	encodedResult := r.encodeBuf.Bytes()

	entryCount := 0
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
	if r.buf != nil {
		readBufPool.Put(r.buf)
		r.buf = nil
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
