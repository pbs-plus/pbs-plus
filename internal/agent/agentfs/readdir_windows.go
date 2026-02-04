//go:build windows

package agentfs

import (
	"errors"
	"fmt"
	"io"
	"os"
	"unsafe"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"golang.org/x/sys/windows"
)

func (r *DirReader) readdir(n int, blockSize uint64) ([]types.AgentFileInfo, error) {
	if r.closed {
		return nil, os.ErrClosed
	}
	if r.noMoreFiles {
		if n > 0 {
			return nil, io.EOF
		}
		return nil, nil
	}

	wantAll := n <= 0
	limit := n
	if wantAll {
		limit = int(^uint(0) >> 1)
	}
	if limit == 0 {
		return []types.AgentFileInfo{}, nil
	}

	h := windows.Handle(r.file.Fd())

	const bufSize = 64 * 1024
	buf := make([]byte, bufSize)

	out := make([]types.AgentFileInfo, 0, min(limit, 128))
	var iosb IoStatusBlock

	for len(out) < limit && !r.noMoreFiles {
		ntErr := ntDirectoryCall(uintptr(h), &iosb, buf, r.winFirstCall)
		r.winFirstCall = false

		if ntErr != nil {
			if errors.Is(ntErr, os.ErrProcessDone) || errors.Is(ntErr, os.ErrExist) {
				r.noMoreFiles = true
				break
			}
			return nil, ntErr
		}

		bufEnd := int(iosb.Information)
		if bufEnd <= 0 {
			r.noMoreFiles = true
			break
		}

		offset := 0
		for offset < bufEnd && len(out) < limit {
			if offset+int(unsafe.Sizeof(FileDirectoryInformation{})) > bufEnd {
				break
			}

			entry := (*FileDirectoryInformation)(unsafe.Pointer(&buf[offset]))

			if entry.FileAttributes&excludedAttrs == 0 {
				nameLen := int(entry.FileNameLength / 2)
				namePtr := unsafe.Pointer(uintptr(unsafe.Pointer(entry)) + unsafe.Offsetof(entry.FileName))

				if uintptr(namePtr)+uintptr(entry.FileNameLength) >
					uintptr(unsafe.Pointer(&buf[0]))+uintptr(len(buf)) {
					return nil, fmt.Errorf("filename data exceeds buffer bounds")
				}

				name := windows.UTF16ToString(unsafe.Slice((*uint16)(namePtr), nameLen))
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

					out = append(out, info)
				}
			}

			if entry.NextEntryOffset == 0 {
				break
			}
			next := offset + int(entry.NextEntryOffset)
			if next <= offset || next > bufEnd {
				return nil, fmt.Errorf("invalid NextEntryOffset: %d from offset %d",
					entry.NextEntryOffset, offset)
			}
			offset = next
		}
	}

	if len(out) == 0 && r.noMoreFiles && n > 0 {
		return nil, io.EOF
	}
	return out, nil
}
