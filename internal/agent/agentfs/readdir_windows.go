//go:build windows

package agentfs

import (
	"errors"
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

	wantAll := n <= 0
	limit := n
	if wantAll {
		limit = int(^uint(0) >> 1)
	}

	h := windows.Handle(r.file.Fd())
	out := make([]types.AgentFileInfo, 0, min(limit, 128))
	var iosb IoStatusBlock

	fullByteBuf := unsafe.Slice((*byte)(unsafe.Pointer(&r.buf[0])), len(r.buf)*8)

	for len(out) < limit {
		if r.bufp >= r.nbuf {
			r.bufp = 0
			err := ntDirectoryCall(uintptr(h), &iosb, fullByteBuf, r.winFirstCall)
			r.winFirstCall = false

			if err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, os.ErrProcessDone) {
					r.noMoreFiles = true
					break
				}
				return nil, err
			}
			r.nbuf = int(iosb.Information)
			if r.nbuf <= 0 {
				r.noMoreFiles = true
				break
			}
		}

		for r.bufp < r.nbuf && len(out) < limit {
			if r.bufp+int(unsafe.Offsetof(FileDirectoryInformation{}.FileName)) > r.nbuf {
				break
			}

			entry := (*FileDirectoryInformation)(unsafe.Pointer(&fullByteBuf[r.bufp]))

			nameLen := int(entry.FileNameLength / 2)
			namePtr := unsafe.Pointer(uintptr(unsafe.Pointer(entry)) + unsafe.Offsetof(entry.FileName))
			name := windows.UTF16ToString(unsafe.Slice((*uint16)(namePtr), nameLen))

			if name != "." && name != ".." {
				isDir := (entry.FileAttributes & windows.FILE_ATTRIBUTE_DIRECTORY) != 0

				mode := uint32(0644)
				if entry.FileAttributes&windows.FILE_ATTRIBUTE_READONLY != 0 {
					mode = 0444
				}
				if isDir {
					mode |= 0x80000000
				}

				info := types.AgentFileInfo{
					Name:           name,
					Size:           entry.EndOfFile,
					Mode:           mode,
					IsDir:          isDir,
					ModTime:        unixNanoFromWin(entry.LastWriteTime),
					CreationTime:   unixNanoFromWin(entry.CreationTime),
					LastAccessTime: unixNanoFromWin(entry.LastAccessTime),
					LastWriteTime:  unixNanoFromWin(entry.LastWriteTime),
					FileAttributes: parseFileAttributes(entry.FileAttributes),
				}

				if !isDir && blockSize > 0 {
					alloc := entry.AllocationSize
					if alloc < 0 {
						alloc = 0
					}
					info.Blocks = uint64((alloc + int64(blockSize) - 1) / int64(blockSize))
				}
				out = append(out, info)
			}

			if entry.NextEntryOffset == 0 {
				r.bufp = r.nbuf
				break
			}
			r.bufp += int(entry.NextEntryOffset)
		}
	}

	if len(out) == 0 && r.noMoreFiles && n > 0 {
		return nil, io.EOF
	}
	return out, nil
}
