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

	for len(out) < limit {
		if r.bufp >= r.nbuf {
			r.bufp = 0
			err := ntDirectoryCall(uintptr(h), &iosb, r.buf[:], r.winFirstCall)
			r.winFirstCall = false

			if err != nil {
				if errors.Is(err, os.ErrProcessDone) || errors.Is(err, io.EOF) {
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

			entry := (*FileDirectoryInformation)(unsafe.Pointer(&r.buf[r.bufp]))

			nameLen := int(entry.FileNameLength / 2)
			namePtr := unsafe.Pointer(uintptr(unsafe.Pointer(entry)) + unsafe.Offsetof(entry.FileName))

			name := windows.UTF16ToString(unsafe.Slice((*uint16)(namePtr), nameLen))

			if name != "." && name != ".." && (entry.FileAttributes&excludedAttrs == 0) {
				isDir := (entry.FileAttributes & windows.FILE_ATTRIBUTE_DIRECTORY) != 0

				mode := windowsFileModeFromHandle(h, entry.FileAttributes)
				info := types.AgentFileInfo{
					Name:           name,
					Mode:           mode,
					IsDir:          isDir,
					Size:           entry.EndOfFile,
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
