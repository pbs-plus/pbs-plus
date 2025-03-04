//go:build windows

package vssfs

import (
	"errors"
	"os"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

// FILE_ID_BOTH_DIR_INFO remains the same as it needs to match Windows API
type FILE_ID_BOTH_DIR_INFO struct {
	NextEntryOffset uint32
	FileIndex       uint32
	CreationTime    syscall.Filetime
	LastAccessTime  syscall.Filetime
	LastWriteTime   syscall.Filetime
	ChangeTime      syscall.Filetime
	EndOfFile       uint64
	AllocationSize  uint64
	FileAttributes  uint32
	FileNameLength  uint32
	EaSize          uint32
	ShortNameLength uint32
	ShortName       [12]uint16
	FileID          uint64
	FileName        [1]uint16
}

// FILE_FULL_DIR_INFO remains the same as it needs to match Windows API
type FILE_FULL_DIR_INFO struct {
	NextEntryOffset uint32
	FileIndex       uint32
	CreationTime    syscall.Filetime
	LastAccessTime  syscall.Filetime
	LastWriteTime   syscall.Filetime
	ChangeTime      syscall.Filetime
	EndOfFile       uint64
	AllocationSize  uint64
	FileAttributes  uint32
	FileNameLength  uint32
	EaSize          uint32
	FileName        [1]uint16
}

// Constants for file attributes checking
const (
	initialBufSize  = 64 * 1024  // 64KB initial buffer
	maxStackBufSize = 128 * 1024 // 128KB maximum buffer size
	exclusions      = windows.FILE_ATTRIBUTE_REPARSE_POINT |
		windows.FILE_ATTRIBUTE_DEVICE |
		windows.FILE_ATTRIBUTE_OFFLINE |
		windows.FILE_ATTRIBUTE_VIRTUAL |
		windows.FILE_ATTRIBUTE_RECALL_ON_OPEN |
		windows.FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS
)

// readDirBulk opens the directory at dirPath and enumerates its entries using
// GetFileInformationByHandleEx. It first attempts to use the file-ID based
// information class; if that fails (with ERROR_INVALID_PARAMETER), it falls
// back to the full-directory information class. The entries that match skipPathWithAttributes
// (and the "." and ".." names) are omitted.
func (s *VSSFSServer) readDirBulk(dirPath string) ([]byte, error) {
	pDir, err := windows.UTF16PtrFromString(dirPath)
	if err != nil {
		return nil, mapWinError(err)
	}

	handle, err := windows.CreateFile(
		pDir,
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS,
		0,
	)
	if err != nil {
		return nil, mapWinError(err)
	}
	defer windows.CloseHandle(handle)

	// Allocate buffer directly without using a pool
	buf := make([]byte, initialBufSize)

	var entries ReadDirEntries
	var usingFull bool
	infoClass := windows.FileIdBothDirectoryInfo

	for {
		err = windows.GetFileInformationByHandleEx(
			handle,
			uint32(infoClass),
			&buf[0],
			uint32(len(buf)),
		)

		if err != nil {
			var errno syscall.Errno
			if errors.As(err, &errno) {
				if errno == windows.ERROR_INVALID_PARAMETER && !usingFull {
					// Switch to FileFullDirectoryInfo
					infoClass = windows.FileFullDirectoryInfo
					usingFull = true
					continue
				}
				if errno == windows.ERROR_NO_MORE_FILES {
					break
				}
			}
			return nil, mapWinError(err)
		}

		// Process entries in the buffer
		offset := 0
		for offset < len(buf) {
			var info *FILE_ID_BOTH_DIR_INFO
			if usingFull {
				info = (*FILE_ID_BOTH_DIR_INFO)(unsafe.Pointer(&buf[offset]))
			} else {
				info = (*FILE_ID_BOTH_DIR_INFO)(unsafe.Pointer(&buf[offset]))
			}

			nameLen := int(info.FileNameLength) / 2
			if nameLen > 0 {
				filenamePtr := (*uint16)(unsafe.Pointer(&info.FileName[0]))
				nameSlice := unsafe.Slice(filenamePtr, nameLen)
				name := syscall.UTF16ToString(nameSlice)

				if shouldIncludeEntry(name, info.FileAttributes) {
					mode := windowsAttributesToFileMode(info.FileAttributes)
					entries = append(entries, VSSDirEntry{Name: name, Mode: mode})
				}
			}

			if info.NextEntryOffset == 0 {
				break
			}
			offset += int(info.NextEntryOffset)
		}
	}

	return entries.MarshalMsg(nil)
}

func shouldIncludeEntry(name string, attrs uint32) bool {
	if name == "." || name == ".." {
		return false
	}

	return attrs&(exclusions) == 0
}

func windowsAttributesToFileMode(attrs uint32) uint32 {
	mode := uint32(0644)
	if attrs&windows.FILE_ATTRIBUTE_DIRECTORY != 0 {
		mode = 0755 | uint32(os.ModeDir)
	}
	if attrs&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		mode |= uint32(os.ModeSymlink)
	}
	if attrs&windows.FILE_ATTRIBUTE_DEVICE != 0 {
		mode |= uint32(os.ModeDevice)
	}
	return mode
}
