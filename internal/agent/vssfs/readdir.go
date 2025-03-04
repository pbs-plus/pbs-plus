//go:build windows

package vssfs

import (
	"errors"
	"os"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

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

const (
	excludedAttrs = windows.FILE_ATTRIBUTE_REPARSE_POINT |
		windows.FILE_ATTRIBUTE_DEVICE |
		windows.FILE_ATTRIBUTE_OFFLINE |
		windows.FILE_ATTRIBUTE_VIRTUAL |
		windows.FILE_ATTRIBUTE_RECALL_ON_OPEN |
		windows.FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS
)

// windowsAttributesToFileMode converts Windows file attributes to Go's os.FileMode
func windowsAttributesToFileMode(attrs uint32) uint32 {
	var mode os.FileMode = 0

	// Check for directory
	if attrs&windows.FILE_ATTRIBUTE_DIRECTORY != 0 {
		mode |= os.ModeDir
	}

	// Check for symlink (reparse point)
	if attrs&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		mode |= os.ModeSymlink
	}

	// Check for device file
	if attrs&windows.FILE_ATTRIBUTE_DEVICE != 0 {
		mode |= os.ModeDevice
	}

	// Set regular file permissions (approximation on Windows)
	if mode == 0 {
		// It's a regular file
		mode |= 0644 // Default permission for files
	} else if mode&os.ModeDir != 0 {
		// It's a directory
		mode |= 0755 // Default permission for directories
	}

	return uint32(mode)
}

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

	const initialBufSize = 128 * 1024
	// Allocate an initial slice with a capacity of 128 KB.
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
				// If the buffer is too small, double its size and try again.
				if errno == windows.ERROR_MORE_DATA {
					newSize := len(buf) * 2
					buf = make([]byte, newSize)
					continue
				}
				// Fallback to using the full-directory information class if needed.
				if errno == windows.ERROR_INVALID_PARAMETER && !usingFull {
					infoClass = windows.FileFullDirectoryInfo
					usingFull = true
					continue
				}
				// When there are no more files, break out of the loop.
				if errno == windows.ERROR_NO_MORE_FILES {
					break
				}
			}
			return nil, mapWinError(err)
		}

		// Process entries in the buffer.
		offset := 0
		for offset < len(buf) {
			var info *FILE_ID_BOTH_DIR_INFO
			// We use the same struct for both info classes here.
			info = (*FILE_ID_BOTH_DIR_INFO)(unsafe.Pointer(&buf[offset]))

			nameLen := int(info.FileNameLength) / 2
			if nameLen > 0 {
				filenamePtr := (*uint16)(unsafe.Pointer(&info.FileName[0]))
				nameSlice := unsafe.Slice(filenamePtr, nameLen)
				name := syscall.UTF16ToString(nameSlice)

				if name != "." && name != ".." && info.FileAttributes&excludedAttrs == 0 {
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
