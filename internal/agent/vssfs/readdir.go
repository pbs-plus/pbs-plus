//go:build windows

package vssfs

import (
	"errors"
	"os"
	"syscall"
	"unsafe"

	"github.com/valyala/bytebufferpool"
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

// skipPathWithAttributes returns true if the file attributes indicate that
// the file should be skipped.
func skipPathWithAttributes(attrs uint32) bool {
	return attrs&(windows.FILE_ATTRIBUTE_REPARSE_POINT|
		windows.FILE_ATTRIBUTE_DEVICE|
		windows.FILE_ATTRIBUTE_OFFLINE|
		windows.FILE_ATTRIBUTE_VIRTUAL|
		windows.FILE_ATTRIBUTE_RECALL_ON_OPEN|
		windows.FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS) != 0
}

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

	const initialBufSize = 64 * 1024
	buf := bytebufferpool.Get()
	defer func() {
		// Ensure the buffer is returned to the pool only if it's not nil
		if buf != nil {
			bytebufferpool.Put(buf)
		}
	}()

	// Ensure the buffer has enough capacity
	if cap(buf.B) < initialBufSize {
		buf.B = make([]byte, initialBufSize)
	} else {
		buf.B = buf.B[:initialBufSize]
	}

	var entries ReadDirEntries
	var usingFull bool
	infoClass := windows.FileIdBothDirectoryInfo

	for {
		err = windows.GetFileInformationByHandleEx(
			handle,
			uint32(infoClass),
			&buf.B[0],
			uint32(buf.Len()),
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
		for offset < buf.Len() {
			var info *FILE_ID_BOTH_DIR_INFO
			if usingFull {
				info = (*FILE_ID_BOTH_DIR_INFO)(unsafe.Pointer(&buf.B[offset]))
			} else {
				info = (*FILE_ID_BOTH_DIR_INFO)(unsafe.Pointer(&buf.B[offset]))
			}

			nameLen := int(info.FileNameLength) / 2
			if nameLen > 0 {
				filenamePtr := (*uint16)(unsafe.Pointer(&info.FileName[0]))
				nameSlice := unsafe.Slice(filenamePtr, nameLen)
				name := syscall.UTF16ToString(nameSlice)

				if name != "." && name != ".." && !skipPathWithAttributes(info.FileAttributes) {
					mode := windowsAttributesToFileMode(info.FileAttributes)
					entries = append(entries, &VSSDirEntry{Name: name, Mode: mode})
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
