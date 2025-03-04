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
	bufSize    = 64 * 1024 // 64KB initial buffer
	exclusions = windows.FILE_ATTRIBUTE_REPARSE_POINT |
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
	// Convert the directory path to UTF-16
	pDir, err := windows.UTF16PtrFromString(dirPath)
	if err != nil {
		return nil, mapWinError(err)
	}

	// Open the directory handle
	handle, err := windows.CreateFile(
		pDir,
		windows.FILE_LIST_DIRECTORY,
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

	// Fixed-size buffer
	var buffer [bufSize]byte

	infoClass := windows.FileIdBothDirectoryInfo
	usingFull := false // false means we’re using FILE_ID_BOTH_DIR_INFO

	// Process the buffer
	var entries ReadDirEntries

	for {
		err = windows.GetFileInformationByHandleEx(
			handle,
			uint32(infoClass),
			&buffer[0],
			uint32(len(buffer)),
		)
		if err != nil {
			var errno syscall.Errno
			if errors.As(err, &errno) {
				// Fallback to FILE_FULL_DIR_INFO if necessary.
				if errno == windows.ERROR_INVALID_PARAMETER && !usingFull {
					infoClass = windows.FileFullDirectoryInfo
					usingFull = true
					continue
				}
				// No more files? (caller interprets ERROR_NO_MORE_FILES)
				if errno == windows.ERROR_NO_MORE_FILES {
					break
				}
			}
			return nil, mapWinError(err)
		}

		// Inner loop: process every entry in the fetched buffer.
		offset := 0
		for {
			if !usingFull {
				// Use FILE_ID_BOTH_DIR_INFO.
				both := (*FILE_ID_BOTH_DIR_INFO)(unsafe.Pointer(&buffer[offset]))
				nameLen := int(both.FileNameLength) / 2
				if nameLen > 0 {
					filenamePtr := (*uint16)(unsafe.Pointer(&both.FileName[0]))
					nameSlice := unsafe.Slice(filenamePtr, nameLen)
					name := syscall.UTF16ToString(nameSlice)
					if shouldIncludeEntry(name, both.FileAttributes) {
						mode := windowsAttributesToFileMode(both.FileAttributes)
						entries = append(entries, VSSDirEntry{Name: name, Mode: mode})
					}
				}

				if both.NextEntryOffset == 0 {
					break
				}
				offset += int(both.NextEntryOffset)
			} else {
				// Use FILE_FULL_DIR_INFO.
				full := (*FILE_FULL_DIR_INFO)(unsafe.Pointer(&buffer[offset]))
				nameLen := int(full.FileNameLength) / 2
				if nameLen > 0 {
					filenamePtr := (*uint16)(unsafe.Pointer(&full.FileName[0]))
					nameSlice := unsafe.Slice(filenamePtr, nameLen)
					name := syscall.UTF16ToString(nameSlice)
					if shouldIncludeEntry(name, full.FileAttributes) {
						mode := windowsAttributesToFileMode(full.FileAttributes)
						entries = append(entries, VSSDirEntry{Name: name, Mode: mode})
					}
				}

				if full.NextEntryOffset == 0 {
					break
				}
				offset += int(full.NextEntryOffset)
			}
		}
	}

	return entries.MarshalMsg(nil)
}

// shouldIncludeEntry determines whether to include a file entry
func shouldIncludeEntry(name string, attrs uint32) bool {
	if name == "." || name == ".." {
		return false
	}

	const exclusions = windows.FILE_ATTRIBUTE_REPARSE_POINT |
		windows.FILE_ATTRIBUTE_DEVICE |
		windows.FILE_ATTRIBUTE_OFFLINE |
		windows.FILE_ATTRIBUTE_VIRTUAL |
		windows.FILE_ATTRIBUTE_RECALL_ON_OPEN |
		windows.FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS

	return attrs&exclusions == 0
}

// windowsAttributesToFileMode converts Windows file attributes to Go file modes
func windowsAttributesToFileMode(attrs uint32) uint32 {
	// Start with a default permission (non-directory files)
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
