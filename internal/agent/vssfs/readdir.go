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
func (s *VSSFSServer) readDirBulk(dirPath string) (ReadDirEntries, error) {
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
		windows.FILE_FLAG_BACKUP_SEMANTICS, // needed for directories
		0,
	)
	if err != nil {
		return nil, mapWinError(err)
	}
	defer windows.CloseHandle(handle)

	// Allocate a 64kB buffer.
	const bufSize = 64 * 1024
	buf := s.bufferPool.Get(bufSize)
	defer s.bufferPool.Put(buf)

	var entries ReadDirEntries
	var usingFull bool

	// First try with FileIdBothDirectoryInfo
	infoClass := windows.FileIdBothDirectoryInfo
	err = windows.GetFileInformationByHandleEx(
		handle,
		uint32(infoClass),
		&buf[0],
		uint32(len(buf)),
	)

	// If it fails with ERROR_INVALID_PARAMETER, try with FileFullDirectoryInfo
	if err != nil {
		var errno syscall.Errno
		if errors.As(err, &errno) && errno == windows.ERROR_INVALID_PARAMETER {
			infoClass = windows.FileFullDirectoryInfo
			usingFull = true
			err = windows.GetFileInformationByHandleEx(
				handle,
				uint32(infoClass),
				&buf[0],
				uint32(len(buf)),
			)
		}
	}

	// Check if we have an error (but ERROR_NO_MORE_FILES is not an error here)
	if err != nil {
		var errno syscall.Errno
		if errors.As(err, &errno) && errno == windows.ERROR_NO_MORE_FILES {
			return nil, nil
		}
		return nil, mapWinError(err)
	}

	for {
		// Process the buffer
		offset := 0
		for {
			if usingFull {
				// Make sure we have enough space for at least the fixed part
				if offset+int(unsafe.Sizeof(FILE_FULL_DIR_INFO{})) > len(buf) {
					break
				}

				info := (*FILE_FULL_DIR_INFO)(unsafe.Pointer(&buf[offset]))

				// Extract the filename safely using unsafe.Slice
				nameLen := int(info.FileNameLength) / 2 // Convert bytes to UTF-16 characters
				if nameLen <= 0 {
					// Skip entries with empty names
					if info.NextEntryOffset == 0 {
						break
					}
					offset += int(info.NextEntryOffset)
					continue
				}

				// Use unsafe.Slice to create a properly bounded slice
				filenamePtr := (*uint16)(unsafe.Pointer(&info.FileName[0]))
				nameSlice := unsafe.Slice(filenamePtr, nameLen)
				name := syscall.UTF16ToString(nameSlice)

				// Add entry if it's not "." or ".." and doesn't match the skip attributes
				if name != "." && name != ".." && !skipPathWithAttributes(info.FileAttributes) {
					// Convert Windows attributes to os.FileMode
					mode := windowsAttributesToFileMode(info.FileAttributes)

					entries = append(entries, &VSSDirEntry{
						Name: name,
						Mode: mode,
					})
				}

				// Move to the next entry or exit this buffer
				if info.NextEntryOffset == 0 {
					break
				}
				offset += int(info.NextEntryOffset)
			} else {
				// Make sure we have enough space for at least the fixed part
				if offset+int(unsafe.Sizeof(FILE_ID_BOTH_DIR_INFO{})) > len(buf) {
					break
				}

				info := (*FILE_ID_BOTH_DIR_INFO)(unsafe.Pointer(&buf[offset]))

				// Extract the filename safely using unsafe.Slice
				nameLen := int(info.FileNameLength) / 2 // Convert bytes to UTF-16 characters
				if nameLen <= 0 {
					// Skip entries with empty names
					if info.NextEntryOffset == 0 {
						break
					}
					offset += int(info.NextEntryOffset)
					continue
				}

				// Use unsafe.Slice to create a properly bounded slice
				filenamePtr := (*uint16)(unsafe.Pointer(&info.FileName[0]))
				nameSlice := unsafe.Slice(filenamePtr, nameLen)
				name := syscall.UTF16ToString(nameSlice)

				// Add entry if it's not "." or ".." and doesn't match the skip attributes
				if name != "." && name != ".." && !skipPathWithAttributes(info.FileAttributes) {
					// Convert Windows attributes to os.FileMode
					mode := windowsAttributesToFileMode(info.FileAttributes)

					entries = append(entries, &VSSDirEntry{
						Name: name,
						Mode: mode,
					})
				}

				// Move to the next entry or exit this buffer
				if info.NextEntryOffset == 0 {
					break
				}
				offset += int(info.NextEntryOffset)
			}
		}

		// Try to get the next batch of entries
		err = windows.GetFileInformationByHandleEx(
			handle,
			uint32(infoClass),
			&buf[0],
			uint32(len(buf)),
		)

		// If no more files, we're done
		if err != nil {
			var errno syscall.Errno
			if errors.As(err, &errno) && errno == windows.ERROR_NO_MORE_FILES {
				break
			}
			return nil, mapWinError(err)
		}
	}

	return entries, nil
}
