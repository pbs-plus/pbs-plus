//go:build windows

package vssfs

import (
	"errors"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

// fileIDBothDirInfo mirrors the fixed portion of the native
// FILE_ID_BOTH_DIR_INFO structure. The file name (a variable‑length
// array of UTF‑16 characters) follows the fixed part in memory.
// (See https://docs.microsoft.com/en-us/windows-hardware/drivers/ddi/ntifs/ns-ntifs-_file_id_both_dir_info)
type fileIDBothDirInfo struct {
	NextEntryOffset uint32
	FileIndex       uint32
	CreationTime    syscall.Filetime
	LastAccessTime  syscall.Filetime
	LastWriteTime   syscall.Filetime
	ChangeTime      syscall.Filetime
	EndOfFile       int64
	AllocationSize  int64
	FileAttributes  uint32
	FileNameLength  uint32 // length in bytes
	EaSize          uint32
	FileId          [16]byte
	// Followed by FileName[1]uint16 (variable length)
}

// fileFullDirInfo mirrors FILE_FULL_DIR_INFO.
// Fixed part size is 64 bytes.
type fileFullDirInfo struct {
	NextEntryOffset uint32
	FileIndex       uint32
	CreationTime    syscall.Filetime
	LastAccessTime  syscall.Filetime
	LastWriteTime   syscall.Filetime
	ChangeTime      syscall.Filetime
	EndOfFile       int64
	AllocationSize  int64
	FileAttributes  uint32
	FileNameLength  uint32 // in bytes
	// Followed by FileName (UTF-16, variable length)
}

func skipPathWithAttributes(attrs uint32) bool {
	return attrs&(windows.FILE_ATTRIBUTE_REPARSE_POINT|
		windows.FILE_ATTRIBUTE_DEVICE|
		windows.FILE_ATTRIBUTE_OFFLINE|
		windows.FILE_ATTRIBUTE_VIRTUAL|
		windows.FILE_ATTRIBUTE_RECALL_ON_OPEN|
		windows.FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS) != 0
}

// readDirBulk opens the directory at dirPath and enumerates its entries
// using GetFileInformationByHandleEx. It first attempts to use the file‑ID
// information class. If that fails with ERROR_INVALID_PARAMETER, it falls
// back to the full-directory information class. The entries are filtered via
// skipPathWithAttributes.
func (s *VSSFSServer) readDirBulk(dirPath string) (ReadDirEntries, error) {
	// Open the directory using FILE_FLAG_BACKUP_SEMANTICS.
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
		windows.FILE_FLAG_BACKUP_SEMANTICS, // Required to open directories.
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

	var usingFull bool // false: using file-ID info; true: using full info.
	var infoClass uint32 = windows.FileIdBothDirectoryRestartInfo

	// Try the first call with file-ID based class.
	err = windows.GetFileInformationByHandleEx(
		handle,
		infoClass,
		&buf[0],
		uint32(len(buf)),
	)
	if err != nil {
		// Use errors.As to check for ERROR_INVALID_PARAMETER.
		var errno syscall.Errno
		if errors.As(err, &errno) && errno == windows.ERROR_INVALID_PARAMETER {
			// Fallback to the full-directory info.
			infoClass = windows.FileFullDirectoryRestartInfo
			usingFull = true
			err = windows.GetFileInformationByHandleEx(
				handle,
				infoClass,
				&buf[0],
				uint32(len(buf)),
			)
		}
	}
	if err != nil {
		// When there are no more files, GetFileInformationByHandleEx returns ERROR_NO_MORE_FILES.
		var errno syscall.Errno
		if errors.As(err, &errno) && errno == windows.ERROR_NO_MORE_FILES {
			return nil, nil
		}
		return nil, mapWinError(err)
	}

	// For subsequent calls use the non-restart version.
	if usingFull {
		infoClass = windows.FileFullDirectoryInfo
	} else {
		infoClass = windows.FileIdBothDirectoryInfo
	}

	var entries ReadDirEntries
	bufp := 0

	for {
		// If the buffer pointer is zero, refill the buffer.
		if bufp == 0 {
			err = windows.GetFileInformationByHandleEx(
				handle,
				infoClass,
				&buf[0],
				uint32(len(buf)),
			)
			if err != nil {
				var errno syscall.Errno
				if errors.As(err, &errno) && errno == windows.ERROR_NO_MORE_FILES {
					break
				}
				return nil, mapWinError(err)
			}
		}

		offset := bufp
		for {
			if usingFull {
				// Make sure there is enough data.
				if offset > len(buf)-int(unsafe.Sizeof(fileFullDirInfo{})) {
					break
				}
				fldi := (*fileFullDirInfo)(unsafe.Pointer(&buf[offset]))
				fixedSize := int(unsafe.Sizeof(fileFullDirInfo{}))
				nameLen := int(fldi.FileNameLength) / 2
				namePtr := (*uint16)(unsafe.Pointer(uintptr(unsafe.Pointer(fldi)) + uintptr(fixedSize)))
				nameSlice := unsafe.Slice(namePtr, nameLen)
				name := syscall.UTF16ToString(nameSlice)

				if name != "." && name != ".." && !skipPathWithAttributes(fldi.FileAttributes) {
					entries = append(entries, &VSSDirEntry{
						Name: name,
						Mode: fldi.FileAttributes,
					})
				}

				if fldi.NextEntryOffset == 0 {
					bufp = 0
					break
				}
				offset += int(fldi.NextEntryOffset)
				bufp = offset
			} else {
				// Using file-ID based info.
				if offset > len(buf)-int(unsafe.Sizeof(fileIDBothDirInfo{})) {
					break
				}
				fid := (*fileIDBothDirInfo)(unsafe.Pointer(&buf[offset]))
				const fixedSize = 84 // fixed size of fileIDBothDirInfo in bytes
				nameLen := int(fid.FileNameLength) / 2
				namePtr := (*uint16)(unsafe.Pointer(uintptr(unsafe.Pointer(fid)) + uintptr(fixedSize)))
				nameSlice := unsafe.Slice(namePtr, nameLen)
				name := syscall.UTF16ToString(nameSlice)

				if name != "." && name != ".." && !skipPathWithAttributes(fid.FileAttributes) {
					entries = append(entries, &VSSDirEntry{
						Name: name,
						Mode: fid.FileAttributes,
					})
				}

				if fid.NextEntryOffset == 0 {
					bufp = 0
					break
				}
				offset += int(fid.NextEntryOffset)
				bufp = offset
			}
		}
	}

	return entries, nil
}
