//go:build windows

package vssfs

import (
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

func skipPathWithAttributes(attrs uint32) bool {
	return attrs&(windows.FILE_ATTRIBUTE_REPARSE_POINT|
		windows.FILE_ATTRIBUTE_DEVICE|
		windows.FILE_ATTRIBUTE_OFFLINE|
		windows.FILE_ATTRIBUTE_VIRTUAL|
		windows.FILE_ATTRIBUTE_RECALL_ON_OPEN|
		windows.FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS) != 0
}

// using GetFileInformationByHandleEx, returning only each file’s name and mode.
func (s *VSSFSServer) readDirBulk(dirPath string) (ReadDirEntries, error) {
	// Open the directory with FILE_FLAG_BACKUP_SEMANTICS.
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

	// Allocate a 64kB buffer.
	const bufSize = 64 * 1024
	buf := s.bufferPool.Get(bufSize)
	defer s.bufferPool.Put(buf)

	var entries []*VSSDirEntry

	// The standard library chooses which information class to use based on
	// file system capabilities. For simplicity, we start with the Restart version,
	// then switch to the non‑restart version.
	infoClass := windows.FileIdBothDirectoryRestartInfo

	// bufp tracks the current position in the buffer.
	bufp := 0

	for {
		// If bufp is 0, we need to refill the buffer with new directory entries.
		if bufp == 0 {
			err = windows.GetFileInformationByHandleEx(
				handle,
				uint32(infoClass),
				&buf[0],
				uint32(len(buf)),
			)
			if err != nil {
				// When we've enumerated all files, GetFileInformationByHandleEx
				// returns ERROR_NO_MORE_FILES.
				if err == syscall.ERROR_NO_MORE_FILES {
					break
				}
				return nil, mapWinError(err)
			}
			// On the first call, switch to the non‑restart version.
			if infoClass == windows.FileIdBothDirectoryRestartInfo {
				infoClass = windows.FileIdBothDirectoryInfo
			}
		}

		// Parse the returned buffer.
		offset := bufp
		// Loop until we run out of entries in the current buffer.
		for {
			// Do a safety check.
			if offset > len(buf)-int(unsafe.Sizeof(fileIDBothDirInfo{})) {
				break
			}

			// Cast the buffer pointer to a fileIDBothDirInfo pointer.
			entry := (*fileIDBothDirInfo)(unsafe.Pointer(&buf[offset]))
			// The fixed size of the structure is 84 bytes:
			// 4+4+8+8+8+8+8+8+4+4+4+16 = 84.
			const fixedSize = 84

			// FileNameLength is in bytes; divide by 2 to get the number of UTF-16
			// code units.
			nameLen := int(entry.FileNameLength) / 2
			// Obtain a pointer to the FileName (which is stored immediately after
			// the fixed structure).
			namePtr := (*uint16)(unsafe.Pointer(uintptr(unsafe.Pointer(entry)) + fixedSize))
			// Create a slice containing the UTF-16 code units.
			nameSlice := unsafe.Slice(namePtr, nameLen)
			name := syscall.UTF16ToString(nameSlice)

			// Skip the special entries "." and "..".
			if name != "." && name != ".." && !skipPathWithAttributes(entry.FileAttributes) {
				entries = append(entries, &VSSDirEntry{
					Name: name,
					Mode: entry.FileAttributes,
				})
			}

			// If NextEntryOffset is zero, we've reached the last entry in the buffer.
			if entry.NextEntryOffset == 0 {
				bufp = 0
				break
			}
			// Move to the next entry.
			offset += int(entry.NextEntryOffset)
			bufp = offset
		}
	}

	return entries, nil
}
