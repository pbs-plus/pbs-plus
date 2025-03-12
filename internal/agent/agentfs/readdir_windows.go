//go:build windows

package agentfs

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"unsafe"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent/agentfs/types"
	"golang.org/x/sys/windows"
)

// FILE_ID_BOTH_DIR_INFO corresponds to:
//
//	typedef struct _FILE_ID_BOTH_DIR_INFO {
//	  DWORD         NextEntryOffset;        // 4 bytes
//	  DWORD         FileIndex;              // 4 bytes
//	  LARGE_INTEGER CreationTime;           // 8 bytes
//	  LARGE_INTEGER LastAccessTime;         // 8 bytes
//	  LARGE_INTEGER LastWriteTime;          // 8 bytes
//	  LARGE_INTEGER ChangeTime;             // 8 bytes
//	  LARGE_INTEGER EndOfFile;              // 8 bytes
//	  LARGE_INTEGER AllocationSize;         // 8 bytes
//	  DWORD         FileAttributes;         // 4 bytes
//	  DWORD         FileNameLength;         // 4 bytes
//	  DWORD         EaSize;                 // 4 bytes
//	  CCHAR         ShortNameLength;        // 1 byte
//	  // 3 bytes padding to get to offset 72
//	  WCHAR         ShortName[12];          // 24 bytes (12 WCHAR's)
//	  LARGE_INTEGER FileId;                 // 8 bytes
//	  WCHAR         FileName[1];            // flexible array member
//	} FILE_ID_BOTH_DIR_INFO;
type FILE_ID_BOTH_DIR_INFO struct {
	NextEntryOffset uint32     // 0   4
	FileIndex       uint32     // 4   4
	CreationTime    [8]byte    // 8   8
	LastAccessTime  [8]byte    // 16  8
	LastWriteTime   [8]byte    // 24  8
	ChangeTime      [8]byte    // 32  8
	EndOfFile       [8]byte    // 40  8
	AllocationSize  [8]byte    // 48  8
	FileAttributes  uint32     // 56  4
	FileNameLength  uint32     // 60  4
	EaSize          uint32     // 64  4
	ShortNameLength byte       // 68  1
	_               [3]byte    // 69-71: 3 bytes padding
	ShortName       [12]uint16 // 72  24 bytes (12 * 2)
	FileId          [8]byte    // 96  8 bytes
	// Fixed size total: 104 bytes.
}

// FILE_FULL_DIR_INFO corresponds to:
//
//	typedef struct _FILE_FULL_DIR_INFO {
//	  ULONG         NextEntryOffset;       // 4 bytes
//	  ULONG         FileIndex;             // 4 bytes
//	  LARGE_INTEGER CreationTime;          // 8 bytes
//	  LARGE_INTEGER LastAccessTime;        // 8 bytes
//	  LARGE_INTEGER LastWriteTime;         // 8 bytes
//	  LARGE_INTEGER ChangeTime;            // 8 bytes
//	  LARGE_INTEGER EndOfFile;             // 8 bytes
//	  LARGE_INTEGER AllocationSize;        // 8 bytes
//	  ULONG         FileAttributes;        // 4 bytes
//	  ULONG         FileNameLength;        // 4 bytes
//	  ULONG         EaSize;                // 4 bytes
//	  WCHAR         FileName[1];           // flexible array member
//	} FILE_FULL_DIR_INFO;
type FILE_FULL_DIR_INFO struct {
	NextEntryOffset uint32  // 0   4
	FileIndex       uint32  // 4   4
	CreationTime    [8]byte // 8   8
	LastAccessTime  [8]byte // 16  8
	LastWriteTime   [8]byte // 24  8
	ChangeTime      [8]byte // 32  8
	EndOfFile       [8]byte // 40  8
	AllocationSize  [8]byte // 48  8
	FileAttributes  uint32  // 56  4
	FileNameLength  uint32  // 60  4
	EaSize          uint32  // 64  4
	_               [4]byte // 68-71: 4 bytes padding
	// Fixed size total: 72 bytes.
}

// For FILE_ID_BOTH_DIR_INFO, the filename data begins at offset 104.
func fileNamePtrIdBoth(info *FILE_ID_BOTH_DIR_INFO) *uint16 {
	return (*uint16)(unsafe.Pointer(uintptr(unsafe.Pointer(info)) + 104))
}

// For FILE_FULL_DIR_INFO, the filename data begins at offset 72.
func fileNamePtrFull(info *FILE_FULL_DIR_INFO) *uint16 {
	return (*uint16)(unsafe.Pointer(uintptr(unsafe.Pointer(info)) + 72))
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

// fileIsAvailable checks if the file (or directory) at "dirPath/name" can be opened.
// If isDir is true, FILE_FLAG_BACKUP_SEMANTICS is used.
func fileIsAvailable(dirPath, name string, isDir bool) bool {
	fullPath := filepath.Join(dirPath, name)
	pFullPath, err := windows.UTF16PtrFromString(fullPath)
	if err != nil {
		return false
	}

	var flags uint32 = 0
	if isDir {
		flags = windows.FILE_FLAG_BACKUP_SEMANTICS
	}

	h, err := windows.CreateFile(
		pFullPath,
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		flags,
		0,
	)
	if err != nil {
		return false
	}
	windows.CloseHandle(h)
	return true
}

// readDirBulk opens the directory at dirPath and enumerates its entries using
// GetFileInformationByHandleEx. It first attempts to use the file-ID based
// information class. If that fails with ERROR_INVALID_PARAMETER, it falls
// back to the full-directory information class. The entries that match
// skipPathWithAttributes (and the "." and ".." names) are omitted.
func readDirBulk(dirPath string) ([]byte, error) {
	pDir, err := windows.UTF16PtrFromString(dirPath)
	if err != nil {
		return nil, mapWinError(err, "readDirBulk UTF16PtrFromString")
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
		return nil, mapWinError(err, "readDirBulk CreateFile")
	}
	defer windows.CloseHandle(handle)

	const initialBufSize = 128 * 1024
	// Allocate an initial slice with a capacity of 128 KB.
	buf := make([]byte, initialBufSize)

	var entries types.ReadDirEntries
	usingFull := false
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
			return nil, mapWinError(err, "readDirBulk GetFileInformationByHandleEx")
		}

		// Process entries in the buffer.
		offset := 0
		for offset < len(buf) {
			if usingFull {
				// Use the FILE_FULL_DIR_INFO structure.
				fullInfo := (*FILE_FULL_DIR_INFO)(unsafe.Pointer(&buf[offset]))
				nameLen := int(fullInfo.FileNameLength) / 2
				if nameLen > 0 {
					filenamePtr := fileNamePtrFull(fullInfo)
					nameSlice := unsafe.Slice(filenamePtr, nameLen)
					name := syscall.UTF16ToString(nameSlice)
					if name != "." && name != ".." && fullInfo.FileAttributes&excludedAttrs == 0 {
						isDir := (fullInfo.FileAttributes & windows.FILE_ATTRIBUTE_DIRECTORY) != 0

						if fileIsAvailable(dirPath, name, isDir) {
							mode := windowsAttributesToFileMode(fullInfo.FileAttributes)
							entries = append(entries, types.AgentDirEntry{
								Name: name,
								Mode: mode,
							})
						}
					}
				}
				if fullInfo.NextEntryOffset == 0 {
					break
				}
				offset += int(fullInfo.NextEntryOffset)
			} else {
				// Use the FILE_ID_BOTH_DIR_INFO structure.
				bothInfo := (*FILE_ID_BOTH_DIR_INFO)(unsafe.Pointer(&buf[offset]))
				nameLen := int(bothInfo.FileNameLength) / 2
				if nameLen > 0 {
					filenamePtr := fileNamePtrIdBoth(bothInfo)
					nameSlice := unsafe.Slice(filenamePtr, nameLen)
					name := syscall.UTF16ToString(nameSlice)
					if name != "." && name != ".." && bothInfo.FileAttributes&excludedAttrs == 0 {
						isDir := (bothInfo.FileAttributes & windows.FILE_ATTRIBUTE_DIRECTORY) != 0

						if fileIsAvailable(dirPath, name, isDir) {
							mode := windowsAttributesToFileMode(bothInfo.FileAttributes)
							entries = append(entries, types.AgentDirEntry{
								Name: name,
								Mode: mode,
							})
						}
					}
				}
				if bothInfo.NextEntryOffset == 0 {
					break
				}
				offset += int(bothInfo.NextEntryOffset)
			}
		}
	}

	return entries.Encode()
}
