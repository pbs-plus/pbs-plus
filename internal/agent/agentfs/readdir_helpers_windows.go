package agentfs

import (
	"os"
	"unsafe"

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
	FILE_ATTRIBUTE_UNPINNED = 0x00100000
	FILE_ATTRIBUTE_PINNED   = 0x00080000
)

const (
	excludedAttrs = windows.FILE_ATTRIBUTE_REPARSE_POINT |
		windows.FILE_ATTRIBUTE_DEVICE |
		windows.FILE_ATTRIBUTE_OFFLINE |
		windows.FILE_ATTRIBUTE_VIRTUAL |
		windows.FILE_ATTRIBUTE_RECALL_ON_OPEN |
		windows.FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS |
		windows.FILE_ATTRIBUTE_ENCRYPTED |
		FILE_ATTRIBUTE_UNPINNED | FILE_ATTRIBUTE_PINNED
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
