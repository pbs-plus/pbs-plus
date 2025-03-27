//go:build windows

package agentfs

import (
	"fmt"
	"strings"
	"sync"
	"syscall"
	"unicode/utf16"
	"unsafe"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
)

const (
	FILE_LIST_DIRECTORY          = 0x0001
	FILE_SHARE_READ              = 0x00000001
	FILE_SHARE_WRITE             = 0x00000002
	FILE_SHARE_DELETE            = 0x00000004
	OPEN_EXISTING                = 3
	FILE_DIRECTORY_FILE          = 0x00000001
	FILE_SYNCHRONOUS_IO_NONALERT = 0x00000020
	OBJ_CASE_INSENSITIVE         = 0x00000040
	STATUS_NO_MORE_FILES         = 0x80000006
	STATUS_SUCCESS               = 0x00000000
)

type UnicodeString struct {
	Length        uint16
	MaximumLength uint16
	Buffer        *uint16
}

type ObjectAttributes struct {
	Length                   uint32
	RootDirectory            uintptr
	ObjectName               *UnicodeString
	Attributes               uint32
	SecurityDescriptor       uintptr
	SecurityQualityOfService uintptr
}

type IoStatusBlock struct {
	Status      int32
	Information uintptr
}

type FileDirectoryInformation struct {
	NextEntryOffset uint32
	FileIndex       uint32
	CreationTime    int64
	LastAccessTime  int64
	LastWriteTime   int64
	ChangeTime      int64
	EndOfFile       int64
	AllocationSize  int64
	FileAttributes  uint32
	FileNameLength  uint32
	FileName        [260]uint16
}

var (
	ntdll                = syscall.NewLazyDLL("ntdll.dll")
	ntCreateFile         = ntdll.NewProc("NtCreateFile")
	ntQueryDirectoryFile = ntdll.NewProc("NtQueryDirectoryFile")
	ntClose              = ntdll.NewProc("NtClose")
	ntWriteFile          = ntdll.NewProc("NtWriteFile")
	rtlInitUnicodeString = ntdll.NewProc("RtlInitUnicodeString")
)

var fileInfoPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 65536)
	},
}

var pathStringPool = sync.Pool{
	New: func() interface{} {
		return make([]uint16, 260)
	},
}

func convertToNTPath(path string) []uint16 {
	// Fast path for already NT-style paths
	if len(path) >= 4 && path[:4] == "\\??\\" {
		return utf16.Encode([]rune(path + "\x00"))
	}

	// Construct NT path
	var ntPath string
	if len(path) >= 2 && path[1] == ':' {
		ntPath = "\\??\\" + path
	} else {
		ntPath = "\\??\\" + path
	}

	if !strings.HasSuffix(ntPath, "\\") {
		ntPath += "\\"
	}

	return utf16.Encode([]rune(ntPath + "\x00"))
}

func readDirNT(path string) ([]byte, error) {
	// Preallocate entries with a reasonable initial capacity
	entries := make(types.ReadDirEntries, 0, 128)

	// Get buffer from pool
	bufInterface := fileInfoPool.Get()
	buffer := bufInterface.([]byte)
	defer fileInfoPool.Put(buffer)

	pathUTF16 := convertToNTPath(path)

	var unicodeString UnicodeString
	rtlInitUnicodeString.Call(
		uintptr(unsafe.Pointer(&unicodeString)),
		uintptr(unsafe.Pointer(&pathUTF16[0])),
	)

	// Setup ObjectAttributes
	var objectAttributes ObjectAttributes
	objectAttributes.Length = uint32(unsafe.Sizeof(objectAttributes))
	objectAttributes.ObjectName = &unicodeString
	objectAttributes.Attributes = OBJ_CASE_INSENSITIVE

	var handle uintptr
	var ioStatusBlock IoStatusBlock

	// Open directory
	status, _, _ := ntCreateFile.Call(
		uintptr(unsafe.Pointer(&handle)),
		FILE_LIST_DIRECTORY|syscall.SYNCHRONIZE,
		uintptr(unsafe.Pointer(&objectAttributes)),
		uintptr(unsafe.Pointer(&ioStatusBlock)),
		0,
		0,
		FILE_SHARE_READ|FILE_SHARE_WRITE|FILE_SHARE_DELETE,
		OPEN_EXISTING,
		FILE_DIRECTORY_FILE|FILE_SYNCHRONOUS_IO_NONALERT,
		0,
		0,
	)

	if status != 0 {
		return nil, fmt.Errorf("NtCreateFile failed with status: %x, path: %s", status, path)
	}
	defer ntClose.Call(handle)

	// First query with restart flag set to true
	restart := true

	for {
		status, _, _ := ntQueryDirectoryFile.Call(
			handle,
			0,
			0,
			0,
			uintptr(unsafe.Pointer(&ioStatusBlock)),
			uintptr(unsafe.Pointer(&buffer[0])),
			uintptr(len(buffer)),
			uintptr(1), // FileBothDirectoryInformation
			uintptr(0), // Return multiple entries
			0,
			uintptr(boolToInt(restart)),
		)

		// After first call, set restart to false for subsequent calls
		restart = false

		if status == STATUS_NO_MORE_FILES {
			break
		}

		if status != 0 {
			return nil, fmt.Errorf("NtQueryDirectoryFile failed with status: %x", status)
		}

		// Process all entries in the buffer
		var offset uint32 = 0
		for {
			fileInfo := (*FileDirectoryInformation)(unsafe.Pointer(&buffer[offset]))

			// Process valid entries
			if fileInfo.FileAttributes&excludedAttrs == 0 {
				// Get exact slice of the filename UTF-16 bytes
				fileNameLen := fileInfo.FileNameLength / 2
				fileNameSlice := fileInfo.FileName[:fileNameLen]

				// Skip . and .. entries
				if fileNameLen == 1 && fileNameSlice[0] == '.' {
					// Skip
				} else if fileNameLen == 2 && fileNameSlice[0] == '.' && fileNameSlice[1] == '.' {
					// Skip
				} else {
					name := string(utf16.Decode(fileNameSlice))
					mode := windowsAttributesToFileMode(fileInfo.FileAttributes)
					entries = append(entries, types.AgentDirEntry{
						Name: name,
						Mode: mode,
					})
				}
			}

			// If NextEntryOffset is 0, we've reached the end of the current batch
			if fileInfo.NextEntryOffset == 0 {
				break
			}

			// Move to next entry
			offset += fileInfo.NextEntryOffset
		}
	}

	return entries.Encode()
}

func boolToInt(b bool) uint32 {
	if b {
		return 1
	}
	return 0
}
