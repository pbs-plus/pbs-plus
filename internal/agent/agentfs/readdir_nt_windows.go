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
	STATUS_BUFFER_OVERFLOW       = 0x80000005

	FileDirectoryInformationClass = 1
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
	FileName        [1]uint16
}

var fileDirInfoBaseSize = unsafe.Offsetof(FileDirectoryInformation{}.FileName)

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
	entries := make(types.ReadDirEntries, 0, 128) // Initial capacity

	bufInterface := fileInfoPool.Get()
	buffer := bufInterface.([]byte)
	defer fileInfoPool.Put(buffer) // Return buffer to pool when done

	pathUTF16 := convertToNTPath(path)

	var unicodeString UnicodeString
	unicodeString.Buffer = &pathUTF16[0]
	unicodeString.Length = uint16((len(pathUTF16) - 1) * 2) // Length in bytes, excluding null terminator
	unicodeString.MaximumLength = uint16(len(pathUTF16) * 2)

	var objectAttributes ObjectAttributes
	objectAttributes.Length = uint32(unsafe.Sizeof(objectAttributes))
	objectAttributes.ObjectName = &unicodeString
	objectAttributes.Attributes = OBJ_CASE_INSENSITIVE

	var handle syscall.Handle // Use syscall.Handle for type safety
	var ioStatusBlock IoStatusBlock

	status, _, err := ntCreateFile.Call(
		uintptr(unsafe.Pointer(&handle)),
		FILE_LIST_DIRECTORY|syscall.SYNCHRONIZE, // Desired access
		uintptr(unsafe.Pointer(&objectAttributes)),
		uintptr(unsafe.Pointer(&ioStatusBlock)),
		0, // AllocationSize (not used for dirs)
		0, // FileAttributes (not used for opening)
		FILE_SHARE_READ|FILE_SHARE_WRITE|FILE_SHARE_DELETE, // Share mode
		OPEN_EXISTING, // CreationDisposition
		FILE_DIRECTORY_FILE|FILE_SYNCHRONOUS_IO_NONALERT, // CreateOptions
		0, // EaBuffer (optional)
		0, // EaLength (optional)
	)
	if status != STATUS_SUCCESS {
		if err != nil && err != syscall.Errno(0) {
			return nil, fmt.Errorf("NtCreateFile failed for path '%s': %w", path, err)
		}
		return nil, fmt.Errorf("NtCreateFile failed for path '%s' with status: 0x%X", path, status)
	}
	defer ntClose.Call(uintptr(handle)) // Close handle when function exits

	restart := true // Start scan from the beginning

	for {
		ioStatusBlock = IoStatusBlock{}

		status, _, err := ntQueryDirectoryFile.Call(
			uintptr(handle),
			0, // Event handle (optional, for async)
			0, // ApcRoutine (optional, for async)
			0, // ApcContext (optional, for async)
			uintptr(unsafe.Pointer(&ioStatusBlock)),
			uintptr(unsafe.Pointer(&buffer[0])), // Pointer to buffer
			uintptr(len(buffer)),                // Buffer length
			uintptr(3),                          // FileInformationClass: FileIdBothDirectoryInformation (3) is often preferred
			uintptr(0),                          // ReturnSingleEntry: 0 for multiple entries
			0,                                   // FileName (optional filter, requires specific FileInformationClass)
			uintptr(boolToInt(restart)),         // RestartScan
		)

		restart = false // Subsequent calls continue the scan

		if status == STATUS_NO_MORE_FILES {
			break // End of directory listing
		}

		if status != STATUS_SUCCESS {
			if err != nil && err != syscall.Errno(0) {
				return nil, fmt.Errorf("NtQueryDirectoryFile failed for path '%s': %w", path, err)
			}
			// STATUS_BUFFER_OVERFLOW might occur if a single entry is larger than the buffer
			if status == STATUS_BUFFER_OVERFLOW {
				return nil, fmt.Errorf("NtQueryDirectoryFile failed for path '%s': single entry too large for buffer (size %d)", path, len(buffer))
			}
			return nil, fmt.Errorf("NtQueryDirectoryFile failed for path '%s' with status: 0x%X", path, status)
		}

		bytesWritten := ioStatusBlock.Information
		if bytesWritten == 0 {
			// If status is success but no bytes written, it might mean the end.
			// Or it could be an edge case. Breaking seems safest.
			break
		}

		var currentOffset uintptr = 0
		for {
			if currentOffset >= bytesWritten {
				break
			}

			if currentOffset+fileDirInfoBaseSize > bytesWritten {
				return nil, fmt.Errorf("NtQueryDirectoryFile buffer read error: offset %d exceeds bytes written %d", currentOffset, bytesWritten)
			}

			entry := (*FileDirectoryInformation)(unsafe.Pointer(&buffer[currentOffset]))

			fileNameBytes := uintptr(entry.FileNameLength) // Length is in bytes
			recordSize := fileDirInfoBaseSize + fileNameBytes
			if currentOffset+recordSize > bytesWritten {
				return nil, fmt.Errorf("NtQueryDirectoryFile buffer read error: record at offset %d with size %d (base %d + name %d) exceeds bytes written %d for path '%s'", currentOffset, recordSize, fileDirInfoBaseSize, fileNameBytes, bytesWritten, path)
			}

			if entry.FileAttributes&excludedAttrs == 0 {
				fileNameChars := fileNameBytes / unsafe.Sizeof(uint16(0)) // Convert byte length to uint16 count

				// Create a slice pointing directly to the filename in the buffer
				namePtr := unsafe.Pointer(uintptr(unsafe.Pointer(entry)) + fileDirInfoBaseSize)
				nameSlice := unsafe.Slice((*uint16)(namePtr), int(fileNameChars))

				name := string(utf16.Decode(nameSlice))

				// Skip "." and ".." using the decoded string name
				if name != "." && name != ".." {
					// Process entry if attributes don't match exclusion mask
					if entry.FileAttributes&excludedAttrs == 0 {
						mode := windowsAttributesToFileMode(entry.FileAttributes)
						entries = append(entries, types.AgentDirEntry{
							Name: name,
							Mode: mode,
						})
					}
				}
			}

			if entry.NextEntryOffset == 0 {
				break // Last entry in this buffer batch
			}
			currentOffset += uintptr(entry.NextEntryOffset)
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
