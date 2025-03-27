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
	FileBothDirectoryInformation = 3 // Information class level
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

func convertToNTPath(path string) string {
	if len(path) >= 4 && path[:4] == "\\??\\" {
		return path
	}

	if len(path) >= 2 && path[1] == ':' {
		return "\\??\\" + path
	} else {
		return "\\??\\" + path
	}
}

var fileInfoPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 32*1024)
		return &b
	},
}

func readDirNT(path string) ([]byte, error) {
	var entries types.ReadDirEntries

	bufferPtr := fileInfoPool.Get().(*[]byte)
	buffer := *bufferPtr
	defer func() {
		*bufferPtr = buffer
		fileInfoPool.Put(bufferPtr)
	}()

	ntPath := convertToNTPath(path)
	if !strings.HasSuffix(ntPath, "\\") {
		ntPath += "\\"
	}

	pathUTF16, err := syscall.UTF16PtrFromString(ntPath)
	if err != nil {
		return nil, fmt.Errorf("UTF16PtrFromString failed: %w", err)
	}

	var unicodeString UnicodeString
	rtlInitUnicodeString.Call(
		uintptr(unsafe.Pointer(&unicodeString)),
		uintptr(unsafe.Pointer(pathUTF16)),
	)

	var objectAttributes ObjectAttributes
	objectAttributes.Length = uint32(unsafe.Sizeof(objectAttributes))
	objectAttributes.ObjectName = &unicodeString
	objectAttributes.Attributes = OBJ_CASE_INSENSITIVE

	var handle uintptr
	var ioStatusBlock IoStatusBlock

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
		return nil, fmt.Errorf(
			"NtCreateFile failed with status: 0x%x, path: %s",
			uint32(status),
			ntPath,
		)
	}
	defer ntClose.Call(handle)

	restartScan := true

	for {
		status, _, _ := ntQueryDirectoryFile.Call(
			handle,
			0,
			0,
			0,
			uintptr(unsafe.Pointer(&ioStatusBlock)),
			uintptr(unsafe.Pointer(&buffer[0])),
			uintptr(len(buffer)),
			uintptr(FileBothDirectoryInformation),
			uintptr(0),
			0,
			uintptr(boolToInt(restartScan)),
		)

		restartScan = false

		if status == STATUS_NO_MORE_FILES {
			break
		}

		if status != 0 {
			return nil, fmt.Errorf(
				"NtQueryDirectoryFile failed with status: 0x%x",
				uint32(status),
			)
		}

		var currentOffset uint32
		for {
			entryPtr := unsafe.Pointer(&buffer[currentOffset])
			fileInfo := (*FileDirectoryInformation)(entryPtr)

			if fileInfo.FileAttributes&excludedAttrs == 0 {
				fileNameLen := fileInfo.FileNameLength / 2
				fileNameSlice := unsafe.Slice(
					&fileInfo.FileName[0],
					fileNameLen,
				)
				name := string(utf16.Decode(fileNameSlice))

				if name != "." && name != ".." {
					mode := windowsAttributesToFileMode(fileInfo.FileAttributes)
					entries = append(entries, types.AgentDirEntry{
						Name: name,
						Mode: mode,
					})
				}
			}

			if fileInfo.NextEntryOffset == 0 {
				break
			}
			currentOffset += fileInfo.NextEntryOffset
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
