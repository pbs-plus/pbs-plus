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
	FileName        uint16
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
	}
	return "\\??\\" + path
}

var fileInfoPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 32768)
	},
}

func readDirNT(path string) ([]byte, error) {
	var entries types.ReadDirEntries

	bufInterface := fileInfoPool.Get()
	buffer := bufInterface.([]byte)
	defer fileInfoPool.Put(buffer)

	ntPath := convertToNTPath(path)
	if !strings.HasSuffix(ntPath, "\\") {
		ntPath += "\\"
	}

	pathUTF16 := utf16.Encode([]rune(ntPath))
	if len(pathUTF16) == 0 || pathUTF16[len(pathUTF16)-1] != 0 {
		pathUTF16 = append(pathUTF16, 0)
	}

	var unicodeString UnicodeString
	rtlInitUnicodeString.Call(
		uintptr(unsafe.Pointer(&unicodeString)),
		uintptr(unsafe.Pointer(&pathUTF16[0])),
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
		FILE_SHARE_READ|FILE_SHARE_WRITE,
		OPEN_EXISTING,
		FILE_DIRECTORY_FILE|FILE_SYNCHRONOUS_IO_NONALERT,
		0,
		0,
	)

	if status != 0 {
		return nil, fmt.Errorf(
			"NtCreateFile failed with status: %x, path: %s",
			status,
			ntPath,
		)
	}
	defer ntClose.Call(handle)

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
			uintptr(1), // FileInformationClass: FileDirectoryInformation
			uintptr(0), // ReturnSingleEntry: FALSE
			0,          // FileName: NULL
			uintptr(boolToInt(restart)),
		)

		restart = false

		if status == STATUS_NO_MORE_FILES {
			break
		}

		if status != 0 {
			return nil, fmt.Errorf(
				"NtQueryDirectoryFile failed with status: %x",
				status,
			)
		}

		offset := 0
		for {
			if offset >= len(buffer) {
				return nil, fmt.Errorf("offset exceeded buffer length")
			}

			entry := (*FileDirectoryInformation)(
				unsafe.Pointer(&buffer[offset]),
			)

			if entry.FileAttributes&excludedAttrs == 0 {
				fileNameLen := entry.FileNameLength / 2 // Length in uint16
				fileNamePtr := unsafe.Pointer(
					uintptr(unsafe.Pointer(entry)) +
						unsafe.Offsetof(entry.FileName),
				)

				if uintptr(fileNamePtr)+uintptr(entry.FileNameLength) > uintptr(unsafe.Pointer(&buffer[0]))+uintptr(len(buffer)) {
					return nil, fmt.Errorf("filename data exceeds buffer bounds")
				}

				fileNameSlice := unsafe.Slice(
					(*uint16)(fileNamePtr),
					fileNameLen,
				)
				fileName := utf16.Decode(fileNameSlice)
				name := string(fileName)

				if name != "." && name != ".." {
					mode := windowsAttributesToFileMode(entry.FileAttributes)
					entries = append(entries, types.AgentDirEntry{
						Name: name,
						Mode: mode,
					})
				}
			}

			if entry.NextEntryOffset == 0 {
				break
			}
			nextOffset := offset + int(entry.NextEntryOffset)
			if nextOffset <= offset || nextOffset > len(buffer) {
				return nil, fmt.Errorf(
					"invalid NextEntryOffset: %d from offset %d",
					entry.NextEntryOffset,
					offset,
				)
			}
			offset = nextOffset
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
