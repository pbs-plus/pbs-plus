//go:build windows

package agentfs

import (
	"fmt"
	"os"
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
	STATUS_PENDING               = 0x00000103
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
	rtlInitUnicodeString = ntdll.NewProc("RtlInitUnicodeString")
)

// Prefer NT path prefix to avoid reparsing and keep long paths working.
func convertToNTPath(path string) string {
	if len(path) >= 4 && path[:4] == "\\??\\" {
		return path
	}
	if len(path) >= 2 && path[1] == ':' {
		return "\\??\\" + path
	}
	return "\\??\\" + path
}

// 1 MiB reusable buffer size. Tunable based on directory size / network latency.
const BUF_SIZE = 1024 * 1024

func boolToInt(b bool) uint32 {
	if b {
		return 1
	}
	return 0
}

type DirReaderNT struct {
	handle      uintptr
	ioStatus    IoStatusBlock
	restartScan bool
	noMoreFiles bool
	path        string
	pool        *sync.Pool
}

// NewDirReaderNT opens the directory handle and prepares a reusable buffer pool.
func NewDirReaderNT(path string) (*DirReaderNT, error) {
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
		FILE_SHARE_READ|FILE_SHARE_WRITE|FILE_SHARE_DELETE,
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

	return &DirReaderNT{
		handle:      handle,
		ioStatus:    ioStatusBlock,
		restartScan: true,
		noMoreFiles: false,
		path:        path,
		pool: &sync.Pool{
			New: func() any { return make([]byte, BUF_SIZE) },
		},
	}, nil
}

// NextBatch retrieves the next batch of directory entries.
// It returns the encoded batch bytes or os.ErrProcessDone when enumeration is finished.
func (r *DirReaderNT) NextBatch() ([]byte, error) {
	if r.noMoreFiles {
		return nil, os.ErrProcessDone
	}

	// Reuse large buffer to avoid per-call allocation and GC churn.
	bufAny := r.pool.Get()
	buffer := bufAny.([]byte)
	defer r.pool.Put(buffer)

	status, _, _ := ntQueryDirectoryFile.Call(
		r.handle,
		0,
		0,
		0,
		uintptr(unsafe.Pointer(&r.ioStatus)),
		uintptr(unsafe.Pointer(&buffer[0])),
		uintptr(len(buffer)),
		uintptr(1), // FileInformationClass: FileDirectoryInformation
		uintptr(0), // ReturnSingleEntry: FALSE (batch)
		0,          // FileName: NULL
		uintptr(boolToInt(r.restartScan)),
	)

	r.restartScan = false

	switch status {
	case 0:
		// success
	case STATUS_NO_MORE_FILES:
		r.noMoreFiles = true
		return nil, os.ErrProcessDone
	case STATUS_PENDING:
		// Since we opened with synchronous I/O, this is unexpected but handle gracefully.
		// Treat as retryable no-op; caller can call NextBatch again.
		return nil, nil
	default:
		return nil, fmt.Errorf("NtQueryDirectoryFile failed with status: %x", status)
	}

	var entries types.ReadDirEntries

	offset := 0
	for {
		if offset+int(unsafe.Sizeof(FileDirectoryInformation{})) > len(buffer) {
			return nil, fmt.Errorf("offset exceeded buffer length")
		}

		entry := (*FileDirectoryInformation)(unsafe.Pointer(&buffer[offset]))

		if entry.FileAttributes&excludedAttrs == 0 {
			fileNameLen := entry.FileNameLength / 2 // length in uint16 code units
			fileNamePtr := unsafe.Pointer(
				uintptr(unsafe.Pointer(entry)) + unsafe.Offsetof(entry.FileName),
			)

			// Bounds check filename region inside the buffer
			if uintptr(fileNamePtr)+uintptr(entry.FileNameLength) >
				uintptr(unsafe.Pointer(&buffer[0]))+uintptr(len(buffer)) {
				return nil, fmt.Errorf("filename data exceeds buffer bounds")
			}

			fileNameSlice := unsafe.Slice((*uint16)(fileNamePtr), fileNameLen)
			name := string(utf16.Decode(fileNameSlice))

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

	encodedBatch, err := entries.Encode()
	if err != nil {
		return nil, fmt.Errorf("failed to encode batch for path '%s': %w", r.path, err)
	}
	return encodedBatch, nil
}

// Close releases the resources used by the directory reader.
func (r *DirReaderNT) Close() error {
	status, _, _ := ntClose.Call(r.handle)
	if status != 0 {
		return fmt.Errorf("NtClose failed for path '%s' with status: 0x%x", r.path, status)
	}
	return nil
}
