//go:build windows

package agentfs

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"unicode/utf16"
	"unsafe"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
)

// 1 MiB reusable buffer size. Tunable based on directory size / network latency.
const BUF_SIZE = 1024 * 1024

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

	if err := ntCreateFileCall(&handle, &objectAttributes, &ioStatusBlock); err != nil {
		return nil, err
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

	err := ntDirectoryCall(r.handle, &r.ioStatus, buffer, r.restartScan)
	r.restartScan = false
	if err != nil {
		if os.IsExist(err) {
			// PENDING
			return nil, nil
		}
		if errors.Is(err, os.ErrProcessDone) {
			r.noMoreFiles = true
			return nil, err
		}

		return nil, err
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
