//go:build windows

package vssfs

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"unsafe"

	"golang.org/x/sys/windows"
)

// readFileBackupOptimized opens a backup reading context on the given handle, reads
// the backup header, then uses BackupSeek to efficiently skip 'offset' bytes, and finally
// reads up to 'length' bytes from the file. It returns the read data, an EOF flag, and
// the total size of the primary stream.
func readFileBackupOptimized(f windows.Handle, offset uint64, length int) (data []byte, eof bool, totalSize uint64, err error) {

	var backupCtx uintptr // backup context pointer (initialized to zero)
	// Read the WIN32_STREAM_ID header.
	headerSize := uint32(unsafe.Sizeof(Win32StreamId{}))
	headerBuf := make([]byte, headerSize)
	n, err := backupRead(f, headerBuf, &backupCtx, false, false)
	if err != nil {
		return nil, false, 0, fmt.Errorf("BackupRead header failed: %w", err)
	}
	if n != headerSize {
		return nil, false, 0, errors.New("incomplete WIN32_STREAM_ID header")
	}
	var streamHeader Win32StreamId
	reader := bytes.NewReader(headerBuf)
	if err = binary.Read(reader, binary.LittleEndian, &streamHeader); err != nil {
		return nil, false, 0, fmt.Errorf("failed to decode WIN32_STREAM_ID: %w", err)
	}
	totalSize = streamHeader.Size
	// Read and discard the stream name if present.
	if streamHeader.NameSize > 0 {
		nameBuf := make([]byte, streamHeader.NameSize)
		n, err = backupRead(f, nameBuf, &backupCtx, false, false)
		if err != nil {
			return nil, false, totalSize, fmt.Errorf("BackupRead stream name failed: %w", err)
		}
		if n != streamHeader.NameSize {
			return nil, false, totalSize, errors.New("incomplete stream name read")
		}
	}
	// Instead of copying and discarding bytes, use BackupSeek to skip 'offset' bytes.
	if offset > 0 {
		skipped, err := backupSeek(f, offset, &backupCtx)
		if err != nil {
			// Abort backup context.
			backupRead(f, nil, &backupCtx, true, false)
			return nil, false, totalSize, fmt.Errorf("backupSeek failed: %w", err)
		}
		if skipped < offset {
			backupRead(f, nil, &backupCtx, true, false)
			// If we skip fewer than desired then we are at EOF.
			return nil, true, totalSize, nil
		}
	}
	// Read up to 'length' bytes of file data.
	buf := make([]byte, length)
	n, err = backupRead(f, buf, &backupCtx, false, false)
	if err != nil {
		backupRead(f, nil, &backupCtx, true, false)
		return nil, false, totalSize, fmt.Errorf("BackupRead file data failed: %w", err)
	}
	// Abort the backup context.
	backupRead(f, nil, &backupCtx, true, false)
	eof = (offset + uint64(n)) >= totalSize
	return buf[:n], eof, totalSize, nil
}
