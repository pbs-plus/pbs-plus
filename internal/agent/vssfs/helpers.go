//go:build windows

package vssfs

import (
	"fmt"
	"os"
	"time"

	"github.com/Microsoft/go-winio"
	"golang.org/x/sys/windows"
)

func skipPathWithAttributes(attrs uint32) bool {
	return attrs&(windows.FILE_ATTRIBUTE_REPARSE_POINT|
		windows.FILE_ATTRIBUTE_DEVICE|
		windows.FILE_ATTRIBUTE_OFFLINE|
		windows.FILE_ATTRIBUTE_VIRTUAL|
		windows.FILE_ATTRIBUTE_RECALL_ON_OPEN|
		windows.FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS) != 0
}

func mapWinError(err error, path string) error {
	switch err {
	case windows.ERROR_FILE_NOT_FOUND:
		return os.ErrNotExist
	case windows.ERROR_PATH_NOT_FOUND:
		return os.ErrNotExist
	case windows.ERROR_ACCESS_DENIED:
		return os.ErrPermission
	default:
		return &os.PathError{
			Op:   "access",
			Path: path,
			Err:  err,
		}
	}
}

func fileIDToKey(info *winio.FileIDInfo) string {
	return fmt.Sprintf("%d_%x", info.VolumeSerialNumber, info.FileID)
}

func readFileAsync(handle windows.Handle, buf []byte, offset int64, timeout time.Duration) (uint32, error) {
	var overlapped windows.Overlapped
	overlapped.Offset = uint32(offset)
	overlapped.OffsetHigh = uint32(offset >> 32)

	// Create an event for this overlapped operation.
	hEvent, err := windows.CreateEvent(nil, 1, 0, nil)
	if err != nil {
		return 0, fmt.Errorf("CreateEvent error: %w", err)
	}
	defer windows.CloseHandle(hEvent)
	overlapped.HEvent = hEvent

	var bytesRead uint32
	err = windows.ReadFile(handle, buf, &bytesRead, &overlapped)
	if err != nil && err != windows.ERROR_IO_PENDING {
		return 0, fmt.Errorf("ReadFile error: %w", err)
	}

	// Wait for the I/O to complete.
	s, err := windows.WaitForSingleObject(hEvent, uint32(timeout.Milliseconds()))
	if err != nil {
		return 0, fmt.Errorf("WaitForSingleObject error: %w", err)
	}
	if s != windows.WAIT_OBJECT_0 {
		return 0, fmt.Errorf("I/O timeout")
	}

	err = windows.GetOverlappedResult(handle, &overlapped, &bytesRead, false)
	if err != nil {
		return 0, fmt.Errorf("GetOverlappedResult error: %w", err)
	}

	return bytesRead, nil
}
