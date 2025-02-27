//go:build windows

package vssfs

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/vmihailenco/msgpack/v5"
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

func createFileInfoFromFindData(name string, fd *windows.Win32finddata) *VSSFileInfo {
	var mode fs.FileMode
	var isDir bool

	// Set base permissions
	if fd.FileAttributes&windows.FILE_ATTRIBUTE_READONLY != 0 {
		mode = 0444 // Read-only for everyone
	} else {
		mode = 0666 // Read-write for everyone
	}

	// Add directory flag and execute permissions
	if fd.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0 {
		mode |= os.ModeDir | 0111 // Add execute bits for traversal
		isDir = true
		// Set directory-specific permissions
		mode = (mode & 0666) | 0111 | os.ModeDir // Final mode: drwxr-xr-x
	}

	size := int64(fd.FileSizeHigh)<<32 + int64(fd.FileSizeLow)
	modTime := time.Unix(0, fd.LastWriteTime.Nanoseconds())

	return &VSSFileInfo{
		Name:    name,
		Size:    size,
		Mode:    mode,
		ModTime: modTime.Unix(),
		IsDir:   isDir,
	}
}

func createFileInfoFromHandleInfo(path string, fd *windows.ByHandleFileInformation) *VSSFileInfo {
	var mode fs.FileMode
	var isDir bool

	// Set base permissions
	if fd.FileAttributes&windows.FILE_ATTRIBUTE_READONLY != 0 {
		mode = 0444 // Read-only for everyone
	} else {
		mode = 0666 // Read-write for everyone
	}

	// Add directory flag and execute permissions
	if fd.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0 {
		mode |= os.ModeDir | 0111 // Add execute bits for traversal
		isDir = true
		// Set directory-specific permissions
		mode = (mode & 0666) | 0111 | os.ModeDir // Final mode: drwxr-xr-x
	}

	size := int64(fd.FileSizeHigh)<<32 + int64(fd.FileSizeLow)
	modTime := time.Unix(0, fd.LastWriteTime.Nanoseconds())

	return &VSSFileInfo{
		Name:    filepath.Base(path),
		Size:    size,
		Mode:    mode,
		ModTime: modTime.Unix(),
		IsDir:   isDir,
	}
}

// --- Error Response Helpers ---

func (s *VSSFSServer) respondError(method, drive string, err error) arpc.Response {
	if syslog.L != nil && err != os.ErrNotExist {
		syslog.L.Errorf("%s (%s): %v", method, drive, err)
	}
	// Wrap error and encode it using our new msgpack encoder.
	return arpc.Response{
		Status: 500,
		Data:   encodeValue(arpc.WrapError(err)),
	}
}

func (s *VSSFSServer) invalidRequest(method, drive string, err error) arpc.Response {
	if syslog.L != nil {
		syslog.L.Errorf("%s (%s): %v", method, drive, err)
	}
	return arpc.Response{
		Status: 400,
		Data:   encodeValue(arpc.WrapError(os.ErrInvalid)),
	}
}

// --- Helper: fastmsgpack decoding for request payloads ---

func encodeValue(v interface{}) msgpack.RawMessage {
	b, err := msgpack.Marshal(v)
	if err != nil {
		b, _ = msgpack.Marshal(map[string]string{
			"error": fmt.Sprintf("failed to marshal value: %v", err),
		})
	}
	return b
}

func getStringField(payload map[string]interface{}, field string) (string, error) {
	val, ok := payload[field]
	if !ok {
		return "", fmt.Errorf("missing field: %s", field)
	}
	s, ok := val.(string)
	if !ok {
		return "", fmt.Errorf("field %s is not a string", field)
	}
	return s, nil
}

func getIntField(payload map[string]interface{}, field string) (int, error) {
	val, ok := payload[field]
	if !ok {
		return 0, fmt.Errorf("missing field: %s", field)
	}
	// msgpack numbers are float64.
	f, ok := val.(float64)
	if !ok {
		return 0, fmt.Errorf("field %s is not a number", field)
	}
	return int(f), nil
}

func getInt64Field(payload map[string]interface{}, field string) (int64, error) {
	val, ok := payload[field]
	if !ok {
		return 0, fmt.Errorf("missing field: %s", field)
	}
	f, ok := val.(float64)
	if !ok {
		return 0, fmt.Errorf("field %s is not a number", field)
	}
	return int64(f), nil
}
