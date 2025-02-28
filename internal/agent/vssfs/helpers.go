//go:build windows

package vssfs

import (
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
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
		Mode:    uint32(mode),
		ModTime: modTime,
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
		Mode:    uint32(mode),
		ModTime: modTime,
		IsDir:   isDir,
	}
}

// --- Error Response Helpers ---

func (s *VSSFSServer) respondError(method, drive string, err error) arpc.Response {
	if syslog.L != nil && err != os.ErrNotExist {
		syslog.L.Errorf("%s (%s): %v", method, drive, err)
	}
	// Wrap error and encode it using our new msgpack encoder.
	errorWrap := arpc.WrapErrorBytes(err)
	if errorWrap != nil {
		defer errorWrap.Release()
	}
	return arpc.Response{
		Status: 500,
		Data:   errorWrap.Data,
	}
}

func (s *VSSFSServer) invalidRequest(method, drive string, err error) arpc.Response {
	if syslog.L != nil {
		syslog.L.Errorf("%s (%s): %v", method, drive, err)
	}

	errorWrap := arpc.WrapErrorBytes(os.ErrInvalid)
	if errorWrap != nil {
		defer errorWrap.Release()
	}
	return arpc.Response{
		Status: 400,
		Data:   errorWrap.Data,
	}
}
