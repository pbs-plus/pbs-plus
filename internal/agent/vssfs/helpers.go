//go:build windows

package vssfs

import (
	"io/fs"
	"os"
	"path/filepath"

	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"golang.org/x/sys/windows"
)

const windowsToUnixEpochOffset = 11644473600

func skipPathWithAttributes(attrs uint32) bool {
	return attrs&(windows.FILE_ATTRIBUTE_REPARSE_POINT|
		windows.FILE_ATTRIBUTE_DEVICE|
		windows.FILE_ATTRIBUTE_OFFLINE|
		windows.FILE_ATTRIBUTE_VIRTUAL|
		windows.FILE_ATTRIBUTE_RECALL_ON_OPEN|
		windows.FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS) != 0
}

func (s *VSSFSServer) fileAttributesToMap(info windows.Win32FileAttributeData, path string) map[string]interface{} {
	fileSize := int64(info.FileSizeHigh)<<32 | int64(info.FileSizeLow)
	isDir := info.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0

	var mode fs.FileMode

	// Set base permissions
	if info.FileAttributes&windows.FILE_ATTRIBUTE_READONLY != 0 {
		mode = 0444 // Read-only for everyone
	} else {
		mode = 0666 // Read-write for everyone
	}

	// Add directory flag and execute permissions
	if info.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0 {
		mode |= os.ModeDir | 0111 // Add execute bits for traversal
		// Set directory-specific permissions
		mode = (mode & 0666) | 0111 | os.ModeDir // Final mode: drwxr-xr-x
	}

	// Convert Windows FILETIME to Unix timestamp
	nsec := int64(info.LastWriteTime.HighDateTime)<<32 | int64(info.LastWriteTime.LowDateTime)
	secs := nsec/10000000 - windowsToUnixEpochOffset

	return map[string]interface{}{
		"name":    filepath.Base(path),
		"size":    fileSize,
		"modTime": secs,
		"isDir":   isDir,
		"mode":    mode,
	}
}

func (s *VSSFSServer) findDataToMap(info *windows.Win32finddata) map[string]interface{} {
	name := windows.UTF16ToString(info.FileName[:])
	fileSize := int64(info.FileSizeHigh)<<32 | int64(info.FileSizeLow)
	isDir := info.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0

	var mode fs.FileMode

	// Set base permissions
	if info.FileAttributes&windows.FILE_ATTRIBUTE_READONLY != 0 {
		mode = 0444 // Read-only for everyone
	} else {
		mode = 0666 // Read-write for everyone
	}

	// Add directory flag and execute permissions
	if info.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0 {
		mode |= os.ModeDir | 0111 // Add execute bits for traversal
		// Set directory-specific permissions
		mode = (mode & 0666) | 0111 | os.ModeDir // Final mode: drwxr-xr-x
	}

	// Convert Windows FILETIME to Unix timestamp
	nsec := int64(info.LastWriteTime.HighDateTime)<<32 | int64(info.LastWriteTime.LowDateTime)
	secs := nsec/10000000 - windowsToUnixEpochOffset

	return map[string]interface{}{
		"name":    name,
		"size":    fileSize,
		"modTime": secs,
		"isDir":   isDir,
		"mode":    mode,
	}
}

// mapWindowsErrorToResponse maps Windows error codes to HTTP-like responses
func (s *VSSFSServer) mapWindowsErrorToResponse(req *arpc.Request, err error) arpc.Response {
	switch err {
	case windows.ERROR_FILE_NOT_FOUND, windows.ERROR_PATH_NOT_FOUND:
		return arpc.Response{Status: 404, Message: "file not found"}
	case windows.ERROR_ACCESS_DENIED:
		return arpc.Response{Status: 403, Message: "permission denied"}
	default:
		return s.respondError(req.Method, s.drive, err)
	}
}

// byHandleFileInfoToMap converts ByHandleFileInformation to a map
func (s *VSSFSServer) byHandleFileInfoToMap(info *windows.ByHandleFileInformation, path string) map[string]interface{} {
	fileSize := int64(info.FileSizeHigh)<<32 | int64(info.FileSizeLow)
	isDir := info.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0

	var mode fs.FileMode

	// Set base permissions
	if info.FileAttributes&windows.FILE_ATTRIBUTE_READONLY != 0 {
		mode = 0444 // Read-only for everyone
	} else {
		mode = 0666 // Read-write for everyone
	}

	// Add directory flag and execute permissions
	if info.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0 {
		mode |= os.ModeDir | 0111 // Add execute bits for traversal
		// Set directory-specific permissions
		mode = (mode & 0666) | 0111 | os.ModeDir // Final mode: drwxr-xr-x
	}

	nsec := int64(info.LastWriteTime.HighDateTime)<<32 | int64(info.LastWriteTime.LowDateTime)
	secs := nsec/10000000 - windowsToUnixEpochOffset

	return map[string]interface{}{
		"name":    filepath.Base(path),
		"size":    fileSize,
		"modTime": secs,
		"isDir":   isDir,
		"mode":    mode,
	}
}
