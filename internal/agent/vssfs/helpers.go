//go:build windows

package vssfs

import (
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

	// Convert Windows FILETIME to Unix timestamp
	nsec := int64(info.LastWriteTime.HighDateTime)<<32 | int64(info.LastWriteTime.LowDateTime)
	secs := nsec/10000000 - windowsToUnixEpochOffset

	return map[string]interface{}{
		"name":     filepath.Base(path),
		"size":     fileSize,
		"modTime":  secs,
		"isDir":    isDir,
		"readonly": (info.FileAttributes & windows.FILE_ATTRIBUTE_READONLY) != 0,
	}
}

func (s *VSSFSServer) findDataToMap(info *windows.Win32finddata) map[string]interface{} {
	name := windows.UTF16ToString(info.FileName[:])
	fileSize := int64(info.FileSizeHigh)<<32 | int64(info.FileSizeLow)
	isDir := info.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0

	// Convert Windows FILETIME to Unix timestamp
	nsec := int64(info.LastWriteTime.HighDateTime)<<32 | int64(info.LastWriteTime.LowDateTime)
	secs := nsec/10000000 - windowsToUnixEpochOffset

	return map[string]interface{}{
		"name":    name,
		"size":    fileSize,
		"modTime": secs,
		"isDir":   isDir,
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

	// Convert Windows FILETIME to Unix timestamp
	nsec := int64(info.LastWriteTime.HighDateTime)<<32 | int64(info.LastWriteTime.LowDateTime)
	secs := nsec/10000000 - windowsToUnixEpochOffset

	return map[string]interface{}{
		"name":     filepath.Base(path),
		"size":     fileSize,
		"modTime":  secs,
		"isDir":    isDir,
		"readonly": (info.FileAttributes & windows.FILE_ATTRIBUTE_READONLY) != 0,
		"system":   (info.FileAttributes & windows.FILE_ATTRIBUTE_SYSTEM) != 0,
		"hidden":   (info.FileAttributes & windows.FILE_ATTRIBUTE_HIDDEN) != 0,
	}
}
