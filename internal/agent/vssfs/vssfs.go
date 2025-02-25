//go:build windows

package vssfs

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"time"
	"unsafe"

	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
	"golang.org/x/sys/windows"
)

type FileHandle struct {
	handle   windows.Handle
	path     string
	isDir    bool
	isClosed bool
}

type VSSFSServer struct {
	drive      string
	rootDir    string
	handles    map[uint64]*FileHandle
	nextHandle uint64
	mu         sync.RWMutex
	arpcRouter *arpc.Router
}

func NewVSSFSServer(drive string, root string) *VSSFSServer {
	return &VSSFSServer{
		rootDir: root,
		drive:   drive,
		handles: make(map[uint64]*FileHandle),
	}
}

func (s *VSSFSServer) RegisterHandlers(r *arpc.Router) {
	r.Handle(s.drive+"/OpenFile", s.handleOpenFile)
	r.Handle(s.drive+"/Stat", s.handleStat)
	r.Handle(s.drive+"/ReadDir", s.handleReadDir)
	r.Handle(s.drive+"/Read", s.handleRead)
	r.Handle(s.drive+"/ReadAt", s.handleReadAt)
	r.Handle(s.drive+"/Close", s.handleClose)
	r.Handle(s.drive+"/Fstat", s.handleFstat)
	r.Handle(s.drive+"/FSstat", s.handleFsStat)

	s.arpcRouter = r
}

func (s *VSSFSServer) Close() {
	s.arpcRouter.CloseHandle(s.drive + "/OpenFile")
	s.arpcRouter.CloseHandle(s.drive + "/Stat")
	s.arpcRouter.CloseHandle(s.drive + "/ReadDir")
	s.arpcRouter.CloseHandle(s.drive + "/Read")
	s.arpcRouter.CloseHandle(s.drive + "/ReadAt")
	s.arpcRouter.CloseHandle(s.drive + "/Close")
	s.arpcRouter.CloseHandle(s.drive + "/Fstat")
	s.arpcRouter.CloseHandle(s.drive + "/FSstat")

	for k := range s.handles {
		delete(s.handles, k)
	}
	s.nextHandle = 0
	s.arpcRouter = nil
}

func (s *VSSFSServer) respondError(method, drive string, err error) arpc.Response {
	if syslog.L != nil {
		syslog.L.Errorf("%s (%s): %v", method, drive, err)
	}
	return arpc.Response{Status: 500, Message: fmt.Sprintf("%s (%s): %v", method, drive, err)}
}

func (s *VSSFSServer) invalidRequest(method, drive string, err error) arpc.Response {
	if syslog.L != nil {
		syslog.L.Errorf("%s (%s): %v", method, drive, err)
	}
	return arpc.Response{Status: 400, Message: "invalid request"}
}

func (s *VSSFSServer) handleFsStat(req arpc.Request) (arpc.Response, error) {
	var totalBytes uint64
	err := windows.GetDiskFreeSpaceEx(
		windows.StringToUTF16Ptr(s.rootDir),
		nil,
		&totalBytes,
		nil,
	)
	if err != nil {
		return s.respondError(req.Method, s.drive, err), nil
	}

	fsStat := &utils.FSStat{
		TotalSize:      int64(totalBytes),
		FreeSize:       0,
		AvailableSize:  0,
		TotalFiles:     1 << 20,
		FreeFiles:      0,
		AvailableFiles: 0,
		CacheHint:      time.Minute,
	}
	return arpc.Response{Status: 200, Data: fsStat}, nil
}

func (s *VSSFSServer) handleOpenFile(req arpc.Request) (arpc.Response, error) {
	var params struct {
		Path string `json:"path"`
		Flag int    `json:"flag"`
		Perm int    `json:"perm"`
	}
	if err := json.Unmarshal(req.Payload, &params); err != nil {
		return s.invalidRequest(req.Method, s.drive, err), nil
	}

	// Verify read-only access
	if params.Flag&(0x1|0x2|0x400|0x40|0x200) != 0 { // O_WRONLY|O_RDWR|O_APPEND|O_CREATE|O_TRUNC
		return arpc.Response{Status: 403, Message: "write operations not allowed"}, nil
	}

	fullPath := filepath.Join(s.rootDir, filepath.Clean(params.Path))
	pathPtr, err := windows.UTF16PtrFromString(fullPath)
	if err != nil {
		return s.respondError(req.Method, s.drive, err), nil
	}

	// Open with direct Windows API
	handle, err := windows.CreateFile(
		pathPtr,
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS,
		0,
	)
	if err != nil {
		return s.mapWindowsErrorToResponse(&req, err), nil
	}

	// Get file information to check if it's a directory
	var fileInfo windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &fileInfo); err != nil {
		windows.CloseHandle(handle)
		return s.mapWindowsErrorToResponse(&req, err), nil
	}

	isDir := fileInfo.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0

	fileHandle := &FileHandle{
		handle: handle,
		path:   fullPath,
		isDir:  isDir,
	}

	// Create a new handle ID
	s.mu.Lock()
	s.nextHandle++
	handleID := s.nextHandle
	s.handles[handleID] = fileHandle
	s.mu.Unlock()

	return arpc.Response{
		Status: 200,
		Data:   map[string]uint64{"handleID": handleID},
	}, nil
}

func (s *VSSFSServer) handleStat(req arpc.Request) (arpc.Response, error) {
	var params struct {
		Path string `json:"path"`
	}
	if err := json.Unmarshal(req.Payload, &params); err != nil {
		return s.invalidRequest(req.Method, s.drive, err), nil
	}

	fullPath := filepath.Join(s.rootDir, filepath.Clean(params.Path))

	pathPtr, err := windows.UTF16PtrFromString(fullPath)
	if err != nil {
		return s.respondError(req.Method, s.drive, err), nil
	}

	var fileInfo windows.Win32FileAttributeData
	err = windows.GetFileAttributesEx(pathPtr, windows.GetFileExInfoStandard, (*byte)(unsafe.Pointer(&fileInfo)))
	if err != nil {
		return s.mapWindowsErrorToResponse(&req, err), nil
	}

	return arpc.Response{
		Status: 200,
		Data:   s.fileAttributesToMap(fileInfo, fullPath),
	}, nil
}

func (s *VSSFSServer) handleReadDir(req arpc.Request) (arpc.Response, error) {
	var params struct {
		Path string `json:"path"`
	}
	if err := json.Unmarshal(req.Payload, &params); err != nil {
		return s.invalidRequest(req.Method, s.drive, err), nil
	}

	fullPath := filepath.Join(s.rootDir, filepath.Clean(params.Path))

	pattern := filepath.Join(fullPath, "*")

	patternPtr, err := windows.UTF16PtrFromString(pattern)
	if err != nil {
		return s.respondError(req.Method, s.drive, err), nil
	}

	var findData windows.Win32finddata
	handle, err := windows.FindFirstFile(patternPtr, &findData)
	if err != nil {
		return s.mapWindowsErrorToResponse(&req, err), nil
	}
	defer windows.FindClose(handle)

	results := make([]map[string]interface{}, 0)

	for {
		fileName := windows.UTF16ToString(findData.FileName[:])
		if fileName != "." && fileName != ".." {
			if !skipPathWithAttributes(findData.FileAttributes) {
				info := s.findDataToMap(&findData)
				results = append(results, info)
			}
		}

		if err := windows.FindNextFile(handle, &findData); err != nil {
			if err == windows.ERROR_NO_MORE_FILES {
				break // No more files
			}
			return s.mapWindowsErrorToResponse(&req, err), nil
		}
	}

	return arpc.Response{
		Status: 200,
		Data:   map[string]interface{}{"entries": results},
	}, nil
}

func (s *VSSFSServer) handleRead(req arpc.Request) (arpc.Response, error) {
	var params struct {
		HandleID uint64 `json:"handleID"`
		Length   int    `json:"length"`
	}
	if err := json.Unmarshal(req.Payload, &params); err != nil {
		return s.invalidRequest(req.Method, s.drive, err), nil
	}

	s.mu.RLock()
	handle, exists := s.handles[params.HandleID]
	s.mu.RUnlock()

	if !exists || handle.isClosed {
		return arpc.Response{Status: 404, Message: "invalid handle"}, nil
	}

	if handle.isDir {
		return arpc.Response{Status: 400, Message: "cannot read from directory"}, nil
	}

	// Simple file read with windows API
	buf := make([]byte, params.Length)
	var bytesRead uint32
	err := windows.ReadFile(handle.handle, buf, &bytesRead, nil)
	isEOF := false

	if err != nil {
		if err != windows.ERROR_HANDLE_EOF {
			return s.respondError(req.Method, s.drive, err), nil
		}
		isEOF = true
	}

	return arpc.Response{
		Status: 200,
		Data: map[string]interface{}{
			"data": buf[:bytesRead],
			"eof":  isEOF,
		},
	}, nil
}

func (s *VSSFSServer) handleReadAt(req arpc.Request) (arpc.Response, error) {
	var params struct {
		HandleID uint64 `json:"handleID"`
		Offset   int64  `json:"offset"`
		Length   int    `json:"length"`
	}
	if err := json.Unmarshal(req.Payload, &params); err != nil {
		return s.invalidRequest(req.Method, s.drive, err), nil
	}

	s.mu.RLock()
	handle, exists := s.handles[params.HandleID]
	s.mu.RUnlock()

	if !exists || handle.isClosed {
		return arpc.Response{Status: 404, Message: "invalid handle"}, nil
	}

	if handle.isDir {
		return arpc.Response{Status: 400, Message: "cannot read from directory"}, nil
	}

	// Read from file at specific offset
	buf := make([]byte, params.Length)
	var bytesRead uint32
	var overlapped windows.Overlapped
	overlapped.Offset = uint32(params.Offset & 0xFFFFFFFF)
	overlapped.OffsetHigh = uint32(params.Offset >> 32)

	err := windows.ReadFile(handle.handle, buf, &bytesRead, &overlapped)
	isEOF := false

	if err != nil {
		if err != windows.ERROR_HANDLE_EOF {
			return s.respondError(req.Method, s.drive, err), nil
		}
		isEOF = true
	}

	return arpc.Response{
		Status: 200,
		Data: map[string]interface{}{
			"data": buf[:bytesRead],
			"eof":  isEOF,
		},
	}, nil
}

func (s *VSSFSServer) handleClose(req arpc.Request) (arpc.Response, error) {
	var params struct {
		HandleID uint64 `json:"handleID"`
	}
	if err := json.Unmarshal(req.Payload, &params); err != nil {
		return s.invalidRequest(req.Method, s.drive, err), nil
	}

	s.mu.Lock()
	handle, exists := s.handles[params.HandleID]
	if exists {
		delete(s.handles, params.HandleID)
	}
	s.mu.Unlock()

	if !exists || handle.isClosed {
		return arpc.Response{Status: 404, Message: "invalid handle"}, nil
	}

	// Close file handle
	windows.CloseHandle(handle.handle)
	handle.isClosed = true

	return arpc.Response{Status: 200}, nil
}

// handleFstat gets information about an open file
func (s *VSSFSServer) handleFstat(req arpc.Request) (arpc.Response, error) {
	var params struct {
		HandleID uint64 `json:"handleID"`
	}
	if err := json.Unmarshal(req.Payload, &params); err != nil {
		return s.invalidRequest(req.Method, s.drive, err), nil
	}

	s.mu.RLock()
	handle, exists := s.handles[params.HandleID]
	s.mu.RUnlock()

	if !exists || handle.isClosed {
		return arpc.Response{Status: 404, Message: "invalid handle"}, nil
	}

	var fileInfo windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle.handle, &fileInfo); err != nil {
		return s.mapWindowsErrorToResponse(&req, err), nil
	}

	return arpc.Response{
		Status: 200,
		Data:   s.byHandleFileInfoToMap(&fileInfo, handle.path),
	}, nil
}
