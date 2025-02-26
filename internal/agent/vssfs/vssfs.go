//go:build windows

package vssfs

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	securejoin "github.com/cyphar/filepath-securejoin"
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
		if err != os.ErrNotExist {
			syslog.L.Errorf("%s (%s): %v", method, drive, err)
		}
	}
	return arpc.Response{Status: 500, Data: arpc.WrapError(err)}
}

func (s *VSSFSServer) invalidRequest(method, drive string, err error) arpc.Response {
	if syslog.L != nil {
		syslog.L.Errorf("%s (%s): %v", method, drive, err)
	}
	return arpc.Response{Status: 400, Data: arpc.WrapError(os.ErrInvalid)}
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

	if params.Flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC) != 0 {
		return arpc.Response{Status: 403, Message: "write operations not allowed"}, nil
	}

	path, err := s.abs(params.Path)
	if err != nil {
		return s.respondError(req.Method, s.drive, err), nil
	}

	pathp, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return s.respondError(req.Method, s.drive, err), nil
	}

	handle, err := windows.CreateFile(
		pathp,
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_SEQUENTIAL_SCAN,
		0,
	)
	if err != nil {
		return s.mapWindowsErrorToResponse(&req, err), nil
	}

	fileHandle := &FileHandle{
		handle: handle,
		path:   path,
		isDir:  false,
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

	fullPath, err := s.abs(params.Path)
	if err != nil {
		return s.respondError(req.Method, s.drive, err), nil
	}

	if params.Path == "." || params.Path == "" {
		fullPath = s.rootDir
	}

	pathPtr, err := windows.UTF16PtrFromString(fullPath)
	if err != nil {
		return s.respondError(req.Method, s.drive, err), nil
	}

	var findData windows.Win32finddata
	handle, err := windows.FindFirstFile(pathPtr, &findData)
	if err != nil {
		return s.respondError(req.Method, s.drive, mapWinError(err, params.Path)), nil
	}
	defer windows.FindClose(handle)

	foundName := windows.UTF16ToString(findData.FileName[:])
	expectedName := filepath.Base(fullPath)
	if params.Path == "." {
		expectedName = foundName
	}

	if !strings.EqualFold(foundName, expectedName) {
		return s.respondError(req.Method, s.drive, os.ErrNotExist), nil
	}

	// Use foundName as the file name for FileInfo
	name := foundName
	if params.Path == "." {
		name = "."
	}
	if params.Path == "/" {
		name = "/"
	}

	info := createFileInfoFromFindData(name, &findData)

	return arpc.Response{
		Status: 200,
		Data:   info,
	}, nil
}

func (s *VSSFSServer) handleReadDir(req arpc.Request) (arpc.Response, error) {
	var params struct {
		Path string `json:"path"`
	}
	if err := json.Unmarshal(req.Payload, &params); err != nil {
		return s.invalidRequest(req.Method, s.drive, err), nil
	}
	windowsDir := filepath.FromSlash(params.Path)
	fullDirPath, err := s.abs(windowsDir)
	if err != nil {
		return s.respondError(req.Method, s.drive, err), nil
	}

	if params.Path == "." || params.Path == "" {
		windowsDir = "."
		fullDirPath = s.rootDir
	}
	searchPath := filepath.Join(fullDirPath, "*")
	var findData windows.Win32finddata
	handle, err := FindFirstFileEx(searchPath, &findData)
	if err != nil {
		return s.respondError(req.Method, s.drive, mapWinError(err, params.Path)), nil
	}
	defer windows.FindClose(handle)

	var entries []*VSSFileInfo
	for {
		name := windows.UTF16ToString(findData.FileName[:])
		if name != "." && name != ".." {
			if !skipPathWithAttributes(findData.FileAttributes) {
				info := createFileInfoFromFindData(name, &findData)
				entries = append(entries, info)
			}
		}

		if err := windows.FindNextFile(handle, &findData); err != nil {
			if err == windows.ERROR_NO_MORE_FILES {
				break
			}
			return s.respondError(req.Method, s.drive, mapWinError(err, params.Path)), nil
		}
	}

	return arpc.Response{
		Status: 200,
		Data:   map[string][]*VSSFileInfo{"entries": entries},
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

	info := createFileInfoFromHandleInfo(handle.path, &fileInfo)

	return arpc.Response{
		Status: 200,
		Data:   info,
	}, nil
}

func (s *VSSFSServer) abs(filename string) (string, error) {
	if filename == "" || filename == "." {
		return s.rootDir, nil
	}

	path, err := securejoin.SecureJoin(s.rootDir, filename)
	if err != nil {
		return "", nil
	}

	return path, nil
}
