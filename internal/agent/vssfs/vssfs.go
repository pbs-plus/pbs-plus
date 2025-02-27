//go:build windows

package vssfs

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	securejoin "github.com/cyphar/filepath-securejoin"
	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
	"github.com/vmihailenco/msgpack/v5"
	"golang.org/x/sys/windows"
)

// --- Types ---

type FileHandle struct {
	handle   windows.Handle
	path     string
	isDir    bool
	isClosed bool
}

type VSSFSServer struct {
	jobId      string
	rootDir    string
	handles    map[uint64]*FileHandle
	nextHandle uint64
	mu         sync.RWMutex
	arpcRouter *arpc.Router
}

type DirectBufferWrite struct {
	Data []byte
}

func (d *DirectBufferWrite) Error() string {
	return "direct buffer write requested"
}

func NewVSSFSServer(jobId string, root string) *VSSFSServer {
	return &VSSFSServer{
		rootDir: root,
		jobId:   jobId,
		handles: make(map[uint64]*FileHandle),
	}
}

// --- Registration & Cleanup ---

func (s *VSSFSServer) RegisterHandlers(r *arpc.Router) {
	r.Handle(s.jobId+"/OpenFile", s.handleOpenFile)
	r.Handle(s.jobId+"/Stat", s.handleStat)
	r.Handle(s.jobId+"/ReadDir", s.handleReadDir)
	r.Handle(s.jobId+"/Read", s.handleRead)
	r.Handle(s.jobId+"/ReadAt", s.handleReadAt)
	r.Handle(s.jobId+"/Close", s.handleClose)
	r.Handle(s.jobId+"/Fstat", s.handleFstat)
	r.Handle(s.jobId+"/FSstat", s.handleFsStat)

	s.arpcRouter = r
}

func (s *VSSFSServer) Close() {
	r := s.arpcRouter
	if r == nil {
		return
	}
	r.CloseHandle(s.jobId + "/OpenFile")
	r.CloseHandle(s.jobId + "/Stat")
	r.CloseHandle(s.jobId + "/ReadDir")
	r.CloseHandle(s.jobId + "/Read")
	r.CloseHandle(s.jobId + "/ReadAt")
	r.CloseHandle(s.jobId + "/Close")
	r.CloseHandle(s.jobId + "/Fstat")
	r.CloseHandle(s.jobId + "/FSstat")

	s.mu.Lock()
	defer s.mu.Unlock()
	for k := range s.handles {
		delete(s.handles, k)
	}
	s.nextHandle = 0
	s.arpcRouter = nil
}

// --- Handlers ---

func (s *VSSFSServer) handleFsStat(req arpc.Request) (arpc.Response, error) {
	// No payload expected.
	var totalBytes uint64
	err := windows.GetDiskFreeSpaceEx(
		windows.StringToUTF16Ptr(s.rootDir),
		nil,
		&totalBytes,
		nil,
	)
	if err != nil {
		return s.respondError(req.Method, s.jobId, err), nil
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
	return arpc.Response{
		Status: 200,
		Data:   encodeValue(fsStat),
	}, nil
}

func (s *VSSFSServer) handleOpenFile(req arpc.Request) (arpc.Response, error) {
	// Unmarshal payload into a map.
	var payload map[string]interface{}
	if len(req.Payload) == 0 {
		return s.invalidRequest(req.Method, s.jobId, fmt.Errorf("invalid payload")), nil
	}
	if err := msgpack.Unmarshal(req.Payload, &payload); err != nil {
		return s.invalidRequest(req.Method, s.jobId, err), nil
	}

	path, err := getStringField(payload, "path")
	if err != nil {
		return s.invalidRequest(req.Method, s.jobId, err), nil
	}
	flag, err := getIntField(payload, "flag")
	if err != nil {
		return s.invalidRequest(req.Method, s.jobId, err), nil
	}
	// perm is optional

	// Disallow write operations.
	if flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC) != 0 {
		return arpc.Response{
			Status: 403,
			Data:   encodeValue("write operations not allowed"),
		}, nil
	}

	path, err = s.abs(path)
	if err != nil {
		return s.respondError(req.Method, s.jobId, err), nil
	}

	pathp, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return s.respondError(req.Method, s.jobId, mapWinError(err, path)), nil
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
		return s.respondError(req.Method, s.jobId, mapWinError(err, path)), nil
	}

	fileHandle := &FileHandle{
		handle: handle,
		path:   path,
		isDir:  false,
	}

	s.mu.Lock()
	s.nextHandle++
	handleID := s.nextHandle
	s.handles[handleID] = fileHandle
	s.mu.Unlock()

	// Return result by encoding a simple map.
	return arpc.Response{
		Status: 200,
		Data:   encodeValue(map[string]uint64{"handleID": handleID}),
	}, nil
}

func (s *VSSFSServer) handleStat(req arpc.Request) (arpc.Response, error) {
	var payload map[string]interface{}
	if len(req.Payload) == 0 {
		return s.invalidRequest(req.Method, s.jobId, fmt.Errorf("invalid payload")), nil
	}
	if err := msgpack.Unmarshal(req.Payload, &payload); err != nil {
		return s.invalidRequest(req.Method, s.jobId, err), nil
	}
	path, err := getStringField(payload, "path")
	if err != nil {
		return s.invalidRequest(req.Method, s.jobId, err), nil
	}

	fullPath, err := s.abs(path)
	if err != nil {
		return s.respondError(req.Method, s.jobId, err), nil
	}

	if path == "." || path == "" {
		fullPath = s.rootDir
	}

	pathPtr, err := windows.UTF16PtrFromString(fullPath)
	if err != nil {
		return s.respondError(req.Method, s.jobId, mapWinError(err, path)), nil
	}

	var findData windows.Win32finddata
	handle, err := windows.FindFirstFile(pathPtr, &findData)
	if err != nil {
		return s.respondError(req.Method, s.jobId, mapWinError(err, path)), nil
	}
	defer windows.FindClose(handle)

	foundName := windows.UTF16ToString(findData.FileName[:])
	expectedName := filepath.Base(fullPath)
	if path == "." {
		expectedName = foundName
	}

	if !strings.EqualFold(foundName, expectedName) {
		return s.respondError(req.Method, s.jobId, os.ErrNotExist), nil
	}

	// Create file info from findData.
	name := foundName
	if path == "." {
		name = "."
	} else if path == "/" {
		name = "/"
	}
	info := createFileInfoFromFindData(name, &findData)

	return arpc.Response{
		Status: 200,
		Data:   encodeValue(info),
	}, nil
}

func (s *VSSFSServer) handleReadDir(req arpc.Request) (arpc.Response, error) {
	var payload map[string]interface{}
	if len(req.Payload) == 0 {
		return s.invalidRequest(req.Method, s.jobId, fmt.Errorf("invalid payload")), nil
	}
	if err := msgpack.Unmarshal(req.Payload, &payload); err != nil {
		return s.invalidRequest(req.Method, s.jobId, err), nil
	}
	path, err := getStringField(payload, "path")
	if err != nil {
		return s.invalidRequest(req.Method, s.jobId, err), nil
	}
	windowsDir := filepath.FromSlash(path)
	fullDirPath, err := s.abs(windowsDir)
	if err != nil {
		return s.respondError(req.Method, s.jobId, err), nil
	}
	if path == "." || path == "" {
		windowsDir = "."
		fullDirPath = s.rootDir
	}
	searchPath := filepath.Join(fullDirPath, "*")
	var findData windows.Win32finddata
	handle, err := FindFirstFileEx(searchPath, &findData)
	if err != nil {
		return s.respondError(req.Method, s.jobId, mapWinError(err, path)), nil
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
			return s.respondError(req.Method, s.jobId, err), nil
		}
	}

	return arpc.Response{
		Status: 200,
		Data:   encodeValue(map[string]interface{}{"entries": entries}),
	}, nil
}

func (s *VSSFSServer) handleRead(req arpc.Request) (arpc.Response, error) {
	var payload map[string]interface{}
	if len(req.Payload) == 0 {
		return s.invalidRequest(req.Method, s.jobId, fmt.Errorf("invalid payload")), nil
	}
	if err := msgpack.Unmarshal(req.Payload, &payload); err != nil {
		return s.invalidRequest(req.Method, s.jobId, err), nil
	}
	handleID, err := getIntField(payload, "handleID")
	if err != nil {
		return s.invalidRequest(req.Method, s.jobId, err), nil
	}
	length, err := getIntField(payload, "length")
	if err != nil {
		return s.invalidRequest(req.Method, s.jobId, err), nil
	}

	s.mu.RLock()
	handle, exists := s.handles[uint64(handleID)]
	s.mu.RUnlock()
	if !exists || handle.isClosed {
		return arpc.Response{Status: 404, Data: encodeValue("invalid handle")}, nil
	}
	if handle.isDir {
		return arpc.Response{Status: 400, Data: encodeValue("cannot read from directory")}, nil
	}

	isDirectBuffer := false
	if req.Headers != nil && req.Headers.Get("X-Direct-Buffer") == "true" {
		isDirectBuffer = true
	}

	buf := make([]byte, length)
	var bytesRead uint32
	err = windows.ReadFile(handle.handle, buf, &bytesRead, nil)
	isEOF := false
	if err != nil && err != windows.ERROR_HANDLE_EOF {
		return s.respondError(req.Method, s.jobId, mapWinError(err, handle.path)), nil
	}
	if err == windows.ERROR_HANDLE_EOF {
		isEOF = true
	}

	if isDirectBuffer {
		meta := map[string]interface{}{
			"bytes_available": int(bytesRead),
			"eof":             isEOF,
		}
		return arpc.Response{
			Status: 200,
			Data:   encodeValue(meta),
		}, &DirectBufferWrite{Data: buf[:bytesRead]}
	}

	data := map[string]interface{}{
		"data": buf[:bytesRead],
		"eof":  isEOF,
	}
	return arpc.Response{
		Status: 200,
		Data:   encodeValue(data),
	}, nil
}

func (s *VSSFSServer) handleReadAt(req arpc.Request) (arpc.Response, error) {
	var payload map[string]interface{}
	if len(req.Payload) == 0 {
		return s.invalidRequest(req.Method, s.jobId, fmt.Errorf("invalid payload")), nil
	}
	if err := msgpack.Unmarshal(req.Payload, &payload); err != nil {
		return s.invalidRequest(req.Method, s.jobId, err), nil
	}
	handleID, err := getIntField(payload, "handleID")
	if err != nil {
		return s.invalidRequest(req.Method, s.jobId, err), nil
	}
	offset, err := getInt64Field(payload, "offset")
	if err != nil {
		return s.invalidRequest(req.Method, s.jobId, err), nil
	}
	length, err := getIntField(payload, "length")
	if err != nil {
		return s.invalidRequest(req.Method, s.jobId, err), nil
	}

	s.mu.RLock()
	handle, exists := s.handles[uint64(handleID)]
	s.mu.RUnlock()
	if !exists || handle.isClosed {
		return arpc.Response{Status: 404, Data: encodeValue("invalid handle")}, nil
	}
	if handle.isDir {
		return arpc.Response{Status: 400, Data: encodeValue("cannot read from directory")}, nil
	}

	buf := make([]byte, length)
	var bytesRead uint32
	var overlapped windows.Overlapped
	overlapped.Offset = uint32(offset & 0xFFFFFFFF)
	overlapped.OffsetHigh = uint32(offset >> 32)

	err = windows.ReadFile(handle.handle, buf, &bytesRead, &overlapped)
	isEOF := false
	if err != nil && err != windows.ERROR_HANDLE_EOF {
		return s.respondError(req.Method, s.jobId, mapWinError(err, handle.path)), nil
	}
	if err == windows.ERROR_HANDLE_EOF {
		isEOF = true
	}

	data := map[string]interface{}{
		"data": buf[:bytesRead],
		"eof":  isEOF,
	}
	return arpc.Response{
		Status: 200,
		Data:   encodeValue(data),
	}, nil
}

func (s *VSSFSServer) handleClose(req arpc.Request) (arpc.Response, error) {
	var payload map[string]interface{}
	if len(req.Payload) == 0 {
		return s.invalidRequest(req.Method, s.jobId, fmt.Errorf("invalid payload")), nil
	}
	if err := msgpack.Unmarshal(req.Payload, &payload); err != nil {
		return s.invalidRequest(req.Method, s.jobId, err), nil
	}
	handleID, err := getIntField(payload, "handleID")
	if err != nil {
		return s.invalidRequest(req.Method, s.jobId, err), nil
	}

	s.mu.Lock()
	handle, exists := s.handles[uint64(handleID)]
	if exists {
		delete(s.handles, uint64(handleID))
	}
	s.mu.Unlock()

	if !exists || handle.isClosed {
		return arpc.Response{Status: 404, Data: encodeValue("invalid handle")}, nil
	}

	windows.CloseHandle(handle.handle)
	handle.isClosed = true

	return arpc.Response{Status: 200, Data: encodeValue("closed")}, nil
}

func (s *VSSFSServer) handleFstat(req arpc.Request) (arpc.Response, error) {
	var payload map[string]interface{}
	if len(req.Payload) == 0 {
		return s.invalidRequest(req.Method, s.jobId, fmt.Errorf("invalid payload")), nil
	}
	if err := msgpack.Unmarshal(req.Payload, &payload); err != nil {
		return s.invalidRequest(req.Method, s.jobId, err), nil
	}
	handleID, err := getIntField(payload, "handleID")
	if err != nil {
		return s.invalidRequest(req.Method, s.jobId, err), nil
	}

	s.mu.RLock()
	handle, exists := s.handles[uint64(handleID)]
	s.mu.RUnlock()
	if !exists || handle.isClosed {
		return arpc.Response{Status: 404, Data: encodeValue("invalid handle")}, nil
	}

	var fileInfo windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle.handle, &fileInfo); err != nil {
		return s.respondError(req.Method, s.jobId, mapWinError(err, handle.path)), nil
	}

	info := createFileInfoFromHandleInfo(handle.path, &fileInfo)
	return arpc.Response{
		Status: 200,
		Data:   encodeValue(info),
	}, nil
}

func (s *VSSFSServer) abs(filename string) (string, error) {
	if filename == "" || filename == "." {
		return s.rootDir, nil
	}
	path, err := securejoin.SecureJoin(s.rootDir, filename)
	if err != nil {
		return "", err
	}
	return path, nil
}
