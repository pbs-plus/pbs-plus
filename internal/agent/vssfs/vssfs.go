//go:build windows

package vssfs

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/alphadose/haxmap"
	securejoin "github.com/cyphar/filepath-securejoin"
	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils/hashmap"
	"github.com/xtaci/smux"
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
	ctx             context.Context
	ctxCancel       context.CancelFunc
	jobId           string
	rootDir         string
	handles         *haxmap.Map[uint64, *FileHandle]
	readAtStatCache *haxmap.Map[uint64, *windows.ByHandleFileInformation]
	nextHandle      uint64
	arpcRouter      *arpc.Router
	bufferPool      *BufferPool
}

func NewVSSFSServer(jobId string, root string) *VSSFSServer {
	ctx, cancel := context.WithCancel(context.Background())
	s := &VSSFSServer{
		rootDir:         root,
		jobId:           jobId,
		handles:         hashmap.NewUint64[*FileHandle](),
		readAtStatCache: hashmap.NewUint64[*windows.ByHandleFileInformation](),
		ctx:             ctx,
		ctxCancel:       cancel,
		bufferPool:      NewBufferPool(),
	}

	return s
}

func (s *VSSFSServer) RegisterHandlers(r *arpc.Router) {
	r.Handle(s.jobId+"/OpenFile", s.handleOpenFile)
	r.Handle(s.jobId+"/Stat", s.handleStat)
	r.Handle(s.jobId+"/ReadDir", s.handleReadDir)
	r.Handle(s.jobId+"/ReadAt", s.handleReadAt)
	r.Handle(s.jobId+"/Close", s.handleClose)
	r.Handle(s.jobId+"/Fstat", s.handleFstat)
	r.Handle(s.jobId+"/FSstat", s.handleFsStat)

	s.arpcRouter = r
}

// Update the Close method so that the cache is stopped:
func (s *VSSFSServer) Close() {
	if s.arpcRouter != nil {
		r := s.arpcRouter
		r.CloseHandle(s.jobId + "/OpenFile")
		r.CloseHandle(s.jobId + "/Stat")
		r.CloseHandle(s.jobId + "/ReadDir")
		r.CloseHandle(s.jobId + "/ReadAt")
		r.CloseHandle(s.jobId + "/Close")
		r.CloseHandle(s.jobId + "/Fstat")
		r.CloseHandle(s.jobId + "/FSstat")
	}

	s.handles.Clear()
	atomic.StoreUint64(&s.nextHandle, 0)

	s.ctxCancel()
}

// --- Handlers ---

func (s *VSSFSServer) handleFsStat(req arpc.Request) (*arpc.Response, error) {
	// No payload expected.
	var totalBytes uint64
	err := windows.GetDiskFreeSpaceEx(
		windows.StringToUTF16Ptr(s.rootDir),
		nil,
		&totalBytes,
		nil,
	)
	if err != nil {
		return nil, err
	}

	statFs := &StatFS{
		Bsize:   uint64(4096), // Standard block size.
		Blocks:  uint64(totalBytes / 4096),
		Bfree:   0,
		Bavail:  0,
		Files:   uint64(1 << 20),
		Ffree:   0,
		NameLen: 255, // Typically supports long filenames.
	}

	fsStatBytes, err := statFs.MarshalMsg(nil)
	if err != nil {
		return nil, err
	}

	return &arpc.Response{
		Status: 200,
		Data:   fsStatBytes,
	}, nil
}

func (s *VSSFSServer) handleOpenFile(req arpc.Request) (*arpc.Response, error) {
	var payload OpenFileReq
	if _, err := payload.UnmarshalMsg(req.Payload); err != nil {
		return nil, err
	}

	// Disallow write operations.
	if payload.Flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC) != 0 {
		errStr := arpc.StringMsg("write operations not allowed")
		errBytes, err := errStr.MarshalMsg(nil)
		if err != nil {
			return nil, err
		}

		return &arpc.Response{
			Status: 403,
			Data:   errBytes,
		}, nil
	}

	path, err := s.abs(payload.Path)
	if err != nil {
		return nil, err
	}

	pathp, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}

	handle, err := windows.CreateFile(
		pathp,
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_SEQUENTIAL_SCAN|windows.FILE_FLAG_BACKUP_SEMANTICS,
		0,
	)
	if err != nil {
		return nil, err
	}

	fileHandle := &FileHandle{
		handle: handle,
		path:   path,
	}

	handleID := atomic.AddUint64(&s.nextHandle, 1)

	s.handles.Set(handleID, fileHandle)
	data := FileHandleId(handleID)

	dataBytes, err := data.MarshalMsg(nil)
	if err != nil {
		return nil, err
	}

	// Return the handle ID.
	return &arpc.Response{
		Status: 200,
		Data:   dataBytes,
	}, nil
}

// handleStat first checks the cache. If an entry is available it pops (removes)
// the CachedEntry and returns the stat info. Otherwise, it falls back to the
// Windows API‐based lookup.
func (s *VSSFSServer) handleStat(req arpc.Request) (*arpc.Response, error) {
	var payload StatReq
	if _, err := payload.UnmarshalMsg(req.Payload); err != nil {
		return nil, err
	}

	fullPath, err := s.abs(payload.Path)
	if err != nil {
		return nil, err
	}

	rawInfo, err := os.Stat(fullPath)
	if err != nil {
		return nil, err
	}

	info := &VSSFileInfo{
		Name:    rawInfo.Name(),
		Size:    rawInfo.Size(),
		Mode:    uint32(rawInfo.Mode()),
		ModTime: rawInfo.ModTime(),
		IsDir:   rawInfo.IsDir(),
	}

	data, err := info.MarshalMsg(nil)
	if err != nil {
		return nil, err
	}
	return &arpc.Response{
		Status: 200,
		Data:   data,
	}, nil
}

// handleReadDir first attempts to serve the directory listing from the cache.
// It returns the cached DirEntries for that directory.
func (s *VSSFSServer) handleReadDir(req arpc.Request) (*arpc.Response, error) {
	var payload ReadDirReq
	if _, err := payload.UnmarshalMsg(req.Payload); err != nil {
		return nil, err
	}

	// Convert the provided path to Windows style and compute the full path.
	windowsDir := filepath.FromSlash(payload.Path)
	fullDirPath, err := s.abs(windowsDir)
	if err != nil {
		return nil, err
	}

	// If the payload is empty (or "."), use the root.
	if payload.Path == "." || payload.Path == "" {
		fullDirPath = s.rootDir
	}

	var entries ReadDirEntries
	dirEntries, err := os.ReadDir(fullDirPath)
	for _, t := range dirEntries {
		entries = append(entries, &VSSDirEntry{
			Name: t.Name(),
			Mode: uint32(t.Type()),
		})
	}

	// Marshal entries into bytes for the FUSE response.
	entryBytes, err := entries.MarshalMsg(nil)
	if err != nil {
		return nil, err
	}

	return &arpc.Response{
		Status: 200,
		Data:   entryBytes,
	}, nil
}

// handleReadAt now duplicates the file handle, opens a backup reading session,
// and then uses backupSeek to skip to the desired offset without copying bytes.
func (s *VSSFSServer) handleReadAt(req arpc.Request) (*arpc.Response, error) {
	var payload ReadAtReq
	if _, err := payload.UnmarshalMsg(req.Payload); err != nil {
		return nil, err
	}

	// Retrieve the stored file handle.
	fh, exists := s.handles.Get(uint64(payload.HandleID))
	if !exists || fh.isClosed || fh.isDir {
		return nil, os.ErrNotExist
	}

	// Duplicate the file handle so we can start a fresh backup read session.
	var dup windows.Handle
	err := windows.DuplicateHandle(
		windows.CurrentProcess(),
		fh.handle,
		windows.CurrentProcess(),
		&dup,
		0,
		false,
		windows.DUPLICATE_SAME_ACCESS,
	)
	if err != nil {
		return nil, err
	}
	defer windows.CloseHandle(dup)

	data, eof, totalSize, err := readFileBackupOptimized(dup, uint64(payload.Offset), int(payload.Length))
	if err != nil {
		return nil, err
	}

	meta := arpc.BufferMetadata{BytesAvailable: len(data), EOF: eof}
	metaBytes, err := meta.MarshalMsg(nil)
	if err != nil {
		return nil, err
	}

	streamCallback := func(stream *smux.Stream) {
		if stream == nil {
			return
		}
		if _, err := stream.Write(data); err != nil {
			syslog.L.Errorf("stream.Write error: %v", err)
		}
	}

	syslog.L.Infof("Optimized ReadAt: offset %d, read %d/%d bytes",
		payload.Offset, len(data), totalSize)
	return &arpc.Response{
		Status:    213,
		Data:      metaBytes,
		RawStream: streamCallback,
	}, nil
}

func (s *VSSFSServer) handleClose(req arpc.Request) (*arpc.Response, error) {
	var payload CloseReq
	if _, err := payload.UnmarshalMsg(req.Payload); err != nil {
		return nil, err
	}

	handle, exists := s.handles.GetAndDel(uint64(payload.HandleID))
	if !exists || handle.isClosed {
		return nil, os.ErrNotExist
	}

	windows.CloseHandle(handle.handle)
	handle.isClosed = true

	s.readAtStatCache.Del(uint64(payload.HandleID))

	closed := arpc.StringMsg("closed")
	data, err := closed.MarshalMsg(nil)
	if err != nil {
		return nil, err
	}

	return &arpc.Response{Status: 200, Data: data}, nil
}

func (s *VSSFSServer) handleFstat(req arpc.Request) (*arpc.Response, error) {
	var payload FstatReq
	if _, err := payload.UnmarshalMsg(req.Payload); err != nil {
		return nil, err
	}

	handle, exists := s.handles.Get(uint64(payload.HandleID))
	if !exists || handle.isClosed {
		return nil, os.ErrNotExist
	}

	var fileInfo windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle.handle, &fileInfo); err != nil {
		return nil, err
	}

	info := createFileInfoFromHandleInfo(handle.path, &fileInfo)
	data, err := info.MarshalMsg(nil)
	if err != nil {
		return nil, err
	}
	return &arpc.Response{
		Status: 200,
		Data:   data,
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
