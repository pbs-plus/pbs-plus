//go:build windows

package vssfs

import (
	"context"
	"io"
	"os"
	"path/filepath"

	"github.com/alphadose/haxmap"
	securejoin "github.com/cyphar/filepath-securejoin"
	"github.com/rekby/fastuuid"
	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils/hashmap"
	"github.com/xtaci/smux"
	"golang.org/x/sys/windows"
)

type FileHandle struct {
	handle windows.Handle
	file   io.ReadWriteCloser
	path   string
	isDir  bool
}

type VSSFSServer struct {
	ctx             context.Context
	ctxCancel       context.CancelFunc
	jobId           string
	rootDir         string
	handles         *haxmap.Map[string, *FileHandle]
	readAtStatCache *haxmap.Map[string, *windows.ByHandleFileInformation]
	arpcRouter      *arpc.Router
	bufferPool      *BufferPool
}

func NewVSSFSServer(jobId string, root string) *VSSFSServer {
	ctx, cancel := context.WithCancel(context.Background())

	s := &VSSFSServer{
		rootDir:         root,
		jobId:           jobId,
		handles:         hashmap.New[*FileHandle](),
		readAtStatCache: hashmap.New[*windows.ByHandleFileInformation](),
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
		r.CloseHandle(s.jobId + "/FSstat")
	}

	s.handles.Clear()
	s.readAtStatCache.Clear()

	s.ctxCancel()
}

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

	// Check file status to mark directories.
	stat, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	handle, err := windows.CreateFile(
		windows.StringToUTF16Ptr(path),
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_SEQUENTIAL_SCAN,
		0,
	)
	if err != nil {
		return nil, err
	}

	f := os.NewFile(uintptr(handle), path)

	handleId, err := fastuuid.UUIDv4String()
	if err != nil {
		return nil, os.ErrInvalid
	}

	fh := &FileHandle{
		handle: handle,
		file:   f,
		path:   path,
		isDir:  stat.IsDir(),
	}
	s.handles.Set(handleId, fh)

	// Return the handle ID to the client.
	respHandle := FileHandleId(handleId)
	dataBytes, err := respHandle.MarshalMsg(nil)
	if err != nil {
		return nil, err
	}

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

	windowsDir := filepath.FromSlash(payload.Path)
	fullDirPath, err := s.abs(windowsDir)
	if err != nil {
		return nil, err
	}

	// If the payload is empty (or "."), use the root.
	if payload.Path == "." || payload.Path == "" {
		fullDirPath = s.rootDir
	}

	entries, err := s.readDirBulk(fullDirPath)
	if err != nil {
		return nil, err
	}

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

	fh, exists := s.handles.Get(string(payload.HandleID))
	if !exists {
		return nil, os.ErrNotExist
	}
	if fh.isDir {
		return nil, os.ErrInvalid
	}

	buf := s.bufferPool.Get(payload.Length)

	// Perform synchronous read - first seek to the position
	distanceToMove := int64(payload.Offset)
	moveMethod := uint32(windows.FILE_BEGIN)

	// Use windows.SetFilePointer for seeking
	newPos, err := windows.Seek(fh.handle, distanceToMove, int(moveMethod))
	if err != nil {
		s.bufferPool.Put(buf)
		return nil, mapWinError(err)
	}
	if newPos != payload.Offset {
		s.bufferPool.Put(buf)
		return nil, os.ErrInvalid
	}

	// Then read from that position
	var bytesRead uint32
	err = windows.ReadFile(fh.handle, buf, &bytesRead, nil)
	if err != nil {
		s.bufferPool.Put(buf)
		return nil, mapWinError(err)
	}

	eof := bytesRead < uint32(payload.Length)
	buf = buf[:bytesRead]

	meta := arpc.BufferMetadata{
		BytesAvailable: int(bytesRead),
		EOF:            eof,
	}
	metaBytes, err := meta.MarshalMsg(nil)
	if err != nil {
		s.bufferPool.Put(buf)
		return nil, err
	}

	streamCallback := func(stream *smux.Stream) {
		defer func() {
			s.bufferPool.Put(buf)
		}()

		if stream == nil {
			return
		}
		if _, err := stream.Write(buf); err != nil {
		}
	}

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

	handle, exists := s.handles.Get(string(payload.HandleID))
	if !exists {
		return nil, os.ErrNotExist
	}

	// Close the underlying file.
	handle.file.Close()

	s.handles.Del(string(payload.HandleID))
	s.readAtStatCache.Del(string(payload.HandleID))

	closed := arpc.StringMsg("closed")
	data, err := closed.MarshalMsg(nil)
	if err != nil {
		return nil, err
	}

	return &arpc.Response{Status: 200, Data: data}, nil
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
