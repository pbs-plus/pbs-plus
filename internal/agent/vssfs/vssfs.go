//go:build windows

package vssfs

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/Microsoft/go-winio"
	securejoin "github.com/cyphar/filepath-securejoin"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils/idgen"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils/safemap"
	"github.com/valyala/bytebufferpool"
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
	ctx         context.Context
	ctxCancel   context.CancelFunc
	jobId       string
	snapshot    snapshots.WinVSSSnapshot
	handleIdGen *idgen.IDGenerator
	handles     *safemap.Map[uint64, *FileHandle]
	arpcRouter  *arpc.Router
	statFs      StatFS
	statFsBytes []byte
}

func NewVSSFSServer(jobId string, snapshot snapshots.WinVSSSnapshot) *VSSFSServer {
	ctx, cancel := context.WithCancel(context.Background())

	s := &VSSFSServer{
		snapshot:    snapshot,
		jobId:       jobId,
		handles:     safemap.New[uint64, *FileHandle](),
		ctx:         ctx,
		ctxCancel:   cancel,
		handleIdGen: idgen.NewIDGenerator(),
	}

	if err := s.initializeStatFS(); err != nil && syslog.L != nil {
		syslog.L.Errorf("initializeStatFS error: %s", err)
	}

	return s
}

func (s *VSSFSServer) RegisterHandlers(r *arpc.Router) {
	r.Handle(s.jobId+"/OpenFile", s.handleOpenFile)
	r.Handle(s.jobId+"/Stat", s.handleStat)
	r.Handle(s.jobId+"/ReadDir", s.handleReadDir)
	r.Handle(s.jobId+"/ReadAt", s.handleReadAt)
	r.Handle(s.jobId+"/Lseek", s.handleLseek)
	r.Handle(s.jobId+"/Close", s.handleClose)
	r.Handle(s.jobId+"/StatFS", s.handleStatFS)

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
		r.CloseHandle(s.jobId + "/Lseek")
		r.CloseHandle(s.jobId + "/Close")
		r.CloseHandle(s.jobId + "/StatFS")
	}

	s.handles.Clear()

	s.ctxCancel()
}

func (s *VSSFSServer) initializeStatFS() error {
	var err error
	s.statFs, err = getStatFS(s.snapshot.DriveLetter)
	if err != nil {
		return err
	}

	s.statFsBytes, err = s.statFs.MarshalMsg(nil)
	if err != nil {
		return err
	}

	return nil
}

func (s *VSSFSServer) handleStatFS(req arpc.Request) (arpc.Response, error) {
	return arpc.Response{
		Status: 200,
		Data:   s.statFsBytes,
	}, nil
}

func (s *VSSFSServer) handleOpenFile(req arpc.Request) (arpc.Response, error) {
	var payload OpenFileReq
	if _, err := payload.UnmarshalMsg(req.Payload); err != nil {
		return arpc.Response{}, err
	}

	// Disallow write operations.
	if payload.Flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC) != 0 {
		errStr := arpc.StringMsg("write operations not allowed")
		errBytes, err := errStr.MarshalMsg(nil)
		if err != nil {
			return arpc.Response{}, err
		}
		return arpc.Response{
			Status: 403,
			Data:   errBytes,
		}, nil
	}

	path, err := s.abs(payload.Path)
	if err != nil {
		return arpc.Response{}, err
	}

	// Check file status to mark directories.
	stat, err := os.Stat(path)
	if err != nil {
		return arpc.Response{}, err
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
		return arpc.Response{}, err
	}

	f := os.NewFile(uintptr(handle), path)

	handleId := s.handleIdGen.NextID()
	fh := &FileHandle{
		handle: handle,
		file:   f,
		path:   path,
		isDir:  stat.IsDir(),
	}
	s.handles.Set(handleId, fh)

	// Return the handle ID to the client.
	dataBytes, err := FileHandleId(handleId).MarshalMsg(nil)
	if err != nil {
		return arpc.Response{}, err
	}

	return arpc.Response{
		Status: 200,
		Data:   dataBytes,
	}, nil
}

// handleStat first checks the cache. If an entry is available it pops (removes)
// the CachedEntry and returns the stat info. Otherwise, it falls back to the
// Windows API‐based lookup.
func (s *VSSFSServer) handleStat(req arpc.Request) (arpc.Response, error) {
	var payload StatReq
	if _, err := payload.UnmarshalMsg(req.Payload); err != nil {
		return arpc.Response{}, err
	}

	fullPath, err := s.abs(payload.Path)
	if err != nil {
		return arpc.Response{}, err
	}

	rawInfo, err := os.Stat(fullPath)
	if err != nil {
		return arpc.Response{}, err
	}

	info := VSSFileInfo{
		Name:    rawInfo.Name(),
		Size:    rawInfo.Size(),
		Mode:    uint32(rawInfo.Mode()),
		ModTime: rawInfo.ModTime(),
		IsDir:   rawInfo.IsDir(),
	}

	// Only check for sparse file attributes and block allocation if it's a regular file
	if !rawInfo.IsDir() {
		file, err := os.Open(fullPath)
		if err != nil {
			return arpc.Response{}, fmt.Errorf("failed to open file: %w", err)
		}
		defer file.Close()

		var blockSize uint64
		if s.statFs != (StatFS{}) {
			blockSize = s.statFs.Bsize
		}
		if blockSize == 0 {
			blockSize = 4096 // Default to 4KB block size
		}

		standardInfo, err := winio.GetFileStandardInfo(file)
		if err != nil {
			return arpc.Response{}, fmt.Errorf("failed to get file standard info: %w", err)
		}

		info.Blocks = uint64((standardInfo.AllocationSize + int64(blockSize) - 1) / int64(blockSize))
	} else {
		info.Blocks = 0
	}

	data, err := info.MarshalMsg(nil)
	if err != nil {
		return arpc.Response{}, err
	}
	return arpc.Response{
		Status: 200,
		Data:   data,
	}, nil
}

// handleReadDir first attempts to serve the directory listing from the cache.
// It returns the cached DirEntries for that directory.
func (s *VSSFSServer) handleReadDir(req arpc.Request) (arpc.Response, error) {
	var payload ReadDirReq
	if _, err := payload.UnmarshalMsg(req.Payload); err != nil {
		return arpc.Response{}, err
	}

	windowsDir := filepath.FromSlash(payload.Path)
	fullDirPath, err := s.abs(windowsDir)
	if err != nil {
		return arpc.Response{}, err
	}

	// If the payload is empty (or "."), use the root.
	if payload.Path == "." || payload.Path == "" {
		fullDirPath = s.snapshot.SnapshotPath
	}

	entries, err := s.readDirBulk(fullDirPath)
	if err != nil {
		return arpc.Response{}, err
	}

	return arpc.Response{
		Status: 200,
		Data:   entries,
	}, nil
}

// handleReadAt now duplicates the file handle, opens a backup reading session,
// and then uses backupSeek to skip to the desired offset without copying bytes.
func (s *VSSFSServer) handleReadAt(req arpc.Request) (arpc.Response, error) {
	var payload ReadAtReq
	if _, err := payload.UnmarshalMsg(req.Payload); err != nil {
		return arpc.Response{}, err
	}

	fh, exists := s.handles.Get(uint64(payload.HandleID))
	if !exists {
		return arpc.Response{}, os.ErrNotExist
	}
	if fh.isDir {
		return arpc.Response{}, os.ErrInvalid
	}

	// Get a buffer from the bytebufferpool
	buf := bytebufferpool.Get()
	defer func() {
		// Ensure the buffer is returned to the pool only if it's not nil
		if buf != nil {
			bytebufferpool.Put(buf)
		}
	}()

	// Ensure the buffer has enough capacity
	if cap(buf.B) < int(payload.Length) {
		buf.B = make([]byte, payload.Length)
	} else {
		buf.B = buf.B[:payload.Length]
	}

	// Perform synchronous read - first seek to the position
	distanceToMove := int64(payload.Offset)
	moveMethod := uint32(windows.FILE_BEGIN)

	newPos, err := windows.Seek(fh.handle, distanceToMove, int(moveMethod))
	if err != nil {
		return arpc.Response{}, mapWinError(err)
	}
	if newPos != payload.Offset {
		return arpc.Response{}, os.ErrInvalid
	}

	var bytesRead uint32
	err = windows.ReadFile(fh.handle, buf.B, &bytesRead, nil)
	if err != nil {
		return arpc.Response{}, mapWinError(err)
	}

	// Resize the buffer to the actual number of bytes read
	buf.B = buf.B[:bytesRead]

	// Create a local reference to the buffer for the callback
	callbackBuf := buf
	buf = nil // Prevent double release in the deferred function

	streamCallback := func(stream *smux.Stream) {
		// Ensure the buffer is returned to the pool after the callback
		defer func() {
			if callbackBuf != nil {
				bytebufferpool.Put(callbackBuf)
			}
		}()

		if stream == nil {
			return
		}

		// Write length prefix
		length := uint32(callbackBuf.Len())
		if err := binary.Write(stream, binary.LittleEndian, length); err != nil {
			return
		}

		// Write data
		_, _ = stream.Write(callbackBuf.B)
	}

	return arpc.Response{
		Status:    213,
		RawStream: streamCallback,
	}, nil
}

func (s *VSSFSServer) handleLseek(req arpc.Request) (arpc.Response, error) {
	var payload LseekReq
	if _, err := payload.UnmarshalMsg(req.Payload); err != nil {
		return arpc.Response{}, err
	}

	// Validate whence
	if payload.Whence != io.SeekStart &&
		payload.Whence != io.SeekCurrent &&
		payload.Whence != io.SeekEnd &&
		payload.Whence != SeekData &&
		payload.Whence != SeekHole {
		return arpc.Response{}, os.ErrInvalid
	}

	// Retrieve the file handle
	fh, exists := s.handles.Get(uint64(payload.HandleID))
	if !exists {
		return arpc.Response{}, os.ErrNotExist
	}
	if fh.isDir {
		return arpc.Response{}, os.ErrInvalid
	}

	// Query the file size
	fileSize, err := getFileSize(fh.handle)
	if err != nil {
		return arpc.Response{}, err
	}

	var newOffset int64

	// Handle sparse file operations
	if payload.Whence == SeekData || payload.Whence == SeekHole {
		newOffset, err = sparseSeek(fh.handle, payload.Offset, payload.Whence, fileSize)
		if err != nil {
			return arpc.Response{}, err
		}
	} else {
		// Handle standard seek operations
		switch payload.Whence {
		case io.SeekStart:
			if payload.Offset < 0 {
				return arpc.Response{}, os.ErrInvalid
			}
			newOffset = payload.Offset

		case io.SeekCurrent:
			currentPos, err := windows.SetFilePointer(fh.handle, 0, nil, windows.FILE_CURRENT)
			if err != nil {
				return arpc.Response{}, err
			}
			newOffset = int64(currentPos) + payload.Offset
			if newOffset < 0 {
				return arpc.Response{}, os.ErrInvalid
			}

		case io.SeekEnd:
			newOffset = fileSize + payload.Offset
			if newOffset < 0 {
				return arpc.Response{}, os.ErrInvalid
			}
		}
	}

	// Validate the new offset
	if newOffset > fileSize {
		return arpc.Response{}, syscall.ENXIO // Seeking beyond EOF
	}

	// Set the new position
	_, err = windows.SetFilePointer(fh.handle, int32(newOffset), nil, windows.FILE_BEGIN)
	if err != nil {
		return arpc.Response{}, err
	}

	// Prepare the response
	resp := LseekResp{
		NewOffset: newOffset,
	}
	respBytes, err := resp.MarshalMsg(nil)
	if err != nil {
		return arpc.Response{}, err
	}

	return arpc.Response{
		Status: 200,
		Data:   respBytes,
	}, nil
}

func (s *VSSFSServer) handleClose(req arpc.Request) (arpc.Response, error) {
	var payload CloseReq
	if _, err := payload.UnmarshalMsg(req.Payload); err != nil {
		return arpc.Response{}, err
	}

	handle, exists := s.handles.Get(uint64(payload.HandleID))
	if !exists {
		return arpc.Response{}, os.ErrNotExist
	}

	// Close the underlying file.
	handle.file.Close()

	s.handles.Del(uint64(payload.HandleID))

	closed := arpc.StringMsg("closed")
	data, err := closed.MarshalMsg(nil)
	if err != nil {
		return arpc.Response{}, err
	}

	return arpc.Response{Status: 200, Data: data}, nil
}

func (s *VSSFSServer) abs(filename string) (string, error) {
	if filename == "" || filename == "." {
		return s.snapshot.SnapshotPath, nil
	}
	path, err := securejoin.SecureJoin(s.snapshot.SnapshotPath, filename)
	if err != nil {
		return "", err
	}
	return path, nil
}
