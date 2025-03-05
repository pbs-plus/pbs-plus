//go:build windows

package vssfs

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"
	"unsafe"

	securejoin "github.com/cyphar/filepath-securejoin"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/vssfs/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	binarystream "github.com/sonroyaalmerol/pbs-plus/internal/arpc/binary"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils/idgen"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils/safemap"
	"github.com/xtaci/smux"
	"golang.org/x/sys/windows"
)

type FileHandle struct {
	handle windows.Handle
	isDir  bool
}

type FileStandardInfo struct {
	AllocationSize, EndOfFile int64
	NumberOfLinks             uint32
	DeletePending, Directory  bool
}

type VSSFSServer struct {
	ctx         context.Context
	ctxCancel   context.CancelFunc
	jobId       string
	snapshot    snapshots.WinVSSSnapshot
	handleIdGen *idgen.IDGenerator
	handles     *safemap.Map[uint64, *FileHandle]
	arpcRouter  *arpc.Router
	statFs      types.StatFS
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

	return nil
}

func (s *VSSFSServer) handleStatFS(req arpc.Request) (arpc.Response, error) {
	enc, err := s.statFs.Encode()
	if err != nil {
		return arpc.Response{}, err
	}
	return arpc.Response{
		Status: 200,
		Data:   enc,
	}, nil
}

func (s *VSSFSServer) handleOpenFile(req arpc.Request) (arpc.Response, error) {
	var payload types.OpenFileReq
	if err := payload.Decode(req.Payload); err != nil {
		return arpc.Response{}, err
	}

	// Disallow write operations.
	if payload.Flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC) != 0 {
		errStr := arpc.WrapError(os.ErrPermission)
		errBytes, err := errStr.Encode()
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
		return arpc.Response{}, mapWinError(err)
	}

	// Determine if the handle is a directory using Windows API
	var fileInfo windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &fileInfo); err != nil {
		windows.CloseHandle(handle)
		return arpc.Response{}, mapWinError(err)
	}
	isDir := (fileInfo.FileAttributes & windows.FILE_ATTRIBUTE_DIRECTORY) != 0

	handleId := s.handleIdGen.NextID()
	fh := &FileHandle{
		handle: handle,
		isDir:  isDir,
	}
	s.handles.Set(handleId, fh)

	// Return the handle ID to the client.
	handleIdT := types.FileHandleId(handleId)
	dataBytes, err := handleIdT.Encode()
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
	var payload types.StatReq
	if err := payload.Decode(req.Payload); err != nil {
		return arpc.Response{}, err
	}

	fullPath, err := s.abs(payload.Path)
	if err != nil {
		return arpc.Response{}, err
	}

	// Use windows.CreateFile to open the file or directory with FILE_FLAG_BACKUP_SEMANTICS
	pathPtr, err := windows.UTF16PtrFromString(fullPath)
	if err != nil {
		return arpc.Response{}, mapWinError(err)
	}

	handle, err := windows.CreateFile(
		pathPtr,
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS,
		0,
	)
	if err != nil {
		return arpc.Response{}, fmt.Errorf("failed to open file: %w", err)
	}
	defer windows.CloseHandle(handle)

	// Use GetFileInformationByHandle to retrieve file information
	var fileInfo windows.ByHandleFileInformation
	err = windows.GetFileInformationByHandle(handle, &fileInfo)
	if err != nil {
		return arpc.Response{}, mapWinError(err)
	}

	// Convert the file information to a Go struct
	isDir := fileInfo.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0
	modTime := time.Unix(0, fileInfo.LastWriteTime.Nanoseconds())
	size := int64(fileInfo.FileSizeHigh)<<32 + int64(fileInfo.FileSizeLow)

	var blocks uint64 = 0
	if !isDir {
		// Use GetFileInformationByHandleEx to retrieve allocation size
		var info FileStandardInfo
		err = windows.GetFileInformationByHandleEx(
			handle,
			windows.FileStandardInfo,
			(*byte)(unsafe.Pointer(&info)),
			uint32(unsafe.Sizeof(info)),
		)
		if err != nil {
			return arpc.Response{}, mapWinError(err)
		}

		// Calculate the number of blocks based on allocation size
		var blockSize = s.statFs.Bsize
		if blockSize == 0 {
			blockSize = 4096 // Default to 4KB
		}
		blocks = uint64((info.AllocationSize + int64(blockSize) - 1) / int64(blockSize))
	}

	info := types.VSSFileInfo{
		Name:    filepath.Base(fullPath),
		Size:    size,
		Mode:    uint32(fileInfo.FileAttributes),
		ModTime: modTime,
		IsDir:   isDir,
		Blocks:  blocks,
	}

	data, err := info.Encode()
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
	var payload types.ReadDirReq
	if err := payload.Decode(req.Payload); err != nil {
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
	var payload types.ReadAtReq
	if err := payload.Decode(req.Payload); err != nil {
		return arpc.Response{}, err
	}

	fh, exists := s.handles.Get(uint64(payload.HandleID))
	if !exists {
		return arpc.Response{}, os.ErrNotExist
	}
	if fh.isDir {
		return arpc.Response{}, os.ErrInvalid
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

	streamCallback := func(stream *smux.Stream) {
		err := binarystream.SendData(fh.handle, payload.Length, stream)
		if err != nil && syslog.L != nil {
			syslog.L.Errorf("handleReadAt error: %v", err)
		}
	}

	return arpc.Response{
		Status:    213,
		RawStream: streamCallback,
	}, nil
}

func (s *VSSFSServer) handleLseek(req arpc.Request) (arpc.Response, error) {
	var payload types.LseekReq
	if err := payload.Decode(req.Payload); err != nil {
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
	resp := types.LseekResp{
		NewOffset: newOffset,
	}
	respBytes, err := resp.Encode()
	if err != nil {
		return arpc.Response{}, err
	}

	return arpc.Response{
		Status: 200,
		Data:   respBytes,
	}, nil
}

func (s *VSSFSServer) handleClose(req arpc.Request) (arpc.Response, error) {
	var payload types.CloseReq
	if err := payload.Decode(req.Payload); err != nil {
		return arpc.Response{}, err
	}

	handle, exists := s.handles.Get(uint64(payload.HandleID))
	if !exists {
		return arpc.Response{}, os.ErrNotExist
	}

	// Close the Windows handle directly
	windows.CloseHandle(handle.handle)
	s.handles.Del(uint64(payload.HandleID))

	closed := arpc.StringMsg("closed")
	data, err := closed.Encode()
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
