//go:build windows

package vssfs

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"unsafe"

	"github.com/Microsoft/go-winio"
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
	handle   windows.Handle
	fileSize int64
	isDir    bool
}

type FileStandardInfo struct {
	AllocationSize, EndOfFile int64
	NumberOfLinks             uint32
	DeletePending, Directory  bool
}

type VSSFSServer struct {
	ctx              context.Context
	ctxCancel        context.CancelFunc
	jobId            string
	snapshot         snapshots.WinVSSSnapshot
	handleIdGen      *idgen.IDGenerator
	handles          *safemap.Map[uint64, *FileHandle]
	arpcRouter       *arpc.Router
	statFs           types.StatFS
	allocGranularity uint32
}

func NewVSSFSServer(jobId string, snapshot snapshots.WinVSSSnapshot) *VSSFSServer {
	ctx, cancel := context.WithCancel(context.Background())

	allocGranularity := GetAllocGranularity()
	if allocGranularity == 0 {
		allocGranularity = 65536 // 64 KB usually
	}

	s := &VSSFSServer{
		snapshot:         snapshot,
		jobId:            jobId,
		handles:          safemap.New[uint64, *FileHandle](),
		ctx:              ctx,
		ctxCancel:        cancel,
		handleIdGen:      idgen.NewIDGenerator(),
		allocGranularity: uint32(allocGranularity),
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

	s.handles.ForEach(func(u uint64, fh *FileHandle) bool {
		windows.CloseHandle(fh.handle)

		return true
	})

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
	var err error // Named return value for error
	defer func() {
		if r := recover(); r != nil {
			// Log the panic
			if syslog.L != nil {
				syslog.L.Errorf("handleOpenFile panic: %v", r)
			}
			// Set the error to indicate a panic occurred
			err = os.ErrInvalid
		}
	}()

	var payload types.OpenFileReq
	if err := payload.Decode(req.Payload); err != nil {
		return arpc.Response{}, err
	}

	// Disallow write operations.
	if payload.Flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC) != 0 {
		errStr := arpc.StringMsg("write operations not allowed")
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
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_SEQUENTIAL_SCAN|windows.FILE_FLAG_OVERLAPPED,
		0,
	)
	if err != nil {
		return arpc.Response{}, err
	}

	fileSize, err := getFileSize(handle)
	if err != nil {
		windows.CloseHandle(handle)
		return arpc.Response{}, err
	}

	handleId := s.handleIdGen.NextID()
	fh := &FileHandle{
		handle:   handle,
		fileSize: fileSize,
		isDir:    stat.IsDir(),
	}
	s.handles.Set(handleId, fh)

	// Return the handle ID to the client.
	fhId := types.FileHandleId(handleId)
	dataBytes, err := fhId.Encode()
	if err != nil {
		windows.CloseHandle(handle)
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

	rawInfo, err := os.Stat(fullPath)
	if err != nil {
		return arpc.Response{}, err
	}

	// Only check for sparse file attributes and block allocation if it's a regular file
	blocks := uint64(0)
	if !rawInfo.IsDir() {
		file, err := os.Open(fullPath)
		if err != nil {
			return arpc.Response{}, err
		}
		defer file.Close()

		var blockSize uint64
		if s.statFs != (types.StatFS{}) {
			blockSize = s.statFs.Bsize
		}
		if blockSize == 0 {
			blockSize = 4096 // Default to 4KB block size
		}

		standardInfo, err := winio.GetFileStandardInfo(file)
		if err == nil {
			blocks = uint64((standardInfo.AllocationSize + int64(blockSize) - 1) / int64(blockSize))
		}
	}

	info := types.VSSFileInfo{
		Name:    rawInfo.Name(),
		Size:    rawInfo.Size(),
		Mode:    uint32(rawInfo.Mode()),
		ModTime: rawInfo.ModTime(),
		IsDir:   rawInfo.IsDir(),
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
	var err error // Named return value for error
	defer func() {
		if r := recover(); r != nil {
			// Log the panic
			if syslog.L != nil {
				syslog.L.Errorf("handleReadDir panic: %v", r)
			}
			// Set the error to indicate a panic occurred
			err = os.ErrInvalid
		}
	}()

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

	entries, err := readDirBulk(fullDirPath)
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
	var err error // Named return value for error
	defer func() {
		if r := recover(); r != nil {
			// Log the panic
			if syslog.L != nil {
				syslog.L.Errorf("handleReadDir panic: %v", r)
			}
			// Set the error to indicate a panic occurred
			err = os.ErrInvalid
		}
	}()

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

	// If the requested offset is at or beyond EOF, return an empty result.
	if payload.Offset >= fh.fileSize {
		streamCallback := func(stream *smux.Stream) {
			err := binarystream.SendDataFromReader(nil, payload.Length, stream)
			if err != nil && syslog.L != nil {
				syslog.L.Errorf("handleReadAt error: %v", err)
			}
		}
		return arpc.Response{
			Status:    213,
			RawStream: streamCallback,
		}, nil
	}

	// Clamp length if the requested region goes beyond EOF
	if payload.Offset+int64(payload.Length) > fh.fileSize {
		payload.Length = int(fh.fileSize - payload.Offset)
	}

	// Align the offset down to the nearest multiple of the page size.
	alignedOffset := payload.Offset - (payload.Offset % int64(s.allocGranularity))
	offsetDiff := int(payload.Offset - alignedOffset)

	// Calculate the view size by adding the difference.
	viewSize := uintptr(payload.Length + offsetDiff)

	// Attempt to create a file mapping.
	h, err := windows.CreateFileMapping(fh.handle, nil, windows.PAGE_READONLY, 0, 0, nil)
	if err == nil {
		// Map the requested view.
		addr, err := windows.MapViewOfFile(h, windows.FILE_MAP_READ, uint32(alignedOffset>>32), uint32(alignedOffset&0xFFFFFFFF), viewSize)
		if err == nil {
			// Successfully mapped the view.
			ptr := (*byte)(unsafe.Pointer(addr))
			data := unsafe.Slice(ptr, viewSize)
			result := data[offsetDiff : offsetDiff+int(payload.Length)]

			reader := bytes.NewReader(result)

			streamCallback := func(stream *smux.Stream) {
				defer func() {
					windows.UnmapViewOfFile(addr)
					windows.CloseHandle(h)
					data = nil
				}()

				err := binarystream.SendDataFromReader(reader, payload.Length, stream)
				if err != nil && syslog.L != nil {
					syslog.L.Errorf("handleReadAt error: %v", err)
				}
			}

			return arpc.Response{
				Status:    213,
				RawStream: streamCallback,
			}, nil
		}

		// Cleanup if mapping fails.
		windows.CloseHandle(h)
	}

	// Fallback to OVERLAPPED method if file mapping fails.
	var overlapped windows.Overlapped
	overlapped.Offset = uint32(payload.Offset & 0xFFFFFFFF)
	overlapped.OffsetHigh = uint32(payload.Offset >> 32)

	buffer := make([]byte, payload.Length)
	var bytesRead uint32

	err = windows.ReadFile(fh.handle, buffer, &bytesRead, &overlapped)
	if err != nil {
		return arpc.Response{}, mapWinError(err, "handleReadAt ReadFile (OVERLAPPED fallback)")
	}

	reader := bytes.NewReader(buffer[:bytesRead])

	streamCallback := func(stream *smux.Stream) {
		err := binarystream.SendDataFromReader(reader, int(bytesRead), stream)
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
				return arpc.Response{}, mapWinError(err, "handleLseek SetFilePointer (FILE_CURRENT)")
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
		return arpc.Response{}, os.ErrInvalid
	}

	// Set the new position
	_, err = windows.SetFilePointer(fh.handle, int32(newOffset), nil, windows.FILE_BEGIN)
	if err != nil {
		return arpc.Response{}, mapWinError(err, "handleLseek SetFilePointer (FILE_BEGIN)")
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
