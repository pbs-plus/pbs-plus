//go:build windows

package vssfs

import (
	"context"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/alphadose/haxmap"
	securejoin "github.com/cyphar/filepath-securejoin"
	"github.com/rekby/fastuuid"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
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
	snapshot        *snapshots.WinVSSSnapshot
	handles         *haxmap.Map[string, *FileHandle]
	readAtStatCache *haxmap.Map[string, *windows.ByHandleFileInformation]
	arpcRouter      *arpc.Router
	bufferPool      *utils.BufferPool
	statFs          *StatFS
	statFsBytes     []byte
}

func NewVSSFSServer(jobId string, snapshot *snapshots.WinVSSSnapshot) *VSSFSServer {
	ctx, cancel := context.WithCancel(context.Background())

	s := &VSSFSServer{
		snapshot:        snapshot,
		jobId:           jobId,
		handles:         hashmap.New[*FileHandle](),
		readAtStatCache: hashmap.New[*windows.ByHandleFileInformation](),
		ctx:             ctx,
		ctxCancel:       cancel,
		bufferPool:      utils.NewBufferPool(),
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
		r.CloseHandle(s.jobId + "/FSstat")
	}

	s.handles.Clear()
	s.readAtStatCache.Clear()

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

func (s *VSSFSServer) handleStatFS(req arpc.Request) (*arpc.Response, error) {
	return &arpc.Response{
		Status: 200,
		Data:   s.statFsBytes,
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

	// Only check for sparse file attributes and block allocation if it's a regular file
	if !rawInfo.IsDir() {
		handle, err := windows.CreateFile(
			windows.StringToUTF16Ptr(fullPath),
			windows.GENERIC_READ,
			windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
			nil,
			windows.OPEN_EXISTING,
			windows.FILE_ATTRIBUTE_NORMAL,
			0,
		)
		if err == nil {
			defer windows.CloseHandle(handle)

			var blockSize uint64
			if s.statFs != nil {
				blockSize = s.statFs.Bsize
			}

			if blockSize == 0 {
				blockSize = 4096 // Default to 4KB block size
			}

			standardInfo, err := getFileStandardInfo(handle)
			if err == nil {
				info.Blocks = uint64((standardInfo.AllocationSize + int64(blockSize) - 1) / int64(blockSize))
			}
		}
	} else {
		info.Blocks = 0
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
		fullDirPath = s.snapshot.SnapshotPath
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

	newPos, err := windows.Seek(fh.handle, distanceToMove, int(moveMethod))
	if err != nil {
		s.bufferPool.Put(buf)
		return nil, mapWinError(err)
	}
	if newPos != payload.Offset {
		s.bufferPool.Put(buf)
		return nil, os.ErrInvalid
	}

	var bytesRead uint32
	err = windows.ReadFile(fh.handle, buf, &bytesRead, nil)
	if err != nil {
		s.bufferPool.Put(buf)
		return nil, mapWinError(err)
	}

	buf = buf[:bytesRead]

	streamCallback := func(stream *smux.Stream) {
		defer s.bufferPool.Put(buf)

		if stream == nil {
			return
		}

		// Write length prefix
		length := uint32(len(buf))
		if err := binary.Write(stream, binary.LittleEndian, length); err != nil {
			return
		}

		// Write data
		stream.Write(buf)
	}

	return &arpc.Response{
		Status:    213,
		RawStream: streamCallback,
	}, nil
}

func (s *VSSFSServer) handleLseek(req arpc.Request) (*arpc.Response, error) {
	var payload LseekReq
	if _, err := payload.UnmarshalMsg(req.Payload); err != nil {
		return nil, err
	}

	// Validate whence
	if payload.Whence != io.SeekStart &&
		payload.Whence != io.SeekCurrent &&
		payload.Whence != io.SeekEnd &&
		payload.Whence != SeekData &&
		payload.Whence != SeekHole {
		return nil, os.ErrInvalid
	}

	// Retrieve the file handle
	fh, exists := s.handles.Get(string(payload.HandleID))
	if !exists {
		return nil, os.ErrNotExist
	}
	if fh.isDir {
		return nil, os.ErrInvalid
	}

	// Query the file size
	fileSize, err := getFileSize(fh.handle)
	if err != nil {
		return nil, err
	}

	var newOffset int64

	// Handle sparse file operations
	if payload.Whence == SeekData || payload.Whence == SeekHole {
		// Check if offset is beyond EOF
		if payload.Offset >= fileSize {
			return nil, syscall.ENXIO
		}

		// Query allocated ranges
		ranges, err := queryAllocatedRanges(fh.handle, fileSize)
		if err != nil {
			return nil, err
		}

		if payload.Whence == SeekData {
			// Find the next data region
			found := false
			for _, r := range ranges {
				if payload.Offset >= r.FileOffset && payload.Offset < r.FileOffset+r.Length {
					// Already in data
					newOffset = payload.Offset
					found = true
					break
				}
				if payload.Offset < r.FileOffset {
					// Found next data
					newOffset = r.FileOffset
					found = true
					break
				}
			}
			if !found {
				return nil, syscall.ENXIO
			}
		} else { // SeekHole
			// Find the next hole
			found := false
			for i, r := range ranges {
				if payload.Offset < r.FileOffset {
					// Already in a hole
					newOffset = payload.Offset
					found = true
					break
				}
				if payload.Offset >= r.FileOffset && payload.Offset < r.FileOffset+r.Length {
					// In data, seek to the end of this region
					newOffset = r.FileOffset + r.Length
					found = true
					break
				}
				// Check if there's a gap between this range and the next
				if i < len(ranges)-1 && r.FileOffset+r.Length < ranges[i+1].FileOffset {
					if payload.Offset < ranges[i+1].FileOffset {
						newOffset = r.FileOffset + r.Length
						found = true
						break
					}
				}
			}
			if !found {
				// After all ranges, everything to EOF is a hole
				newOffset = payload.Offset
			}
		}
	} else {
		// Handle standard seek operations
		switch payload.Whence {
		case io.SeekStart:
			if payload.Offset < 0 {
				return nil, os.ErrInvalid
			}
			newOffset = payload.Offset

		case io.SeekCurrent:
			currentPos, err := windows.SetFilePointer(fh.handle, 0, nil, windows.FILE_CURRENT)
			if err != nil {
				return nil, err
			}
			newOffset = int64(currentPos) + payload.Offset
			if newOffset < 0 {
				return nil, os.ErrInvalid
			}

		case io.SeekEnd:
			newOffset = fileSize + payload.Offset
			if newOffset < 0 {
				return nil, os.ErrInvalid
			}
		}
	}

	// Set the new position
	_, err = windows.SetFilePointer(fh.handle, int32(newOffset), nil, windows.FILE_BEGIN)
	if err != nil {
		return nil, err
	}

	// Prepare the response
	resp := LseekResp{
		NewOffset: newOffset,
	}
	respBytes, err := resp.MarshalMsg(nil)
	if err != nil {
		return nil, err
	}

	return &arpc.Response{
		Status: 200,
		Data:   respBytes,
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
		return s.snapshot.SnapshotPath, nil
	}
	path, err := securejoin.SecureJoin(s.snapshot.SnapshotPath, filename)
	if err != nil {
		return "", err
	}
	return path, nil
}
