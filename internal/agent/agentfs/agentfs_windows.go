//go:build windows

package agentfs

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"unicode/utf16"
	"unsafe"

	"github.com/Microsoft/go-winio"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	binarystream "github.com/pbs-plus/pbs-plus/internal/arpc/binary"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils/pathjoin"
	"github.com/pkg/errors"
	"github.com/xtaci/smux"
	"golang.org/x/sys/windows"
)

// Win32 structures mirrored per Microsoft documentation.
// References:
// - FILE_BASIC_INFO: https://learn.microsoft.com/windows/win32/api/winbase/ns-winbase-file_basic_info
// - FILE_STANDARD_INFO: https://learn.microsoft.com/windows/win32/api/winbase/ns-winbase-file_standard_info
// - FILE_ID_INFO: https://learn.microsoft.com/windows/win32/api/winbase/ns-winbase-file_id_info
// - FILETIME (in x/sys/windows: type Filetime struct { LowDateTime, HighDateTime uint32 })

type fileBasicInfo struct {
	CreationTime   windows.Filetime // 8 bytes
	LastAccessTime windows.Filetime // 8 bytes
	LastWriteTime  windows.Filetime // 8 bytes
	ChangeTime     windows.Filetime // 8 bytes
	FileAttributes uint32           // 4 bytes
	_              uint32           // padding to 8-byte alignment (per SDK layout)
}

type fileStandardInfo struct {
	AllocationSize int64  // LARGE_INTEGER
	EndOfFile      int64  // LARGE_INTEGER
	NumberOfLinks  uint32 // DWORD
	DeletePending  byte   // BOOLEAN
	Directory      byte   // BOOLEAN
	_              [2]byte
}

type fileID128 struct {
	// FILE_ID_128: 16 bytes
	Identifier [16]byte
}

type fileIDInfo struct {
	VolumeSerialNumber uint64 // ULONGLONG
	FileId             fileID128
}

// Used for FSCTL_QUERY_ALLOCATED_RANGES
type fileAllocatedRangeBuffer struct {
	FileOffset int64
	Length     int64
}

type FileHandle struct {
	sync.Mutex
	handle    windows.Handle
	fileSize  int64
	isDir     bool
	dirReader *DirReaderNT

	// Cached mapping handle for mmap mode
	mapping windows.Handle
}

type FileStandardInfo struct {
	AllocationSize, EndOfFile int64
	NumberOfLinks             uint32
	DeletePending, Directory  bool
}

func (s *AgentFSServer) abs(filename string) (string, error) {
	windowsDir := filepath.FromSlash(filename)

	if windowsDir == "" || windowsDir == "." || windowsDir == "/" {
		return s.snapshot.Path, nil
	}

	path := pathjoin.Join(s.snapshot.Path, windowsDir)
	return path, nil
}

func (s *AgentFSServer) absUNC(filename string) (string, error) {
	windowsDir := filepath.FromSlash(filename)

	if windowsDir == "" || windowsDir == "." || windowsDir == "/" {
		return "\\\\?\\" + s.snapshot.Path, nil
	}

	path := pathjoin.Join(s.snapshot.Path, windowsDir)

	return "\\\\?\\" + path, nil
}

func (s *AgentFSServer) closeFileHandles() {
	s.handles.ForEach(func(u uint64, fh *FileHandle) bool {
		fh.Lock()
		if fh.mapping != 0 {
			windows.CloseHandle(fh.mapping)
			fh.mapping = 0
		}
		windows.CloseHandle(fh.handle)
		fh.Unlock()
		return true
	})
	s.handles.Clear()
}

func (s *AgentFSServer) initializeStatFS() error {
	var err error

	if s.snapshot.SourcePath != "" {
		driveLetter := s.snapshot.SourcePath[:1]
		s.statFs, err = getStatFS(driveLetter)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *AgentFSServer) handleStatFS(req arpc.Request) (arpc.Response, error) {
	enc, err := s.statFs.Encode()
	if err != nil {
		return arpc.Response{}, err
	}
	return arpc.Response{
		Status: 200,
		Data:   enc,
	}, nil
}

func (s *AgentFSServer) handleOpenFile(req arpc.Request) (arpc.Response, error) {
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

	path, err := s.absUNC(payload.Path)
	if err != nil {
		return arpc.Response{}, err
	}

	// Open with CreateFileW; decide flags.
	pathUTF16 := utf16.Encode([]rune(path))
	if len(pathUTF16) == 0 || pathUTF16[len(pathUTF16)-1] != 0 {
		pathUTF16 = append(pathUTF16, 0)
	}

	desiredAccess := uint32(windows.GENERIC_READ)
	share := uint32(windows.FILE_SHARE_READ | windows.FILE_SHARE_WRITE | windows.FILE_SHARE_DELETE)
	flags := uint32(windows.FILE_FLAG_BACKUP_SEMANTICS)
	// Sequential scan is a hint; keep if readMode says mmap (optional).
	if s.readMode == "mmap" {
		flags |= windows.FILE_FLAG_SEQUENTIAL_SCAN
	}

	handle, err := windows.CreateFile(
		&pathUTF16[0],
		desiredAccess,
		share,
		nil,
		windows.OPEN_EXISTING,
		flags,
		0,
	)
	if err != nil {
		return arpc.Response{}, err
	}

	// Query FileStandardInfo to get size and dir bit.
	var std fileStandardInfo
	if err := getFileStandardInfoByHandle(handle, &std); err != nil {
		windows.CloseHandle(handle)
		return arpc.Response{}, err
	}

	fh := &FileHandle{
		handle:   handle,
		fileSize: std.EndOfFile,
		isDir:    std.Directory != 0,
	}

	// If directory, keep your existing DirReaderNT
	if fh.isDir {
		dirPath, err := s.abs(payload.Path)
		if err != nil {
			windows.CloseHandle(handle)
			return arpc.Response{}, err
		}
		reader, err := NewDirReaderNT(dirPath)
		if err != nil {
			windows.CloseHandle(handle)
			return arpc.Response{}, err
		}
		fh.dirReader = reader
	}

	handleId := s.handleIdGen.NextID()
	s.handles.Set(handleId, fh)

	fhId := types.FileHandleId(handleId)
	dataBytes, err := fhId.Encode()
	if err != nil {
		if fh.dirReader != nil {
			fh.dirReader.Close()
		}
		if fh.mapping != 0 {
			windows.CloseHandle(fh.mapping)
		}
		windows.CloseHandle(fh.handle)
		return arpc.Response{}, err
	}

	return arpc.Response{
		Status: 200,
		Data:   dataBytes,
	}, nil
}

func (s *AgentFSServer) handleAttr(req arpc.Request) (arpc.Response, error) {
	var payload types.StatReq
	if err := payload.Decode(req.Payload); err != nil {
		return arpc.Response{}, err
	}

	fullPath, err := s.absUNC(payload.Path)
	if err != nil {
		return arpc.Response{}, err
	}

	// Prefer handle-based metadata (open minimal handle)
	pathUTF16 := utf16.Encode([]rune(fullPath))
	if len(pathUTF16) == 0 || pathUTF16[len(pathUTF16)-1] != 0 {
		pathUTF16 = append(pathUTF16, 0)
	}

	h, err := windows.CreateFile(
		&pathUTF16[0],
		windows.FILE_READ_ATTRIBUTES|windows.SYNCHRONIZE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		// Fallback to os.Stat
		rawInfo, e := os.Stat(fullPath)
		if e != nil {
			return arpc.Response{}, e
		}
		blocks := uint64(0)
		if !rawInfo.IsDir() {
			file, e := os.Open(fullPath)
			if e == nil {
				defer file.Close()
				var blockSize uint64
				if s.statFs != (types.StatFS{}) {
					blockSize = s.statFs.Bsize
				}
				if blockSize == 0 {
					blockSize = 4096
				}
				if standardInfo, e2 := winio.GetFileStandardInfo(file); e2 == nil {
					blocks = uint64((standardInfo.AllocationSize + int64(blockSize) - 1) / int64(blockSize))
				}
			}
		}
		info := types.AgentFileInfo{
			Name:    rawInfo.Name(),
			Size:    rawInfo.Size(),
			Mode:    uint32(rawInfo.Mode()),
			ModTime: rawInfo.ModTime(),
			IsDir:   rawInfo.IsDir(),
			Blocks:  blocks,
		}
		data, e := info.Encode()
		if e != nil {
			return arpc.Response{}, e
		}
		return arpc.Response{Status: 200, Data: data}, nil
	}
	defer windows.CloseHandle(h)

	var basic fileBasicInfo
	var std fileStandardInfo
	if err := getFileBasicInfoByHandle(h, &basic); err != nil {
		return arpc.Response{}, err
	}
	if err := getFileStandardInfoByHandle(h, &std); err != nil {
		return arpc.Response{}, err
	}

	// Compute fields
	name := filepath.Base(filepath.Clean(fullPath))
	size := std.EndOfFile
	isDir := std.Directory != 0
	mode := fileModeFromAttrs(basic.FileAttributes, isDir)
	modTime := filetimeToTime(basic.LastWriteTime)

	var blockSize uint64
	if s.statFs != (types.StatFS{}) {
		blockSize = s.statFs.Bsize
	}
	if blockSize == 0 {
		blockSize = 4096
	}
	blocks := uint64(0)
	if !isDir {
		blocks = uint64((std.AllocationSize + int64(blockSize) - 1) / int64(blockSize))
	}

	info := types.AgentFileInfo{
		Name:    name,
		Size:    size,
		Mode:    mode,
		ModTime: modTime,
		IsDir:   isDir,
		Blocks:  blocks,
	}
	data, err := info.Encode()
	if err != nil {
		return arpc.Response{}, err
	}
	return arpc.Response{Status: 200, Data: data}, nil
}

// handleXattr populates extended file statistics including Windows-specific
// creation time, last access time, group/owner and file attributes.
func (s *AgentFSServer) handleXattr(req arpc.Request) (arpc.Response, error) {
	var payload types.StatReq
	if err := payload.Decode(req.Payload); err != nil {
		return arpc.Response{}, err
	}

	fullPath, err := s.absUNC(payload.Path)
	if err != nil {
		return arpc.Response{}, err
	}

	pathUTF16 := utf16.Encode([]rune(fullPath))
	if len(pathUTF16) == 0 || pathUTF16[len(pathUTF16)-1] != 0 {
		pathUTF16 = append(pathUTF16, 0)
	}
	h, err := windows.CreateFile(
		&pathUTF16[0],
		windows.FILE_READ_ATTRIBUTES|windows.SYNCHRONIZE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		return arpc.Response{}, err
	}
	defer windows.CloseHandle(h)

	var basic fileBasicInfo
	if err := getFileBasicInfoByHandle(h, &basic); err != nil {
		return arpc.Response{}, err
	}

	creationTime := filetimeToTime(basic.CreationTime)
	lastAccessTime := filetimeToTime(basic.LastAccessTime)
	lastWriteTime := filetimeToTime(basic.LastWriteTime)
	fileAttributes := parseFileAttributes(basic.FileAttributes)

	// Retrieve owner, group, and ACL info via handle (fallback to path if needed).
	owner, group, acls, err := GetWinACLsHandle(h)
	if err != nil {
		return arpc.Response{}, err
	}

	info := types.AgentFileInfo{
		Name:           fullPath,
		CreationTime:   creationTime.UnixNano(),
		LastAccessTime: lastAccessTime.UnixNano(),
		LastWriteTime:  lastWriteTime.UnixNano(),
		FileAttributes: fileAttributes,
		Owner:          owner,
		Group:          group,
		WinACLs:        acls,
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
func (s *AgentFSServer) handleReadDir(req arpc.Request) (arpc.Response, error) {
	var payload types.ReadDirReq
	if err := payload.Decode(req.Payload); err != nil {
		return arpc.Response{}, err
	}

	fh, exists := s.handles.Get(uint64(payload.HandleID))
	if !exists {
		return arpc.Response{}, os.ErrNotExist
	}

	fh.Lock()
	defer fh.Unlock()

	encodedBatch, err := fh.dirReader.NextBatch()
	if err != nil {
		if !errors.Is(err, os.ErrProcessDone) {
			syslog.L.Error(err).WithMessage("error reading batch").Write()
		}
		return arpc.Response{}, err
	}

	byteReader := bytes.NewReader(encodedBatch)
	streamCallback := func(stream *smux.Stream) {
		if err := binarystream.SendDataFromReader(byteReader, int(len(encodedBatch)), stream); err != nil {
			syslog.L.Error(err).WithMessage("failed sending data from reader via binary stream").Write()
		}
	}

	return arpc.Response{
		Status:    213,
		RawStream: streamCallback,
	}, nil
}

// handleReadAt now duplicates the file handle, opens a backup reading session,
// and then uses backupSeek to skip to the desired offset without copying bytes.
func (s *AgentFSServer) handleReadAt(req arpc.Request) (arpc.Response, error) {
	var payload types.ReadAtReq
	if err := payload.Decode(req.Payload); err != nil {
		return arpc.Response{}, err
	}

	// Validate the payload parameters.
	if payload.Length < 0 {
		return arpc.Response{}, fmt.Errorf("invalid negative length requested: %d", payload.Length)
	}

	// Retrieve the file handle.
	fh, exists := s.handles.Get(uint64(payload.HandleID))
	if !exists {
		return arpc.Response{}, os.ErrNotExist
	}
	if fh.isDir {
		return arpc.Response{}, os.ErrInvalid
	}

	fh.Lock()
	defer fh.Unlock()

	// If the requested offset is at or beyond EOF, stream nothing.
	if payload.Offset >= fh.fileSize {
		emptyReader := bytes.NewReader([]byte{})
		streamCallback := func(stream *smux.Stream) {
			if err := binarystream.SendDataFromReader(emptyReader, payload.Length, stream); err != nil {
				syslog.L.Error(err).
					WithMessage("failed sending empty reader via binary stream").Write()
			}
		}
		return arpc.Response{
			Status:    213,
			RawStream: streamCallback,
		}, nil
	}

	// Clamp length if the requested region goes beyond EOF.
	if payload.Offset+int64(payload.Length) > fh.fileSize {
		payload.Length = int(fh.fileSize - payload.Offset)
	}

	if s.readMode == "mmap" {
		// Create mapping once and reuse
		if fh.mapping == 0 {
			mh, err := windows.CreateFileMapping(fh.handle, nil, windows.PAGE_READONLY, 0, 0, nil)
			if err == nil {
				fh.mapping = mh
			}
		}
		if fh.mapping != 0 {
			alignedOffset := payload.Offset - (payload.Offset % int64(s.allocGranularity))
			offsetDiff := int(payload.Offset - alignedOffset)
			viewSize := uintptr(payload.Length + offsetDiff)

			addr, err := windows.MapViewOfFile(
				fh.mapping,
				windows.FILE_MAP_READ,
				uint32(uint64(alignedOffset)>>32),
				uint32(uint64(alignedOffset)&0xFFFFFFFF),
				viewSize,
			)
			if err == nil {
				ptr := (*byte)(unsafe.Pointer(addr))
				data := unsafe.Slice(ptr, viewSize)
				if offsetDiff+payload.Length > len(data) {
					syslog.L.Error(fmt.Errorf(
						"invalid slice bounds: offsetDiff=%d, payload.Length=%d, data len=%d",
						offsetDiff, payload.Length, len(data)),
					).WithMessage("invalid file mapping boundaries").Write()

					windows.UnmapViewOfFile(addr)
					return arpc.Response{}, fmt.Errorf("invalid file mapping boundaries")
				}
				result := data[offsetDiff : offsetDiff+payload.Length]
				reader := bytes.NewReader(result)

				streamCallback := func(stream *smux.Stream) {
					defer windows.UnmapViewOfFile(addr)
					if err := binarystream.SendDataFromReader(reader, payload.Length, stream); err != nil {
						syslog.L.Error(err).WithMessage("failed sending data from reader via binary stream").Write()
					}
				}

				return arpc.Response{
					Status:    213,
					RawStream: streamCallback,
				}, nil
			}
			// fall through if mapping fails
		}
	}

	lowOffset := int32(payload.Offset & 0xFFFFFFFF)
	highOffset := int32(payload.Offset >> 32)
	_, err := windows.SetFilePointer(fh.handle, lowOffset, &highOffset, windows.FILE_BEGIN)
	if err != nil {
		return arpc.Response{}, mapWinError(err, "handleReadAt Seek (sync fallback)")
	}

	buffer := make([]byte, payload.Length)
	var bytesRead uint32

	err = windows.ReadFile(fh.handle, buffer, &bytesRead, nil)
	if err != nil {
		return arpc.Response{}, mapWinError(err, "handleReadAt ReadFile (sync fallback)")
	}

	reader := bytes.NewReader(buffer[:bytesRead])
	streamCallback := func(stream *smux.Stream) {
		if err := binarystream.SendDataFromReader(reader, int(bytesRead), stream); err != nil {
			syslog.L.Error(err).
				WithMessage("failed sending data from reader via binary stream (sync fallback)").
				Write()
		}
	}

	return arpc.Response{
		Status:    213,
		RawStream: streamCallback,
	}, nil
}

func (s *AgentFSServer) handleLseek(req arpc.Request) (arpc.Response, error) {
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

	fh.Lock()
	defer fh.Unlock()

	// Query the file size via handle
	var std fileStandardInfo
	if err := getFileStandardInfoByHandle(fh.handle, &std); err != nil {
		return arpc.Response{}, err
	}
	fileSize := std.EndOfFile

	var newOffset int64

	// Handle sparse file operations
	if payload.Whence == SeekData || payload.Whence == SeekHole {
		no, err := sparseSeekAllocatedRanges(fh.handle, payload.Offset, payload.Whence, fileSize)
		if err != nil {
			return arpc.Response{}, err
		}
		newOffset = no
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
	_, err := windows.SetFilePointer(fh.handle, int32(newOffset), nil, windows.FILE_BEGIN)
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

func (s *AgentFSServer) handleClose(req arpc.Request) (arpc.Response, error) {
	var payload types.CloseReq
	if err := payload.Decode(req.Payload); err != nil {
		return arpc.Response{}, err
	}

	handle, exists := s.handles.Get(uint64(payload.HandleID))
	if !exists {
		return arpc.Response{}, os.ErrNotExist
	}

	handle.Lock()
	defer handle.Unlock()

	// Close the Windows handle directly
	if handle.mapping != 0 {
		windows.CloseHandle(handle.mapping)
		handle.mapping = 0
	}
	if !handle.isDir {
		windows.CloseHandle(handle.handle)
	} else {
		handle.dirReader.Close()
		windows.CloseHandle(handle.handle)
	}

	s.handles.Del(uint64(payload.HandleID))

	closed := arpc.StringMsg("closed")
	data, err := closed.Encode()
	if err != nil {
		return arpc.Response{}, err
	}

	return arpc.Response{Status: 200, Data: data}, nil
}
