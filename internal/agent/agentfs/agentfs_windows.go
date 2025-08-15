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
// - FILE_STANDARD_INFO: https://learn.microsoft.com/windows/win32/api/winbase/ns-winbase-file_standard_info
// - FILE_ID_INFO: https://learn.microsoft.com/windows/win32/api/winbase/ns-winbase-file_id_info
// - FILETIME (in x/sys/windows: type Filetime struct { LowDateTime, HighDateTime uint32 })

type fileStandardInfo struct {
	AllocationSize int64  // LARGE_INTEGER
	EndOfFile      int64  // LARGE_INTEGER
	NumberOfLinks  uint32 // DWORD
	DeletePending  byte   // BOOLEAN
	Directory      byte   // BOOLEAN
	_              [2]byte
}

// Used for FSCTL_QUERY_ALLOCATED_RANGES
type fileAllocatedRangeBuffer struct {
	FileOffset int64
	Length     int64
}

type FileHandle struct {
	sync.Mutex
	handle        windows.Handle
	fileSize      int64
	isDir         bool
	dirReader     *DirReaderNT
	mapping       windows.Handle
	logicalOffset int64
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
	share := uint32(
		windows.FILE_SHARE_READ |
			windows.FILE_SHARE_WRITE |
			windows.FILE_SHARE_DELETE,
	)
	flags := uint32(windows.FILE_FLAG_BACKUP_SEMANTICS | windows.FILE_FLAG_OVERLAPPED)
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
		handle:        handle,
		fileSize:      std.EndOfFile,
		isDir:         std.Directory != 0,
		logicalOffset: 0,
	}

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

	pathUTF16 := utf16.Encode([]rune(fullPath))
	if len(pathUTF16) == 0 || pathUTF16[len(pathUTF16)-1] != 0 {
		pathUTF16 = append(pathUTF16, 0)
	}

	h, err := windows.CreateFile(
		&pathUTF16[0],
		windows.READ_CONTROL|windows.FILE_READ_ATTRIBUTES|windows.SYNCHRONIZE,
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

	var bhi windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(h, &bhi); err != nil {
		return arpc.Response{}, err
	}

	isDir := (bhi.FileAttributes & windows.FILE_ATTRIBUTE_DIRECTORY) != 0
	size := int64(bhi.FileSizeHigh)<<32 | int64(bhi.FileSizeLow)
	modTime := filetimeToTime(windows.Filetime(bhi.LastWriteTime))
	mode := fileModeFromAttrs(bhi.FileAttributes, isDir)
	name := filepath.Base(filepath.Clean(fullPath))

	var std fileStandardInfo
	if err := getFileStandardInfoByHandle(h, &std); err != nil {
		return arpc.Response{}, err
	}

	blockSize := s.statFs.Bsize
	if blockSize == 0 {
		blockSize = 4096
	}

	var blocks uint64
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
		windows.READ_CONTROL|windows.FILE_READ_ATTRIBUTES|windows.SYNCHRONIZE,
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

	var bhi windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(h, &bhi); err != nil {
		return arpc.Response{}, err
	}

	creationTime := filetimeToTime(windows.Filetime(bhi.CreationTime)).UnixNano()
	lastAccessTime := filetimeToTime(windows.Filetime(bhi.LastAccessTime)).UnixNano()
	lastWriteTime := filetimeToTime(windows.Filetime(bhi.LastWriteTime)).UnixNano()
	fileAttributes := parseFileAttributes(bhi.FileAttributes)

	owner, group, acls, err := GetWinACLsHandle(h)
	if err != nil {
		return arpc.Response{}, err
	}

	info := types.AgentFileInfo{
		Name:           fullPath,
		CreationTime:   creationTime,
		LastAccessTime: lastAccessTime,
		LastWriteTime:  lastWriteTime,
		FileAttributes: fileAttributes,
		Owner:          owner,
		Group:          group,
		WinACLs:        acls,
	}
	data, err := info.Encode()
	if err != nil {
		return arpc.Response{}, err
	}
	return arpc.Response{Status: 200, Data: data}, nil
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

// handleReadAt: mmap fast-path; OVERLAPPED fallback. No SetFilePointer.
func (s *AgentFSServer) handleReadAt(req arpc.Request) (arpc.Response, error) {
	var payload types.ReadAtReq
	if err := payload.Decode(req.Payload); err != nil {
		return arpc.Response{}, err
	}
	if payload.Length < 0 {
		return arpc.Response{}, fmt.Errorf("invalid negative length requested: %d", payload.Length)
	}

	fh, exists := s.handles.Get(uint64(payload.HandleID))
	if !exists {
		return arpc.Response{}, os.ErrNotExist
	}
	if fh.isDir {
		return arpc.Response{}, os.ErrInvalid
	}

	// Snapshot mapping handle without holding lock during I/O
	fh.Lock()
	mapping := fh.mapping
	fh.Unlock()

	// Refresh EOF from the handle to avoid stale size issues.
	var std fileStandardInfo
	if err := getFileStandardInfoByHandle(fh.handle, &std); err != nil {
		return arpc.Response{}, err
	}
	fileSize := std.EndOfFile

	if payload.Offset >= fileSize {
		emptyReader := bytes.NewReader(nil)
		return arpc.Response{
			Status: 213,
			RawStream: func(stream *smux.Stream) {
				_ = binarystream.SendDataFromReader(emptyReader, 0, stream)
			},
		}, nil
	}

	// Clamp requested length to current EOF
	reqLen := payload.Length
	if payload.Offset+int64(reqLen) > fileSize {
		reqLen = int(fileSize - payload.Offset)
	}
	if reqLen == 0 {
		emptyReader := bytes.NewReader(nil)
		return arpc.Response{
			Status: 213,
			RawStream: func(stream *smux.Stream) {
				_ = binarystream.SendDataFromReader(emptyReader, 0, stream)
			},
		}, nil
	}

	// mmap fast path: ensure we don't map past EOF.
	if s.readMode == "mmap" {
		// Ensure we have/create mapping
		var mh windows.Handle
		if mapping == 0 {
			fh.Lock()
			if fh.mapping == 0 {
				if m, err := windows.CreateFileMapping(
					fh.handle,
					nil,
					windows.PAGE_READONLY,
					0,
					0,
					nil,
				); err == nil {
					fh.mapping = m
					mh = m
				}
			} else {
				mh = fh.mapping
			}
			fh.Unlock()
		} else {
			mh = mapping
		}

		if mh != 0 {
			alloc := int64(s.allocGranularity)
			alignedOffset := payload.Offset - (payload.Offset % alloc)
			offsetDiff := int(payload.Offset - alignedOffset)

			// Compute maximum view limited by EOF
			maxAtEOF := fileSize - alignedOffset
			if maxAtEOF < 0 {
				maxAtEOF = 0
			}
			// We need at most reqLen+offsetDiff bytes in the view,
			// but cannot exceed maxAtEOF.
			maxView := int64(reqLen + offsetDiff)
			if maxView > maxAtEOF {
				maxView = maxAtEOF
			}
			if maxView <= int64(offsetDiff) {
				// Nothing readable in this region
				emptyReader := bytes.NewReader(nil)
				return arpc.Response{
					Status: 213,
					RawStream: func(stream *smux.Stream) {
						_ = binarystream.SendDataFromReader(emptyReader, 0, stream)
					},
				}, nil
			}

			viewSize := uintptr(maxView)
			addr, err := windows.MapViewOfFile(
				mh,
				windows.FILE_MAP_READ,
				uint32(uint64(alignedOffset)>>32),
				uint32(uint64(alignedOffset)&0xFFFFFFFF),
				viewSize,
			)
			if err == nil {
				ptr := (*byte)(unsafe.Pointer(addr))
				data := unsafe.Slice(ptr, viewSize)

				// Actual readable length from requested offset
				avail := len(data) - offsetDiff
				if avail < 0 {
					avail = 0
				}
				resultLen := reqLen
				if resultLen > avail {
					resultLen = avail
				}
				if resultLen < 0 {
					resultLen = 0
				}

				reader := bytes.NewReader(data[offsetDiff : offsetDiff+resultLen])
				return arpc.Response{
					Status: 213,
					RawStream: func(stream *smux.Stream) {
						defer windows.UnmapViewOfFile(addr)
						if err := binarystream.SendDataFromReader(reader, resultLen, stream); err != nil {
							syslog.L.Error(err).
								WithMessage("failed sending data from reader via binary stream").
								Write()
						}
					},
				}, nil
			}
			// fall through to overlapped if mapping fails
		}
	}

	// OVERLAPPED fallback: read up to reqLen bytes.
	buf := make([]byte, reqLen)
	ov := new(windows.Overlapped)
	ov.Offset = uint32(payload.Offset & 0xffffffff)
	ov.OffsetHigh = uint32(uint64(payload.Offset) >> 32)

	var n uint32
	err := windows.ReadFile(fh.handle, buf, &n, ov)
	if err != nil && err != windows.ERROR_IO_PENDING {
		return arpc.Response{}, mapWinError(err, "handleReadAt ReadFile overlapped")
	}
	if err == windows.ERROR_IO_PENDING {
		if err = windows.GetOverlappedResult(fh.handle, ov, &n, true); err != nil {
			return arpc.Response{}, mapWinError(err, "handleReadAt GetOverlappedResult")
		}
	}

	// Stream exactly the bytes we actually read.
	reader := bytes.NewReader(buf[:n])
	return arpc.Response{
		Status: 213,
		RawStream: func(stream *smux.Stream) {
			if err := binarystream.SendDataFromReader(reader, int(n), stream); err != nil {
				syslog.L.Error(err).
					WithMessage("failed sending data from reader via binary stream (overlapped)").
					Write()
			}
		},
	}, nil
}

func (s *AgentFSServer) handleLseek(req arpc.Request) (arpc.Response, error) {
	var payload types.LseekReq
	if err := payload.Decode(req.Payload); err != nil {
		return arpc.Response{}, err
	}

	if payload.Whence != io.SeekStart &&
		payload.Whence != io.SeekCurrent &&
		payload.Whence != io.SeekEnd &&
		payload.Whence != SeekData &&
		payload.Whence != SeekHole {
		return arpc.Response{}, os.ErrInvalid
	}

	fh, exists := s.handles.Get(uint64(payload.HandleID))
	if !exists {
		return arpc.Response{}, os.ErrNotExist
	}
	if fh.isDir {
		return arpc.Response{}, os.ErrInvalid
	}

	// We will not touch the kernel file pointer; we only update fh.logicalOffset.
	fh.Lock()
	defer fh.Unlock()

	var std fileStandardInfo
	if err := getFileStandardInfoByHandle(fh.handle, &std); err != nil {
		return arpc.Response{}, err
	}
	fileSize := std.EndOfFile

	cur := fh.logicalOffset
	var newOffset int64

	switch payload.Whence {
	case io.SeekStart:
		if payload.Offset < 0 {
			return arpc.Response{}, os.ErrInvalid
		}
		newOffset = payload.Offset

	case io.SeekCurrent:
		if cur+payload.Offset < 0 {
			return arpc.Response{}, os.ErrInvalid
		}
		newOffset = cur + payload.Offset

	case io.SeekEnd:
		if fileSize+payload.Offset < 0 {
			return arpc.Response{}, os.ErrInvalid
		}
		newOffset = fileSize + payload.Offset

	case SeekData, SeekHole:
		no, err := sparseSeekAllocatedRanges(
			fh.handle,
			payload.Offset,
			payload.Whence,
			fileSize,
		)
		if err != nil {
			return arpc.Response{}, err
		}
		newOffset = no

	default:
		return arpc.Response{}, os.ErrInvalid
	}

	if newOffset > fileSize {
		return arpc.Response{}, os.ErrInvalid
	}

	fh.logicalOffset = newOffset

	resp := types.LseekResp{NewOffset: newOffset}
	respBytes, err := resp.Encode()
	if err != nil {
		return arpc.Response{}, err
	}
	return arpc.Response{Status: 200, Data: respBytes}, nil
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
