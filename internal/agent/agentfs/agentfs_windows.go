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

func openForAttrs(path string) (windows.Handle, error) {
	pathUTF16 := utf16.Encode([]rune(path))
	if len(pathUTF16) == 0 || pathUTF16[len(pathUTF16)-1] != 0 {
		pathUTF16 = append(pathUTF16, 0)
	}
	return windows.CreateFile(
		&pathUTF16[0],
		windows.READ_CONTROL|windows.FILE_READ_ATTRIBUTES|windows.SYNCHRONIZE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
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

	h, err := openForAttrs(fullPath)
	if err != nil {
		return arpc.Response{}, err
	}
	defer windows.CloseHandle(h)

	var all fileAllInformation
	if err := ntQueryFileAllInformation(h, &all); err != nil {
		return arpc.Response{}, err
	}

	isDir := all.StandardInformation.Directory != 0
	size := all.StandardInformation.EndOfFile

	// Convert LastWriteTime
	modTime := filetimeToTime(windows.Filetime{
		LowDateTime:  uint32(uint64(all.BasicInformation.LastWriteTime) & 0xFFFFFFFF),
		HighDateTime: uint32(uint64(all.BasicInformation.LastWriteTime) >> 32),
	})

	mode := fileModeFromAttrs(all.BasicInformation.FileAttributes, isDir)
	name := filepath.Base(filepath.Clean(fullPath))

	blockSize := s.statFs.Bsize
	if blockSize == 0 {
		blockSize = 4096
	}

	var blocks uint64
	if !isDir {
		alloc := all.StandardInformation.AllocationSize
		if alloc < 0 {
			alloc = 0
		}
		blocks = uint64((alloc + int64(blockSize) - 1) / int64(blockSize))
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

	h, err := openForAttrs(fullPath)
	if err != nil {
		return arpc.Response{}, err
	}
	defer windows.CloseHandle(h)

	var all fileAllInformation
	if err := ntQueryFileAllInformation(h, &all); err != nil {
		return arpc.Response{}, err
	}

	creationTime := filetimeToTime(windows.Filetime{
		LowDateTime:  uint32(uint64(all.BasicInformation.CreationTime) & 0xFFFFFFFF),
		HighDateTime: uint32(uint64(all.BasicInformation.CreationTime) >> 32),
	}).UnixNano()
	lastAccessTime := filetimeToTime(windows.Filetime{
		LowDateTime:  uint32(uint64(all.BasicInformation.LastAccessTime) & 0xFFFFFFFF),
		HighDateTime: uint32(uint64(all.BasicInformation.LastAccessTime) >> 32),
	}).UnixNano()
	lastWriteTime := filetimeToTime(windows.Filetime{
		LowDateTime:  uint32(uint64(all.BasicInformation.LastWriteTime) & 0xFFFFFFFF),
		HighDateTime: uint32(uint64(all.BasicInformation.LastWriteTime) >> 32),
	}).UnixNano()

	fileAttributes := parseFileAttributes(all.BasicInformation.FileAttributes)

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

	fh.Lock()
	defer fh.Unlock()

	if payload.Offset >= fh.fileSize {
		// Nothing to send.
		emptyReader := bytes.NewReader(nil)
		return arpc.Response{
			Status: 213,
			RawStream: func(stream *smux.Stream) {
				if err := binarystream.SendDataFromReader(emptyReader, 0, stream); err != nil {
					syslog.L.Error(err).
						WithMessage("failed sending empty reader via binary stream").Write()
				}
			},
		}, nil
	}

	// Clamp to EOF.
	maxEnd := fh.fileSize
	reqEnd := payload.Offset + int64(payload.Length)
	if reqEnd > maxEnd {
		reqEnd = maxEnd
	}
	if reqEnd <= payload.Offset {
		emptyReader := bytes.NewReader(nil)
		return arpc.Response{
			Status: 213,
			RawStream: func(stream *smux.Stream) {
				if err := binarystream.SendDataFromReader(emptyReader, 0, stream); err != nil {
					syslog.L.Error(err).
						WithMessage("failed sending empty reader via binary stream").Write()
				}
			},
		}, nil
	}
	reqLen := int(reqEnd - payload.Offset)

	const maxChunk = 512 * 1024
	zeroBuf := make([]byte, maxChunk)

	// Try to fetch allocated ranges. If it fails, fall back to treating it as fully allocated.
	ranges, err := queryAllocatedRanges(fh.handle, payload.Offset, int64(reqLen))
	if err != nil || len(ranges) == 0 {
		// Either non-sparse, or API not supported. Treat as contiguous allocation.
		ranges = []allocatedRange{{FileOffset: payload.Offset, Length: int64(reqLen)}}
	}

	// Build a closure to stream data
	streamFn := func(stream *smux.Stream) {
		write := func(p []byte) error {
			return binarystream.SendDataFromReader(bytes.NewReader(p), len(p), stream)
		}

		pos := payload.Offset
		end := reqEnd

		// Iterate each allocated range and fill gaps with zeros.
		for _, r := range ranges {
			// Clamp r to [pos, end)
			rStart := r.FileOffset
			rEnd := r.FileOffset + r.Length
			if rEnd <= pos {
				continue
			}
			if rStart < pos {
				rStart = pos
			}
			if rEnd > end {
				rEnd = end
			}
			if rStart >= rEnd {
				continue
			}

			// Emit gap before this range.
			if rStart > pos {
				gap := rStart - pos
				for gap > 0 {
					ch := int64(maxChunk)
					if gap < ch {
						ch = gap
					}
					if err := write(zeroBuf[:ch]); err != nil {
						syslog.L.Error(err).WithMessage("stream write gap").Write()
						return
					}
					gap -= ch
				}
				pos = rStart
			}

			// Read and emit allocated bytes.
			cur := rStart
			for cur < rEnd {
				ch := int64(maxChunk)
				if rEnd-cur < ch {
					ch = rEnd - cur
				}
				buf := make([]byte, ch)
				n, rerr := readAtOverlapped(fh.handle, cur, buf)
				if rerr != nil && rerr != io.EOF {
					syslog.L.Error(rerr).
						WithMessage("overlapped read error").Write()
					return
				}
				if n > 0 {
					if err := write(buf[:n]); err != nil {
						syslog.L.Error(err).WithMessage("stream write data").Write()
						return
					}
					cur += int64(n)
				}
				if rerr == io.EOF && n == 0 {
					// Unexpected EOF inside an allocated range; stop early.
					return
				}
			}
			pos = rEnd
		}

		// Trailing gap to end.
		if pos < end {
			gap := end - pos
			for gap > 0 {
				ch := int64(maxChunk)
				if gap < ch {
					ch = gap
				}
				if err := write(zeroBuf[:ch]); err != nil {
					syslog.L.Error(err).WithMessage("stream write tail gap").Write()
					return
				}
				gap -= ch
			}
		}
	}

	return arpc.Response{
		Status:    213,
		RawStream: streamFn,
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
