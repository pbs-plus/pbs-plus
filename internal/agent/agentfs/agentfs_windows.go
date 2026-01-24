//go:build windows

package agentfs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unsafe"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	binarystream "github.com/pbs-plus/pbs-plus/internal/arpc/binary"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils/pathjoin"
	"github.com/xtaci/smux"
	"golang.org/x/sys/windows"
)

func NewFileHandle(handle *os.File) *FileHandle {
	return &FileHandle{
		handle: handle,
	}
}

func (fh *FileHandle) acquireOp() bool {
	fh.mu.Lock()
	defer fh.mu.Unlock()

	if fh.closing {
		return false
	}
	fh.activeOps++
	return true
}

func (fh *FileHandle) releaseOp() {
	fh.mu.Lock()
	defer fh.mu.Unlock()

	fh.activeOps--
	if fh.activeOps == 0 && fh.closing {
		close(fh.closeDone)
	}
}

func (fh *FileHandle) beginClose() bool {
	fh.mu.Lock()
	defer fh.mu.Unlock()

	if fh.closing {
		return false
	}
	fh.closing = true
	fh.closeDone = make(chan struct{})
	if fh.activeOps == 0 {
		close(fh.closeDone)
	}
	return true
}

func (fh *FileHandle) waitForOps(timeout time.Duration) bool {
	fh.mu.Lock()
	done := fh.closeDone
	fh.mu.Unlock()

	if done == nil {
		return true
	}

	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (s *AgentFSServer) abs(filename string) (string, error) {
	windowsDir := filepath.FromSlash(filename)
	if windowsDir == "" || windowsDir == "." || windowsDir == "/" {
		syslog.L.Debug().WithMessage("abs: returning snapshot path for root").Write()
		return s.snapshot.Path, nil
	}
	path := pathjoin.Join(s.snapshot.Path, windowsDir)
	syslog.L.Debug().WithMessage("abs: joined path").WithField("path", path).Write()
	return path, nil
}

func (s *AgentFSServer) absUNC(filename string) (string, error) {
	windowsDir := filepath.FromSlash(filename)
	if windowsDir == "" || windowsDir == "." || windowsDir == "/" {
		unc := `\\?\` + s.snapshot.Path
		syslog.L.Debug().WithMessage("absUNC: returning UNC snapshot path for root").WithField("path", unc).Write()
		return unc, nil
	}
	path := pathjoin.Join(s.snapshot.Path, windowsDir)
	var b strings.Builder
	b.Grow(4 + len(path))
	b.WriteString(`\\?\`)
	b.WriteString(path)
	unc := b.String()
	syslog.L.Debug().WithMessage("absUNC: joined UNC path").WithField("path", unc).Write()
	return unc, nil
}

func (s *AgentFSServer) closeFileHandles() {
	syslog.L.Debug().WithMessage("closeFileHandles: closing all file handles").Write()
	s.handles.ForEach(func(u uint64, fh *FileHandle) bool {
		if !fh.beginClose() {
			return true
		}

		fh.waitForOps(30 * time.Second)

		fh.mu.Lock()
		if fh.mapping != 0 {
			windows.CloseHandle(fh.mapping)
			syslog.L.Debug().WithMessage("closeFileHandles: closed mapping handle").WithField("handle_id", u).Write()
			fh.mapping = 0
		}
		if fh.dirReader != nil {
			fh.dirReader.Close()
		}
		if fh.handle != nil {
			fh.handle.Close()
		}
		syslog.L.Debug().WithMessage("closeFileHandles: closed file handle").WithField("handle_id", u).Write()
		fh.mu.Unlock()
		return true
	})
	s.handles.Clear()
	syslog.L.Debug().WithMessage("closeFileHandles: all handles cleared").Write()
}

func (s *AgentFSServer) initializeStatFS() error {
	var err error
	if s.snapshot.SourcePath != "" {
		driveLetter := s.snapshot.SourcePath[:1]
		syslog.L.Debug().WithMessage("initializeStatFS: initializing").WithField("drive", driveLetter).Write()
		s.statFs, err = getStatFS(driveLetter)
		if err != nil {
			syslog.L.Error(err).WithMessage("initializeStatFS: getStatFS failed").WithField("drive", driveLetter).Write()
			return err
		}
		syslog.L.Debug().WithMessage("initializeStatFS: initialized successfully").WithField("drive", driveLetter).Write()
	} else {
		syslog.L.Debug().WithMessage("initializeStatFS: snapshot.SourcePath is empty").Write()
	}
	return nil
}

func (s *AgentFSServer) handleStatFS(req *arpc.Request) (arpc.Response, error) {
	syslog.L.Debug().WithMessage("handleStatFS: encoding statFs").Write()
	enc, err := cbor.Marshal(s.statFs)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleStatFS: encode failed").Write()
		return arpc.Response{}, err
	}
	syslog.L.Debug().WithMessage("handleStatFS: success").Write()
	return arpc.Response{
		Status: 200,
		Data:   enc,
	}, nil
}

func (s *AgentFSServer) handleOpenFile(req *arpc.Request) (arpc.Response, error) {
	syslog.L.Debug().WithMessage("handleOpenFile: decoding request").Write()
	var payload types.OpenFileReq
	if err := cbor.Unmarshal(req.Payload, &payload); err != nil {
		syslog.L.Error(err).WithMessage("handleOpenFile: decode failed").Write()
		return arpc.Response{}, err
	}

	if payload.Flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC) != 0 {
		syslog.L.Debug().WithMessage("handleOpenFile: write operation attempted and blocked").WithField("path", payload.Path).Write()
		const errStr = "write operations not allowed"
		errBytes, err := cbor.Marshal(errStr)
		if err != nil {
			syslog.L.Error(err).WithMessage("handleOpenFile: encode error message failed").Write()
			return arpc.Response{}, err
		}
		return arpc.Response{
			Status: 403,
			Data:   errBytes,
		}, nil
	}

	path, err := s.absUNC(payload.Path)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleOpenFile: absUNC failed").WithField("path", payload.Path).Write()
		return arpc.Response{}, err
	}

	pathUTF16, err := windows.UTF16PtrFromString(path)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleOpenFile: UTF16 conversion failed").WithField("path", path).Write()
		return arpc.Response{}, err
	}

	desiredAccess := uint32(windows.GENERIC_READ)
	share := uint32(
		windows.FILE_SHARE_READ |
			windows.FILE_SHARE_WRITE |
			windows.FILE_SHARE_DELETE,
	)
	flags := uint32(windows.FILE_FLAG_BACKUP_SEMANTICS | windows.FILE_FLAG_OVERLAPPED)

	syslog.L.Debug().WithMessage("handleOpenFile: CreateFile").WithField("path", path).Write()
	rawHandle, err := windows.CreateFile(
		pathUTF16,
		desiredAccess,
		share,
		nil,
		windows.OPEN_EXISTING,
		flags,
		0,
	)
	handle := os.NewFile(uintptr(rawHandle), path)

	if err != nil {
		syslog.L.Error(err).WithMessage("handleOpenFile: CreateFile failed").WithField("path", path).Write()
		return arpc.Response{}, err
	}

	var std fileStandardInfo
	if err := getFileStandardInfoByHandle(windows.Handle(handle.Fd()), &std); err != nil {
		handle.Close()
		syslog.L.Error(err).WithMessage("handleOpenFile: getFileStandardInfoByHandle failed").WithField("path", path).Write()
		return arpc.Response{}, err
	}

	fh := NewFileHandle(handle)
	fh.fileSize = std.EndOfFile
	fh.isDir = std.Directory != 0

	if fh.isDir {
		fh.handle.Close()

		dirPath, err := s.abs(payload.Path)
		if err != nil {
			syslog.L.Error(err).WithMessage("handleOpenFile: abs failed for dir").WithField("path", payload.Path).Write()
			return arpc.Response{}, err
		}
		reader, err := NewDirReaderNT(dirPath)
		if err != nil {
			syslog.L.Error(err).WithMessage("handleOpenFile: NewDirReaderNT failed").WithField("path", dirPath).Write()
			return arpc.Response{}, err
		}
		fh.dirReader = reader
	}

	handleId := s.handleIdGen.NextID()
	s.handles.Set(handleId, fh)

	fhId := types.FileHandleId(handleId)
	dataBytes, err := cbor.Marshal(fhId)
	if err != nil {
		if fh.dirReader != nil {
			fh.dirReader.Close()
		}
		if fh.mapping != 0 {
			windows.CloseHandle(fh.mapping)
		}
		fh.handle.Close()
		syslog.L.Error(err).WithMessage("handleOpenFile: encode handle id failed").Write()
		return arpc.Response{}, err
	}

	syslog.L.Debug().WithMessage("handleOpenFile: success").WithField("handle_id", handleId).WithField("is_dir", fh.isDir).WithField("size", fh.fileSize).Write()
	return arpc.Response{
		Status: 200,
		Data:   dataBytes,
	}, nil
}

func (s *AgentFSServer) handleAttr(req *arpc.Request) (arpc.Response, error) {
	syslog.L.Debug().WithMessage("handleAttr: decoding request").Write()
	var payload types.StatReq
	if err := cbor.Unmarshal(req.Payload, &payload); err != nil {
		syslog.L.Error(err).WithMessage("handleAttr: decode failed").Write()
		return arpc.Response{}, err
	}

	fullPath, err := s.absUNC(payload.Path)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleAttr: absUNC failed").WithField("path", payload.Path).Write()
		return arpc.Response{}, err
	}

	h, err := openForAttrs(fullPath)
	if err != nil {
		if !strings.HasSuffix(fullPath, ".pxarexclude") {
			syslog.L.Error(err).WithMessage("handleAttr: openForAttrs failed").WithField("path", fullPath).Write()
		}
		return arpc.Response{}, err
	}
	defer windows.CloseHandle(h)

	var nfo fileNetworkOpenInformation
	if err := ntQueryFileNetworkOpenInformation(h, &nfo); err != nil {
		syslog.L.Error(err).WithMessage("handleAttr: ntQueryFileNetworkOpenInformation failed").WithField("path", fullPath).Write()
		return arpc.Response{}, err
	}

	isDir := (nfo.FileAttributes & windows.FILE_ATTRIBUTE_DIRECTORY) != 0
	size := nfo.EndOfFile

	modTime := filetimeToTime(windows.Filetime{
		LowDateTime:  uint32(uint64(nfo.LastWriteTime) & 0xFFFFFFFF),
		HighDateTime: uint32(uint64(nfo.LastWriteTime) >> 32),
	})

	mode := windowsFileModeFromHandle(h, nfo.FileAttributes)
	name := filepath.Base(filepath.Clean(fullPath))

	blockSize := s.statFs.Bsize
	if blockSize == 0 {
		blockSize = 4096
	}

	var blocks uint64
	if !isDir {
		alloc := nfo.AllocationSize
		if alloc < 0 {
			alloc = 0
		}
		blocks = uint64((alloc + int64(blockSize) - 1) / int64(blockSize))
	}

	info := types.AgentFileInfo{
		Name:    name,
		Size:    size,
		Mode:    mode,
		ModTime: modTime.UnixNano(),
		IsDir:   isDir,
		Blocks:  blocks,
	}
	data, err := cbor.Marshal(info)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleAttr: encode failed").WithField("path", fullPath).Write()
		return arpc.Response{}, err
	}
	syslog.L.Debug().WithMessage("handleAttr: success").WithField("path", fullPath).WithField("is_dir", isDir).WithField("size", size).Write()
	return arpc.Response{Status: 200, Data: data}, nil
}

func (s *AgentFSServer) handleXattr(req *arpc.Request) (arpc.Response, error) {
	syslog.L.Debug().WithMessage("handleXattr: decoding request").Write()
	var payload types.StatReq
	if err := cbor.Unmarshal(req.Payload, &payload); err != nil {
		syslog.L.Error(err).WithMessage("handleXattr: decode failed").Write()
		return arpc.Response{}, err
	}

	fullPath, err := s.absUNC(payload.Path)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleXattr: absUNC failed").WithField("path", payload.Path).Write()
		return arpc.Response{}, err
	}

	h, err := openForAttrs(fullPath)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleXattr: openForAttrs failed").WithField("path", fullPath).Write()
		return arpc.Response{}, err
	}
	defer windows.CloseHandle(h)

	var creationTime int64
	var lastAccessTime int64
	var lastWriteTime int64
	var fileAttributes map[string]bool

	if !payload.AclOnly {
		var nfo fileNetworkOpenInformation
		if err := ntQueryFileNetworkOpenInformation(h, &nfo); err != nil {
			syslog.L.Error(err).WithMessage("handleXattr: ntQueryFileNetworkOpenInformation failed").WithField("path", fullPath).Write()
			return arpc.Response{}, err
		}

		creationTime = filetimeToUnix(windows.Filetime{
			LowDateTime:  uint32(uint64(nfo.CreationTime) & 0xFFFFFFFF),
			HighDateTime: uint32(uint64(nfo.CreationTime) >> 32),
		})
		lastAccessTime = filetimeToUnix(windows.Filetime{
			LowDateTime:  uint32(uint64(nfo.LastAccessTime) & 0xFFFFFFFF),
			HighDateTime: uint32(uint64(nfo.LastAccessTime) >> 32),
		})
		lastWriteTime = filetimeToUnix(windows.Filetime{
			LowDateTime:  uint32(uint64(nfo.LastWriteTime) & 0xFFFFFFFF),
			HighDateTime: uint32(uint64(nfo.LastWriteTime) >> 32),
		})

		fileAttributes = parseFileAttributes(nfo.FileAttributes)
		syslog.L.Debug().WithMessage("handleXattr: file attributes and times acquired").WithField("path", fullPath).Write()
	}
	owner, group, acls, err := GetWinACLsHandle(h)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleXattr: GetWinACLsHandle failed").WithField("path", fullPath).Write()
		return arpc.Response{}, err
	}

	info := types.AgentFileInfo{
		CreationTime:   creationTime,
		LastAccessTime: lastAccessTime,
		LastWriteTime:  lastWriteTime,
		FileAttributes: fileAttributes,
		Owner:          owner,
		Group:          group,
		WinACLs:        acls,
	}
	data, err := cbor.Marshal(info)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleXattr: encode failed").WithField("path", fullPath).Write()
		return arpc.Response{}, err
	}
	syslog.L.Debug().WithMessage("handleXattr: success").WithField("path", fullPath).WithField("data", info).Write()
	return arpc.Response{Status: 200, Data: data}, nil
}

func (s *AgentFSServer) handleReadDir(req *arpc.Request) (arpc.Response, error) {
	syslog.L.Debug().WithMessage("handleReadDir: decoding request").Write()
	var payload types.ReadDirReq
	if err := cbor.Unmarshal(req.Payload, &payload); err != nil {
		syslog.L.Error(err).WithMessage("handleReadDir: decode failed").Write()
		return arpc.Response{}, err
	}

	fh, exists := s.handles.Get(uint64(payload.HandleID))
	if !exists {
		syslog.L.Debug().WithMessage("handleReadDir: handle not found").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, os.ErrNotExist
	}

	if !fh.acquireOp() {
		syslog.L.Debug().WithMessage("handleReadDir: handle is closing").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, os.ErrClosed
	}

	fh.mu.Lock()
	dirReader := fh.dirReader
	fh.mu.Unlock()

	encodedBatch, err := dirReader.NextBatch(req.Context, s.statFs.Bsize)
	isDone := errors.Is(err, os.ErrProcessDone)
	if err != nil && !isDone {
		fh.releaseOp()
		return arpc.Response{}, err
	}

	syslog.L.Debug().WithMessage("handleReadDir: sending batch").WithField("handle_id", payload.HandleID).WithField("bytes", len(encodedBatch)).Write()

	streamCallback := func(stream *smux.Stream) {
		defer fh.releaseOp()

		if err := binarystream.SendDataFromReader(bytes.NewReader(encodedBatch), len(encodedBatch), stream); err != nil {
			syslog.L.Error(err).WithMessage("handleReadDir: failed sending data from reader via binary stream").WithField("handle_id", payload.HandleID).Write()
		}

		if isDone {
			s.handleDirClose(uint64(payload.HandleID), fh)
		}
	}

	return arpc.Response{
		Status:    213,
		RawStream: streamCallback,
	}, nil
}

func (s *AgentFSServer) handleReadAt(req *arpc.Request) (arpc.Response, error) {
	syslog.L.Debug().WithMessage("handleReadAt: decoding request").Write()
	var payload types.ReadAtReq
	if err := cbor.Unmarshal(req.Payload, &payload); err != nil {
		syslog.L.Error(err).WithMessage("handleReadAt: decode failed").Write()
		return arpc.Response{}, err
	}

	if payload.Length < 0 {
		syslog.L.Debug().WithMessage("handleReadAt: negative length requested").WithField("length", payload.Length).Write()
		return arpc.Response{}, fmt.Errorf("invalid negative length requested: %d", payload.Length)
	}

	fh, exists := s.handles.Get(uint64(payload.HandleID))
	if !exists {
		syslog.L.Warn().WithMessage("handleReadAt: handle not found").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, os.ErrNotExist
	}

	if fh.isDir {
		syslog.L.Debug().WithMessage("handleReadAt: attempted read on directory handle").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, os.ErrInvalid
	}

	if !fh.acquireOp() {
		syslog.L.Debug().WithMessage("handleReadAt: handle is closing").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, os.ErrClosed
	}

	fh.mu.Lock()
	handle := fh.handle
	fileSize := fh.fileSize
	readMode := s.readMode
	fh.mu.Unlock()

	if payload.Offset >= fileSize {
		fh.releaseOp()
		syslog.L.Debug().WithMessage("handleReadAt: offset beyond EOF").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{Status: 213, RawStream: func(stream *smux.Stream) {
			_ = binarystream.SendDataFromReader(bytes.NewReader(nil), 0, stream)
		}}, nil
	}

	maxEnd := fileSize
	reqEnd := payload.Offset + int64(payload.Length)
	if reqEnd > maxEnd {
		reqEnd = maxEnd
	}
	reqLen := int(reqEnd - payload.Offset)

	if readMode == "mmap" {
		syslog.L.Debug().WithMessage("handleReadAt: attempting memory-mapped read").WithField("handle_id", payload.HandleID).Write()

		mappingHandle, err := windows.CreateFileMapping(
			windows.Handle(handle.Fd()),
			nil,
			windows.PAGE_READONLY,
			0, 0,
			nil,
		)
		if err == nil {
			addr, err := windows.MapViewOfFile(
				mappingHandle,
				windows.FILE_MAP_READ,
				uint32(payload.Offset>>32),
				uint32(payload.Offset),
				uintptr(reqLen),
			)
			if err == nil {
				defer windows.UnmapViewOfFile(addr)

				data := unsafe.Slice((*byte)(unsafe.Pointer(addr)), reqLen)
				dataCopy := make([]byte, len(data))
				copy(dataCopy, data)

				return arpc.Response{Status: 213, RawStream: func(stream *smux.Stream) {
					defer fh.releaseOp()
					_ = binarystream.SendDataFromReader(bytes.NewReader(dataCopy), len(dataCopy), stream)
				}}, nil
			}
		}
		syslog.L.Warn().WithMessage("handleReadAt: mmap failed, falling back to overlapped I/O").Write()
	}

	bptr := readBufPool.Get().(*[]byte)
	workBuf := *bptr
	isTemporary := false
	if len(workBuf) < reqLen {
		workBuf = make([]byte, reqLen)
		isTemporary = true
	}

	n, err := handle.ReadAt(workBuf[:reqLen], payload.Offset)

	if err != nil && err != io.EOF {
		if !isTemporary {
			readBufPool.Put(bptr)
		}
		fh.releaseOp()
		syslog.L.Error(err).WithMessage("handleReadAt: overlapped read failed").Write()
		return arpc.Response{}, err
	}

	return arpc.Response{Status: 213, RawStream: func(stream *smux.Stream) {
		defer fh.releaseOp()
		if !isTemporary {
			defer readBufPool.Put(bptr)
		}
		_ = binarystream.SendDataFromReader(bytes.NewReader(workBuf[:n]), n, stream)
		syslog.L.Debug().WithMessage("handleReadAt: stream completed").WithField("bytes", n).Write()
	}}, nil
}

func (s *AgentFSServer) handleLseek(req *arpc.Request) (arpc.Response, error) {
	syslog.L.Debug().WithMessage("handleLseek: decoding request").Write()
	var payload types.LseekReq
	if err := cbor.Unmarshal(req.Payload, &payload); err != nil {
		syslog.L.Error(err).WithMessage("handleLseek: decode failed").Write()
		return arpc.Response{}, err
	}

	if payload.Whence != io.SeekStart &&
		payload.Whence != io.SeekCurrent &&
		payload.Whence != io.SeekEnd &&
		payload.Whence != SeekData &&
		payload.Whence != SeekHole {
		syslog.L.Debug().WithMessage("handleLseek: invalid whence").WithField("whence", payload.Whence).Write()
		return arpc.Response{}, os.ErrInvalid
	}

	fh, exists := s.handles.Get(uint64(payload.HandleID))
	if !exists {
		syslog.L.Debug().WithMessage("handleLseek: handle not found").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, os.ErrNotExist
	}
	if fh.isDir {
		syslog.L.Debug().WithMessage("handleLseek: attempted lseek on directory").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, os.ErrInvalid
	}

	if !fh.acquireOp() {
		syslog.L.Debug().WithMessage("handleLseek: handle closed").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, os.ErrClosed
	}
	defer fh.releaseOp()

	fh.mu.Lock()
	defer fh.mu.Unlock()

	var std fileStandardInfo
	if err := getFileStandardInfoByHandle(windows.Handle(fh.handle.Fd()), &std); err != nil {
		syslog.L.Error(err).WithMessage("handleLseek: getFileStandardInfoByHandle failed").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, err
	}
	fileSize := std.EndOfFile

	cur := fh.logicalOffset
	var newOffset int64

	switch payload.Whence {
	case io.SeekStart:
		if payload.Offset < 0 {
			syslog.L.Debug().WithMessage("handleLseek: negative offset with SeekStart").WithField("offset", payload.Offset).Write()
			return arpc.Response{}, os.ErrInvalid
		}
		newOffset = payload.Offset

	case io.SeekCurrent:
		if cur+payload.Offset < 0 {
			syslog.L.Debug().WithMessage("handleLseek: negative resulting offset with SeekCurrent").WithField("current", cur).WithField("delta", payload.Offset).Write()
			return arpc.Response{}, os.ErrInvalid
		}
		newOffset = cur + payload.Offset

	case io.SeekEnd:
		if fileSize+payload.Offset < 0 {
			syslog.L.Debug().WithMessage("handleLseek: negative resulting offset with SeekEnd").WithField("file_size", fileSize).WithField("delta", payload.Offset).Write()
			return arpc.Response{}, os.ErrInvalid
		}
		newOffset = fileSize + payload.Offset

	case SeekData, SeekHole:
		no, err := sparseSeekAllocatedRanges(
			windows.Handle(fh.handle.Fd()),
			payload.Offset,
			payload.Whence,
			fileSize,
		)
		if err != nil {
			syslog.L.Error(err).WithMessage("handleLseek: sparseSeekAllocatedRanges failed").WithField("handle_id", payload.HandleID).Write()
			return arpc.Response{}, err
		}
		newOffset = no

	default:
		syslog.L.Debug().WithMessage("handleLseek: invalid whence in default").Write()
		return arpc.Response{}, os.ErrInvalid
	}

	if newOffset > fileSize {
		syslog.L.Debug().WithMessage("handleLseek: newOffset beyond EOF").WithField("new_offset", newOffset).WithField("file_size", fileSize).Write()
		return arpc.Response{}, os.ErrInvalid
	}

	fh.logicalOffset = newOffset

	resp := types.LseekResp{NewOffset: newOffset}
	respBytes, err := cbor.Marshal(resp)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleLseek: encode failed").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, err
	}
	syslog.L.Debug().WithMessage("handleLseek: success").WithField("handle_id", payload.HandleID).WithField("new_offset", newOffset).Write()
	return arpc.Response{Status: 200, Data: respBytes}, nil
}

func (s *AgentFSServer) handleDirClose(id uint64, fh *FileHandle) {
	if !fh.beginClose() {
		return
	}

	fh.mu.Lock()
	if fh.dirReader != nil {
		_ = fh.dirReader.Close()
		fh.dirReader = nil
	}
	fh.mu.Unlock()

	s.handles.Del(id)
	syslog.L.Debug().WithMessage("handleDirClose: handle closed and removed").
		WithField("handle_id", id).Write()
}

func (s *AgentFSServer) handleClose(req *arpc.Request) (arpc.Response, error) {
	syslog.L.Debug().WithMessage("handleClose: decoding request").Write()
	var payload types.CloseReq
	if err := cbor.Unmarshal(req.Payload, &payload); err != nil {
		syslog.L.Error(err).WithMessage("handleClose: decode failed").Write()
		return arpc.Response{}, err
	}

	handle, exists := s.handles.Get(uint64(payload.HandleID))
	if !exists {
		syslog.L.Debug().WithMessage("handleClose: handle not found").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, os.ErrNotExist
	}

	if !handle.beginClose() {
		syslog.L.Debug().WithMessage("handleClose: handle already closing").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, os.ErrClosed
	}

	if !handle.waitForOps(30 * time.Second) {
		syslog.L.Warn().WithMessage("handleClose: timeout waiting for operations, forcing close").
			WithField("handle_id", payload.HandleID).Write()
	}

	handle.mu.Lock()
	if handle.mapping != 0 {
		windows.CloseHandle(handle.mapping)
		handle.mapping = 0
		syslog.L.Debug().WithMessage("handleClose: mapping closed").
			WithField("handle_id", payload.HandleID).Write()
	}
	if !handle.isDir {
		handle.handle.Close()
		syslog.L.Debug().WithMessage("handleClose: file handle closed").
			WithField("handle_id", payload.HandleID).Write()
	} else {
		if handle.dirReader != nil {
			handle.dirReader.Close()
			handle.dirReader = nil
		}
		syslog.L.Debug().WithMessage("handleClose: dir handle closed").
			WithField("handle_id", payload.HandleID).Write()
	}
	handle.mu.Unlock()

	s.handles.Del(uint64(payload.HandleID))

	const closed = "closed"
	data, err := cbor.Marshal(closed)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleClose: encode close message failed").
			WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, err
	}

	syslog.L.Debug().WithMessage("handleClose: success").
		WithField("handle_id", payload.HandleID).Write()
	return arpc.Response{Status: 200, Data: data}, nil
}
