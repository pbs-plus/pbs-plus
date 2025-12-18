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
	"sync"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	binarystream "github.com/pbs-plus/pbs-plus/internal/arpc/binary"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils/pathjoin"
	"github.com/xtaci/smux"
	"golang.org/x/sys/windows"
)

var (
	utf16PathBufPool = sync.Pool{
		New: func() interface{} {
			buf := make([]uint16, 512)
			return &buf
		},
	}
	zeroBufPool = sync.Pool{
		New: func() interface{} {
			buf := make([]byte, 512*1024)
			return &buf
		},
	}
)

func NewFileHandle(handle windows.Handle) *FileHandle {
	fh := &FileHandle{
		handle: handle,
	}
	return fh
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

func toUTF16Z(s string, buf []uint16) []uint16 {
	runes := []rune(s)
	needed := len(runes) + 1
	if cap(buf) < needed {
		buf = make([]uint16, needed)
	} else {
		buf = buf[:needed]
	}
	n := 0
	for _, r := range runes {
		if r < 0x10000 {
			buf[n] = uint16(r)
			n++
		} else {
			r -= 0x10000
			buf[n] = uint16(0xD800 + (r >> 10))
			buf[n+1] = uint16(0xDC00 + (r & 0x3FF))
			n += 2
		}
	}
	buf[n] = 0
	return buf[:n+1]
}

func (s *AgentFSServer) absUNC(filename string) (string, error) {
	windowsDir := filepath.FromSlash(filename)
	if windowsDir == "" || windowsDir == "." || windowsDir == "/" {
		unc := "\\\\?\\" + s.snapshot.Path
		syslog.L.Debug().WithMessage("absUNC: returning UNC snapshot path for root").WithField("path", unc).Write()
		return unc, nil
	}
	path := pathjoin.Join(s.snapshot.Path, windowsDir)
	unc := "\\\\?\\" + path
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
		if fh.ov != nil {
			_ = fh.ov.Close()
			syslog.L.Debug().WithMessage("closeFileHandles: closed overlapped").WithField("handle_id", u).Write()
			fh.ov = nil
		}
		if fh.dirReader != nil {
			fh.dirReader.Close()
		}
		windows.CloseHandle(fh.handle)
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
		errStr := "write operations not allowed"
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

	bufPtr := utf16PathBufPool.Get().(*[]uint16)
	pathUTF16 := toUTF16Z(path, *bufPtr)

	desiredAccess := uint32(windows.GENERIC_READ)
	share := uint32(
		windows.FILE_SHARE_READ |
			windows.FILE_SHARE_WRITE |
			windows.FILE_SHARE_DELETE,
	)
	flags := uint32(windows.FILE_FLAG_BACKUP_SEMANTICS | windows.FILE_FLAG_OVERLAPPED)

	syslog.L.Debug().WithMessage("handleOpenFile: CreateFile").WithField("path", path).Write()
	handle, err := windows.CreateFile(
		&pathUTF16[0],
		desiredAccess,
		share,
		nil,
		windows.OPEN_EXISTING,
		flags,
		0,
	)
	utf16PathBufPool.Put(bufPtr)

	if err != nil {
		syslog.L.Error(err).WithMessage("handleOpenFile: CreateFile failed").WithField("path", path).Write()
		return arpc.Response{}, err
	}

	var std fileStandardInfo
	if err := getFileStandardInfoByHandle(handle, &std); err != nil {
		windows.CloseHandle(handle)
		syslog.L.Error(err).WithMessage("handleOpenFile: getFileStandardInfoByHandle failed").WithField("path", path).Write()
		return arpc.Response{}, err
	}

	fh := NewFileHandle(handle)
	fh.fileSize = std.EndOfFile
	fh.isDir = std.Directory != 0
	fh.logicalOffset = 0
	fh.ov = newOverlapped(handle)

	if fh.isDir {
		dirPath, err := s.abs(payload.Path)
		if err != nil {
			windows.CloseHandle(handle)
			syslog.L.Error(err).WithMessage("handleOpenFile: abs failed for dir").WithField("path", payload.Path).Write()
			return arpc.Response{}, err
		}
		reader, err := NewDirReaderNT(dirPath)
		if err != nil {
			windows.CloseHandle(handle)
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
		if fh.ov != nil {
			_ = fh.ov.Close()
		}
		windows.CloseHandle(fh.handle)
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
	if err != nil {
		fh.releaseOp()
		if !errors.Is(err, os.ErrProcessDone) {
			syslog.L.Error(err).WithMessage("handleReadDir: error reading batch").WithField("handle_id", payload.HandleID).Write()
		} else {
			syslog.L.Debug().WithMessage("handleReadDir: directory read complete").WithField("handle_id", payload.HandleID).Write()
		}
		return arpc.Response{}, err
	}

	syslog.L.Debug().WithMessage("handleReadDir: sending batch").WithField("handle_id", payload.HandleID).WithField("bytes", len(encodedBatch)).Write()
	byteReader := bytes.NewReader(encodedBatch)
	streamCallback := func(stream *smux.Stream) {
		defer fh.releaseOp()

		if err := binarystream.SendDataFromReader(byteReader, int(len(encodedBatch)), stream); err != nil {
			syslog.L.Error(err).WithMessage("handleReadDir: failed sending data from reader via binary stream").WithField("handle_id", payload.HandleID).Write()
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
		syslog.L.Debug().WithMessage("handleReadAt: handle not found").WithField("handle_id", payload.HandleID).Write()
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
	fileSize := fh.fileSize
	ov := fh.ov
	fh.mu.Unlock()

	if err := req.Context.Err(); err != nil {
		fh.releaseOp()
		syslog.L.Debug().WithMessage("handleReadAt: context cancelled before read").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, err
	}

	if payload.Offset >= fileSize {
		fh.releaseOp()
		syslog.L.Debug().WithMessage("handleReadAt: offset beyond EOF, sending empty").WithField("handle_id", payload.HandleID).WithField("offset", payload.Offset).WithField("size", fileSize).Write()
		emptyReader := bytes.NewReader(nil)
		return arpc.Response{
			Status: 213,
			RawStream: func(stream *smux.Stream) {
				if err := binarystream.SendDataFromReader(emptyReader, 0, stream); err != nil {
					syslog.L.Error(err).WithMessage("handleReadAt: failed sending empty reader").WithField("handle_id", payload.HandleID).Write()
				}
			},
		}, nil
	}

	maxEnd := fileSize
	reqEnd := payload.Offset + int64(payload.Length)
	if reqEnd > maxEnd {
		reqEnd = maxEnd
	}
	readLen := int(reqEnd - payload.Offset)

	streamFn := func(stream *smux.Stream) {
		defer fh.releaseOp()

		bufPtr := zeroBufPool.Get().(*[]byte)
		defer zeroBufPool.Put(bufPtr)
		buf := (*bufPtr)[:readLen]

		fh.mu.Lock()
		if ov.DefaultTimeout == -1 {
			ov.DefaultTimeout = 30000
		}
		fh.mu.Unlock()

		n, err := ov.ReadAt(buf, payload.Offset)

		if err != nil && err != io.EOF {
			syslog.L.Error(err).WithMessage("handleReadAt: ov.ReadAt failed").WithField("handle_id", payload.HandleID).Write()
			return
		}

		if n > 0 {
			if err := binarystream.SendDataFromReader(bytes.NewReader(buf[:n]), n, stream); err != nil {
				syslog.L.Error(err).WithMessage("handleReadAt: stream write failed").WithField("handle_id", payload.HandleID).Write()
				return
			}
		}

		syslog.L.Debug().WithMessage("handleReadAt: stream completed").WithField("handle_id", payload.HandleID).WithField("bytes", n).Write()
	}

	syslog.L.Debug().WithMessage("handleReadAt: starting stream").WithField("handle_id", payload.HandleID).WithField("offset", payload.Offset).WithField("length", readLen).Write()
	return arpc.Response{
		Status:    213,
		RawStream: streamFn,
	}, nil
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
	if err := getFileStandardInfoByHandle(fh.handle, &std); err != nil {
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
			fh.handle,
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
		if handle.ov != nil {
			_ = handle.ov.Close()
			handle.ov = nil
			syslog.L.Debug().WithMessage("handleClose: overlapped closed").
				WithField("handle_id", payload.HandleID).Write()
		}
		windows.CloseHandle(handle.handle)
		syslog.L.Debug().WithMessage("handleClose: file handle closed").
			WithField("handle_id", payload.HandleID).Write()
	} else {
		if handle.dirReader != nil {
			handle.dirReader.Close()
		}
		if handle.ov != nil {
			_ = handle.ov.Close()
			handle.ov = nil
			syslog.L.Debug().WithMessage("handleClose: dir overlapped closed").
				WithField("handle_id", payload.HandleID).Write()
		}
		windows.CloseHandle(handle.handle)
		syslog.L.Debug().WithMessage("handleClose: dir handle closed").
			WithField("handle_id", payload.HandleID).Write()
	}
	handle.mu.Unlock()

	s.handles.Del(uint64(payload.HandleID))

	closed := "closed"
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
