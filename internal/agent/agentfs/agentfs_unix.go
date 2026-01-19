//go:build unix && !linux

package agentfs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
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
	"golang.org/x/sys/unix"
)

type FileHandle struct {
	sync.Mutex
	file        *os.File
	dirReader   *DirReaderUnix
	fileSize    int64
	isDir       bool
	isVirtual   bool
	virtualPath string
	curOffset   int64
	closing     bool
	activeOps   int
	closeDone   chan struct{}
}

func (fh *FileHandle) acquireOp() bool {
	fh.Lock()
	defer fh.Unlock()
	if fh.closing {
		return false
	}
	fh.activeOps++
	return true
}

func (fh *FileHandle) releaseOp() {
	fh.Lock()
	defer fh.Unlock()
	fh.activeOps--
	if fh.closing && fh.activeOps == 0 && fh.closeDone != nil {
		select {
		case <-fh.closeDone: // Already closed
		default:
			close(fh.closeDone)
		}
	}
}

func (fh *FileHandle) beginClose() bool {
	fh.Lock()
	defer fh.Unlock()
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

func (s *AgentFSServer) writeIDString(dst *string, id uint32) {
	s.idBuf = strconv.AppendUint(s.idBuf[:0], uint64(id), 10)
	*dst = string(s.idBuf)
}

func (s *AgentFSServer) abs(filename string) (string, bool, error) {
	cleanReq := strings.Trim(filename, "/")
	cleanSource := strings.Trim(s.snapshot.SourcePath, "/")

	if cleanReq == "" || (len(cleanSource) > len(cleanReq) && strings.HasPrefix(cleanSource, cleanReq+"/")) {
		return cleanReq, true, nil
	}

	if cleanReq == cleanSource || strings.HasPrefix(cleanReq, cleanSource+"/") {
		relPath := strings.TrimPrefix(strings.TrimPrefix(cleanReq, cleanSource), "/")
		return pathjoin.Join(s.snapshot.Path, relPath), false, nil
	}

	return "", false, os.ErrNotExist
}

func (s *AgentFSServer) closeFileHandles() {
	syslog.L.Debug().WithMessage("closeFileHandles: closing all file handles").Write()
	s.handles.ForEach(func(u uint64, fh *FileHandle) bool {
		fh.Lock()
		defer fh.Unlock()

		if fh.file != nil {
			_ = fh.file.Close()
			fh.file = nil
			syslog.L.Debug().WithMessage("closeFileHandles: closed file").WithField("handle_id", u).Write()
		} else if fh.dirReader != nil {
			_ = fh.dirReader.Close()
			fh.dirReader = nil
			syslog.L.Debug().WithMessage("closeFileHandles: closed dir reader").WithField("handle_id", u).Write()
		}

		return true
	})
	s.handles.Clear()
	syslog.L.Debug().WithMessage("closeFileHandles: all handles cleared").Write()
}

func (s *AgentFSServer) initializeStatFS() error {
	var err error
	if s.snapshot.SourcePath != "" {
		syslog.L.Debug().WithMessage("initializeStatFS: initializing").WithField("source_path", s.snapshot.SourcePath).Write()
		s.statFs, err = getStatFS(s.snapshot.SourcePath)
		if err != nil {
			syslog.L.Error(err).WithMessage("initializeStatFS: getStatFS failed").WithField("source_path", s.snapshot.SourcePath).Write()
			return err
		}
		syslog.L.Debug().WithMessage("initializeStatFS: initialized successfully").WithField("source_path", s.snapshot.SourcePath).Write()
	} else {
		syslog.L.Warn().WithMessage("initializeStatFS: snapshot.SourcePath is empty").Write()
	}
	return nil
}

func getStatFS(path string) (types.StatFS, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return types.StatFS{}, fmt.Errorf("path cannot be empty")
	}
	var statfs unix.Statfs_t
	err := unix.Statfs(path, &statfs)
	if err != nil {
		return types.StatFS{}, fmt.Errorf("failed to get filesystem stats for path %s: %w", path, err)
	}
	stat := types.StatFS{
		Bsize:   uint64(statfs.Bsize),
		Blocks:  uint64(statfs.Blocks),
		Bfree:   uint64(statfs.Bfree),
		Bavail:  uint64(statfs.Bavail),
		Files:   uint64(statfs.Files),
		Ffree:   uint64(statfs.Ffree),
		NameLen: getNameLen(statfs),
	}
	return stat, nil
}

func getNameLen(statfs unix.Statfs_t) uint64 {
	return getNameLenPlatform(statfs)
}

func (s *AgentFSServer) handleStatFS(req *arpc.Request) (arpc.Response, error) {
	syslog.L.Debug().WithMessage("handleStatFS: encoding statFs").Write()
	enc, err := cbor.Marshal(s.statFs)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleStatFS: encode failed").Write()
		return arpc.Response{}, err
	}
	syslog.L.Debug().WithMessage("handleStatFS: success").Write()
	return arpc.Response{Status: 200, Data: enc}, nil
}

func (s *AgentFSServer) handleOpenFile(req *arpc.Request) (arpc.Response, error) {
	syslog.L.Debug().WithMessage("handleOpenFile: decoding request").Write()
	var payload types.OpenFileReq
	if err := cbor.Unmarshal(req.Payload, &payload); err != nil {
		syslog.L.Error(err).WithMessage("handleOpenFile: decode failed").Write()
		return arpc.Response{}, err
	}

	if payload.Flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC) != 0 {
		syslog.L.Warn().WithMessage("handleOpenFile: write operation blocked").WithField("path", payload.Path).Write()
		errBytes, _ := cbor.Marshal("write operations not allowed")
		return arpc.Response{Status: 403, Data: errBytes}, nil
	}

	path, isVirtual, err := s.abs(payload.Path)
	if err != nil {
		return arpc.Response{}, err
	}

	handleId := s.handleIdGen.NextID()
	fh := &FileHandle{}

	if isVirtual {
		fh.isDir = true
		fh.isVirtual = true
		fh.virtualPath = path
	} else {
		syslog.L.Debug().WithMessage("handleOpenFile: opening path").WithField("path", path).Write()
		fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if err != nil {
			syslog.L.Error(err).WithMessage("handleOpenFile: open failed").WithField("path", path).Write()
			return arpc.Response{}, fmt.Errorf("open failed: %w", err)
		}

		var st unix.Stat_t
		if err := unix.Fstat(fd, &st); err != nil {
			_ = unix.Close(fd)
			syslog.L.Error(err).WithMessage("handleOpenFile: fstat failed").Write()
			return arpc.Response{}, err
		}

		fh.isDir = (st.Mode & unix.S_IFMT) == unix.S_IFDIR

		fh.fileSize = int64(st.Size)

		if fh.isDir {
			reader, err := NewDirReaderUnix(fd, path)
			if err != nil {
				_ = unix.Close(fd)
				return arpc.Response{}, err
			}
			fh.dirReader = reader
		} else {
			fh.file = os.NewFile(uintptr(fd), path)
		}
	}

	s.handles.Set(handleId, fh)
	dataBytes, _ := cbor.Marshal(types.FileHandleId(handleId))

	syslog.L.Debug().WithMessage("handleOpenFile: success").
		WithField("handle_id", handleId).
		WithField("is_dir", fh.isDir).Write()

	return arpc.Response{Status: 200, Data: dataBytes}, nil
}

func (s *AgentFSServer) handleAttr(req *arpc.Request) (arpc.Response, error) {
	syslog.L.Debug().WithMessage("handleAttr: decoding request").Write()
	var payload types.StatReq
	if err := cbor.Unmarshal(req.Payload, &payload); err != nil {
		syslog.L.Error(err).WithMessage("handleAttr: decode failed").Write()
		return arpc.Response{}, err
	}

	fullPath, isVirtual, err := s.abs(payload.Path)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleAttr: abs failed").WithField("path", payload.Path).Write()
		return arpc.Response{}, err
	}

	if isVirtual {
		info := types.AgentFileInfo{
			Name:  lastPathElem(payload.Path),
			Mode:  uint32(os.ModeDir | 0555),
			IsDir: true,
		}
		data, _ := cbor.Marshal(info)
		return arpc.Response{Status: 200, Data: data}, nil
	}

	var st unix.Stat_t
	if err := unix.Fstatat(unix.AT_FDCWD, fullPath, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		if !strings.HasSuffix(fullPath, ".pxarexclude") {
			syslog.L.Error(err).WithMessage("handleAttr: fstatat failed").WithField("path", fullPath).Write()
		}
		return arpc.Response{}, err
	}

	blockSize := s.statFs.Bsize
	if blockSize == 0 {
		blockSize = 4096
	}

	isDir := (st.Mode & unix.S_IFMT) == unix.S_IFDIR
	var blocks uint64
	if !isDir {
		blocks = uint64((st.Size + int64(blockSize) - 1) / int64(blockSize))
	}

	info := types.AgentFileInfo{
		Name:    lastPathElem(fullPath),
		Size:    st.Size,
		Mode:    uint32(modeFromUnix(uint32(st.Mode))),
		ModTime: time.Unix(int64(st.Mtim.Sec), int64(st.Mtim.Nsec)).UnixNano(),
		IsDir:   isDir,
		Blocks:  blocks,
	}

	data, err := cbor.Marshal(info)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleAttr: encode failed").Write()
		return arpc.Response{}, err
	}

	syslog.L.Debug().WithMessage("handleAttr: success").WithField("path", fullPath).Write()
	return arpc.Response{Status: 200, Data: data}, nil
}

func (s *AgentFSServer) handleXattr(req *arpc.Request) (arpc.Response, error) {
	syslog.L.Debug().WithMessage("handleXattr: decoding request").Write()
	var payload types.StatReq
	if err := cbor.Unmarshal(req.Payload, &payload); err != nil {
		syslog.L.Error(err).WithMessage("handleXattr: decode failed").Write()
		return arpc.Response{}, err
	}

	fullPath, isVirtual, err := s.abs(payload.Path)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleXattr: abs failed").WithField("path", payload.Path).Write()
		return arpc.Response{}, err
	}

	if isVirtual {
		return arpc.Response{}, fmt.Errorf("handleXattr: virtual dirs do not support xattr")
	}

	var st unix.Stat_t
	if err := unix.Fstatat(unix.AT_FDCWD, fullPath, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		syslog.L.Error(err).WithMessage("handleXattr: fstatat failed").WithField("path", fullPath).Write()
		return arpc.Response{}, err
	}

	owner := ""
	group := ""

	s.writeIDString(&owner, st.Uid)
	s.writeIDString(&group, st.Gid)

	acls, err := GetUnixACLs(fullPath, -1)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleXattr: GetUnixACLs failed").WithField("path", fullPath).Write()
		return arpc.Response{}, err
	}

	info := types.AgentFileInfo{
		CreationTime:   getBirthTime(&st),
		LastAccessTime: time.Unix(int64(st.Atim.Sec), int64(st.Atim.Nsec)).Unix(),
		LastWriteTime:  time.Unix(int64(st.Mtim.Sec), int64(st.Mtim.Nsec)).Unix(),
		Owner:          owner,
		Group:          group,
		PosixACLs:      acls,
	}

	data, err := cbor.Marshal(info)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleXattr: encode failed").WithField("path", fullPath).Write()
		return arpc.Response{}, err
	}

	syslog.L.Debug().WithMessage("handleXattr: success").
		WithField("path", fullPath).
		WithField("acl_count", len(acls)).
		Write()

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
		syslog.L.Warn().WithMessage("handleReadDir: handle not found").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, os.ErrNotExist
	}
	fh.Lock()
	defer fh.Unlock()

	if fh.isVirtual {
		cleanSource := strings.Trim(s.snapshot.SourcePath, "/")
		searchPath := strings.Trim(fh.virtualPath, "/")

		subStr := cleanSource
		if searchPath != "" {
			subStr = strings.TrimPrefix(strings.TrimPrefix(cleanSource, searchPath), "/")
		}

		parts := strings.Split(subStr, "/")
		if subStr == "" || len(parts) == 0 {
			return arpc.Response{}, os.ErrNotExist
		}

		nextElem := parts[0]

		entry := types.AgentFileInfo{
			Name:  nextElem,
			IsDir: true,
			Mode:  uint32(os.ModeDir | 0555),
		}

		encodedBatch, _ := cbor.Marshal([]types.AgentFileInfo{entry})

		return arpc.Response{Status: 213, RawStream: func(stream *smux.Stream) {
			defer fh.releaseOp()
			binarystream.SendDataFromReader(bytes.NewReader(encodedBatch), len(encodedBatch), stream)
			s.handleDirClose(uint64(payload.HandleID), fh)
		}}, nil
	}

	encodedBatch, err := fh.dirReader.NextBatch()
	isDone := errors.Is(err, os.ErrProcessDone)
	if err != nil && !isDone {
		fh.releaseOp()
		return arpc.Response{}, err
	}

	syslog.L.Debug().WithMessage("handleReadDir: sending batch").
		WithField("handle_id", payload.HandleID).
		WithField("is_last", isDone).Write()

	byteReader := bytes.NewReader(encodedBatch)
	streamCallback := func(stream *smux.Stream) {
		if err := binarystream.SendDataFromReader(byteReader, len(encodedBatch), stream); err != nil {
			syslog.L.Error(err).WithMessage("handleReadDir: failed sending data from reader").WithField("handle_id", payload.HandleID).Write()
		}

		if isDone {
			s.handleDirClose(uint64(payload.HandleID), fh)
		}
	}
	return arpc.Response{Status: 213, RawStream: streamCallback}, nil
}

func (s *AgentFSServer) handleReadAt(req *arpc.Request) (arpc.Response, error) {
	syslog.L.Debug().WithMessage("handleReadAt: decoding request").Write()
	var payload types.ReadAtReq
	if err := cbor.Unmarshal(req.Payload, &payload); err != nil {
		syslog.L.Error(err).WithMessage("handleReadAt: decode failed").Write()
		return arpc.Response{}, err
	}
	fh, exists := s.handles.Get(uint64(payload.HandleID))
	if !exists {
		syslog.L.Warn().WithMessage("handleReadAt: handle not found").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, os.ErrNotExist
	}

	if !fh.acquireOp() {
		syslog.L.Debug().WithMessage("handleReadAt: handle is closing").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, os.ErrClosed
	}

	fh.Lock()
	if fh.file == nil {
		fh.Unlock()
		fh.releaseOp()
		return arpc.Response{}, os.ErrClosed
	}
	fd := int(fh.file.Fd())
	fileSize := fh.fileSize
	readMode := s.readMode
	allocGran := s.allocGranularity
	fh.Unlock()

	if payload.Offset >= fileSize {
		fh.releaseOp()
		syslog.L.Debug().WithMessage("handleReadAt: offset beyond EOF").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{Status: 213, RawStream: func(stream *smux.Stream) {
			_ = binarystream.SendDataFromReader(bytes.NewReader(nil), 0, stream)
		}}, nil
	}

	if readMode == "mmap" {
		syslog.L.Debug().WithMessage("handleReadAt: attempting mmap read").WithField("handle_id", payload.HandleID).Write()
		alignedOffset := payload.Offset - (payload.Offset % int64(allocGran))
		offsetDiff := int(payload.Offset - alignedOffset)
		viewSize := payload.Length + offsetDiff
		data, err := unix.Mmap(fd, alignedOffset, viewSize, unix.PROT_READ, unix.MAP_SHARED)
		if err == nil {
			result := data[offsetDiff : offsetDiff+payload.Length]
			return arpc.Response{Status: 213, RawStream: func(stream *smux.Stream) {
				defer fh.releaseOp()
				defer unix.Munmap(data)
				_ = binarystream.SendDataFromReader(bytes.NewReader(result), len(result), stream)
				syslog.L.Debug().WithMessage("handleReadAt: stream completed (mmap)").WithField("bytes", len(result)).Write()
			}}, nil
		}
		syslog.L.Warn().WithMessage("handleReadAt: mmap failed, falling back to pread").Write()
	}

	bptr := readBufPool.Get().(*[]byte)
	workBuf := *bptr
	isTemporary := false
	if len(workBuf) < payload.Length {
		workBuf = make([]byte, payload.Length)
		isTemporary = true
	}

	n, err := unix.Pread(fd, workBuf[:payload.Length], payload.Offset)
	if err != nil {
		if !isTemporary {
			readBufPool.Put(bptr)
		}
		fh.releaseOp()
		syslog.L.Error(err).WithMessage("handleReadAt: pread failed").Write()
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
	fh, exists := s.handles.Get(uint64(payload.HandleID))
	if !exists {
		syslog.L.Warn().WithMessage("handleLseek: handle not found").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, os.ErrNotExist
	}

	if !fh.acquireOp() {
		syslog.L.Debug().WithMessage("handleLseek: handle is closing").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, os.ErrClosed
	}
	defer fh.releaseOp()

	fh.Lock()
	defer fh.Unlock()
	var newOffset int64
	switch payload.Whence {
	case io.SeekStart:
		newOffset = payload.Offset
	case io.SeekCurrent:
		newOffset = fh.curOffset + payload.Offset
	case io.SeekEnd:
		newOffset = fh.fileSize + payload.Offset
	default:
		return arpc.Response{}, os.ErrInvalid
	}
	if newOffset < 0 || newOffset > fh.fileSize {
		return arpc.Response{}, os.ErrInvalid
	}
	fh.curOffset = newOffset
	respBytes, _ := cbor.Marshal(types.LseekResp{NewOffset: newOffset})
	syslog.L.Debug().WithMessage("handleLseek: success").WithField("new_offset", newOffset).Write()
	return arpc.Response{Status: 200, Data: respBytes}, nil
}

func (s *AgentFSServer) handleDirClose(id uint64, fh *FileHandle) {
	if !fh.beginClose() {
		return
	}

	fh.Lock()
	if fh.file != nil {
		_ = fh.file.Close()
		fh.file = nil
	}
	if fh.dirReader != nil {
		_ = fh.dirReader.Close()
		fh.dirReader = nil
	}
	fh.Unlock()

	s.handles.Del(id)
	syslog.L.Debug().WithMessage("autoCloseHandle: handle closed and removed").
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
		syslog.L.Warn().WithMessage("handleClose: handle not found").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, os.ErrNotExist
	}

	if !handle.beginClose() {
		syslog.L.Debug().WithMessage("handleClose: handle already closing").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{Status: 200}, nil
	}

	select {
	case <-handle.closeDone:
	case <-time.After(30 * time.Second):
		syslog.L.Warn().WithMessage("handleClose: timeout waiting for operations, forcing close").WithField("handle_id", payload.HandleID).Write()
	}

	handle.Lock()
	defer handle.Unlock()

	if handle.file != nil {
		_ = handle.file.Close()
		handle.file = nil
	}
	if handle.dirReader != nil {
		_ = handle.dirReader.Close()
		handle.dirReader = nil
	}

	s.handles.Del(uint64(payload.HandleID))

	data, _ := cbor.Marshal("closed")
	syslog.L.Debug().WithMessage("handleClose: success").WithField("handle_id", payload.HandleID).Write()
	return arpc.Response{Status: 200, Data: data}, nil
}
