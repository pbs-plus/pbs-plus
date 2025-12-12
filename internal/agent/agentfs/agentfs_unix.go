//go:build unix

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

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	binarystream "github.com/pbs-plus/pbs-plus/internal/arpc/binary"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils/pathjoin"
	"github.com/quic-go/quic-go"
	"golang.org/x/sys/unix"
)

type FileHandle struct {
	sync.Mutex
	file        *os.File
	fd          int
	dirPath     string
	fileSize    int64
	isDir       bool
	dirReader   *DirReaderUnix
	cachedInode uint64
	curOffset   int64
}

var readBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 512*1024)
		return &b
	},
}

func (s *AgentFSServer) abs(filename string) (string, error) {
	if filename == "" || filename == "." || filename == "/" {
		syslog.L.Debug().WithMessage("abs: returning snapshot path for root").Write()
		return s.snapshot.Path, nil
	}
	path := pathjoin.Join(s.snapshot.Path, filename)
	syslog.L.Debug().WithMessage("abs: joined path").WithField("path", path).Write()
	return path, nil
}

func (s *AgentFSServer) closeFileHandles() {
	syslog.L.Debug().WithMessage("closeFileHandles: closing all file handles").Write()
	s.handles.ForEach(func(u uint64, fh *FileHandle) bool {
		fh.Lock()
		if fh.file != nil {
			_ = fh.file.Close()
			syslog.L.Debug().WithMessage("closeFileHandles: closed file").WithField("handle_id", u).Write()
		}
		if fh.dirReader != nil {
			_ = fh.dirReader.Close()
			syslog.L.Debug().WithMessage("closeFileHandles: closed dir reader").WithField("handle_id", u).Write()
		}
		fh.Unlock()
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

func (s *AgentFSServer) handleStatFS(req arpc.Request) (arpc.Response, error) {
	syslog.L.Debug().WithMessage("handleStatFS: encoding statFs").Write()
	enc, err := s.statFs.Encode()
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

func (s *AgentFSServer) handleOpenFile(req arpc.Request) (arpc.Response, error) {
	syslog.L.Debug().WithMessage("handleOpenFile: decoding request").Write()
	var payload types.OpenFileReq
	if err := payload.Decode(req.Payload); err != nil {
		syslog.L.Error(err).WithMessage("handleOpenFile: decode failed").Write()
		return arpc.Response{}, err
	}
	if payload.Flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC) != 0 {
		syslog.L.Warn().WithMessage("handleOpenFile: write operation attempted and blocked").WithField("path", payload.Path).Write()
		errStr := arpc.StringMsg("write operations not allowed")
		errBytes, err := errStr.Encode()
		if err != nil {
			syslog.L.Error(err).WithMessage("handleOpenFile: encode error message failed").Write()
			return arpc.Response{}, err
		}
		return arpc.Response{
			Status: 403,
			Data:   errBytes,
		}, nil
	}
	path, err := s.abs(payload.Path)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleOpenFile: abs failed").WithField("path", payload.Path).Write()
		return arpc.Response{}, err
	}
	syslog.L.Debug().WithMessage("handleOpenFile: opening file").WithField("path", path).Write()
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleOpenFile: open failed").WithField("path", path).Write()
		return arpc.Response{}, fmt.Errorf("open failed '%s': %w", path, err)
	}
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		_ = unix.Close(fd)
		syslog.L.Error(err).WithMessage("handleOpenFile: fstat failed").WithField("path", path).Write()
		return arpc.Response{}, fmt.Errorf("fstat failed '%s': %w", path, err)
	}
	handleId := s.handleIdGen.NextID()
	var fh *FileHandle
	if (st.Mode & unix.S_IFMT) == unix.S_IFDIR {
		_ = unix.Close(fd)
		reader, err := NewDirReaderUnix(path)
		if err != nil {
			syslog.L.Error(err).WithMessage("handleOpenFile: NewDirReaderUnix failed").WithField("path", path).Write()
			return arpc.Response{}, err
		}
		fh = &FileHandle{
			dirReader: reader,
			isDir:     true,
		}
	} else {
		file := os.NewFile(uintptr(fd), path)
		if file == nil {
			_ = unix.Close(fd)
			syslog.L.Error(fmt.Errorf("wrap fd failed")).WithMessage("handleOpenFile: failed to wrap fd").WithField("path", path).Write()
			return arpc.Response{}, fmt.Errorf("failed to wrap fd for '%s'", path)
		}
		fh = &FileHandle{
			file:     file,
			fd:       fd,
			fileSize: st.Size,
			isDir:    false,
		}
	}
	s.handles.Set(handleId, fh)
	fhId := types.FileHandleId(handleId)
	dataBytes, err := fhId.Encode()
	if err != nil {
		if fh != nil {
			if fh.isDir && fh.dirReader != nil {
				_ = fh.dirReader.Close()
			} else if fh.fd != 0 {
				_ = unix.Close(fh.fd)
			}
		}
		syslog.L.Error(err).WithMessage("handleOpenFile: encode handle id failed").Write()
		return arpc.Response{}, err
	}
	syslog.L.Debug().WithMessage("handleOpenFile: success").WithField("handle_id", handleId).WithField("is_dir", fh.isDir).WithField("size", fh.fileSize).Write()
	return arpc.Response{
		Status: 200,
		Data:   dataBytes,
	}, nil
}

func (s *AgentFSServer) handleAttr(req arpc.Request) (arpc.Response, error) {
	syslog.L.Debug().WithMessage("handleAttr: decoding request").Write()
	var payload types.StatReq
	if err := payload.Decode(req.Payload); err != nil {
		syslog.L.Error(err).WithMessage("handleAttr: decode failed").Write()
		return arpc.Response{}, err
	}
	fullPath, err := s.abs(payload.Path)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleAttr: abs failed").WithField("path", payload.Path).Write()
		return arpc.Response{}, err
	}
	var st unix.Stat_t
	if err := unix.Fstatat(unix.AT_FDCWD, fullPath, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		syslog.L.Error(err).WithMessage("handleAttr: fstatat failed").WithField("path", fullPath).Write()
		return arpc.Response{}, err
	}
	blockSize := s.statFs.Bsize
	if blockSize == 0 {
		blockSize = 4096
	}
	blocks := uint64(0)
	if (st.Mode & unix.S_IFMT) != unix.S_IFDIR {
		blocks = uint64((st.Size + int64(blockSize) - 1) / int64(blockSize))
	}
	info := types.AgentFileInfo{
		Name:    lastPathElem(fullPath),
		Size:    st.Size,
		Mode:    uint32(modeFromUnix(uint32(st.Mode))),
		ModTime: time.Unix(int64(st.Mtim.Sec), int64(st.Mtim.Nsec)),
		IsDir:   (st.Mode & unix.S_IFMT) == unix.S_IFDIR,
		Blocks:  blocks,
	}
	data, err := info.Encode()
	if err != nil {
		syslog.L.Error(err).WithMessage("handleAttr: encode failed").WithField("path", fullPath).Write()
		return arpc.Response{}, err
	}
	syslog.L.Debug().WithMessage("handleAttr: success").WithField("path", fullPath).WithField("is_dir", info.IsDir).WithField("size", info.Size).Write()
	return arpc.Response{Status: 200, Data: data}, nil
}

func (s *AgentFSServer) handleXattr(req arpc.Request) (arpc.Response, error) {
	syslog.L.Debug().WithMessage("handleXattr: decoding request").Write()
	var payload types.StatReq
	if err := payload.Decode(req.Payload); err != nil {
		syslog.L.Error(err).WithMessage("handleXattr: decode failed").Write()
		return arpc.Response{}, err
	}
	fullPath, err := s.abs(payload.Path)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleXattr: abs failed").WithField("path", payload.Path).Write()
		return arpc.Response{}, err
	}
	var st unix.Stat_t
	if err := unix.Fstatat(unix.AT_FDCWD, fullPath, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		syslog.L.Error(err).WithMessage("handleXattr: fstatat failed").WithField("path", fullPath).Write()
		return arpc.Response{}, err
	}
	var creationTime int64
	lastAccessTime := time.Unix(int64(st.Atim.Sec), int64(st.Atim.Nsec)).Unix()
	lastWriteTime := time.Unix(int64(st.Mtim.Sec), int64(st.Mtim.Nsec)).Unix()
	uidStr := strconv.Itoa(int(st.Uid))
	gidStr := strconv.Itoa(int(st.Gid))
	owner := uidStr
	group := gidStr
	fileAttributes := make(map[string]bool)
	info := types.AgentFileInfo{
		CreationTime:   creationTime,
		LastAccessTime: lastAccessTime,
		LastWriteTime:  lastWriteTime,
		FileAttributes: fileAttributes,
		Owner:          owner,
		Group:          group,
	}
	data, err := info.Encode()
	if err != nil {
		syslog.L.Error(err).WithMessage("handleXattr: encode failed").WithField("path", fullPath).Write()
		return arpc.Response{}, err
	}
	syslog.L.Debug().WithMessage("handleXattr: success").WithField("path", fullPath).Write()
	return arpc.Response{Status: 200, Data: data}, nil
}

func (s *AgentFSServer) handleReadDir(req arpc.Request) (arpc.Response, error) {
	syslog.L.Debug().WithMessage("handleReadDir: decoding request").Write()
	var payload types.ReadDirReq
	if err := payload.Decode(req.Payload); err != nil {
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
	encodedBatch, err := fh.dirReader.NextBatch()
	if err != nil {
		if !errors.Is(err, os.ErrProcessDone) {
			syslog.L.Error(err).WithMessage("handleReadDir: error reading batch").WithField("handle_id", payload.HandleID).Write()
		} else {
			syslog.L.Debug().WithMessage("handleReadDir: directory read complete").WithField("handle_id", payload.HandleID).Write()
		}
		return arpc.Response{}, err
	}
	syslog.L.Debug().WithMessage("handleReadDir: sending batch").WithField("handle_id", payload.HandleID).WithField("bytes", len(encodedBatch)).Write()
	byteReader := bytes.NewReader(encodedBatch)
	streamCallback := func(stream *quic.Stream) {
		defer stream.Close()
		if err := binarystream.SendDataFromReader(byteReader, int(len(encodedBatch)), stream); err != nil {
			syslog.L.Error(err).WithMessage("handleReadDir: failed sending data from reader via binary stream").WithField("handle_id", payload.HandleID).Write()
		}
	}
	return arpc.Response{
		Status:    213,
		RawStream: streamCallback,
	}, nil
}

func (s *AgentFSServer) handleReadAt(req arpc.Request) (arpc.Response, error) {
	syslog.L.Debug().WithMessage("handleReadAt: decoding request").Write()
	var payload types.ReadAtReq
	if err := payload.Decode(req.Payload); err != nil {
		syslog.L.Error(err).WithMessage("handleReadAt: decode failed").Write()
		return arpc.Response{}, err
	}
	if payload.Length < 0 {
		syslog.L.Warn().WithMessage("handleReadAt: negative length requested").WithField("length", payload.Length).Write()
		return arpc.Response{}, fmt.Errorf("invalid negative length requested: %d", payload.Length)
	}
	fh, exists := s.handles.Get(uint64(payload.HandleID))
	if !exists {
		syslog.L.Warn().WithMessage("handleReadAt: handle not found").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, os.ErrNotExist
	}
	if fh.isDir {
		syslog.L.Warn().WithMessage("handleReadAt: attempted read on directory handle").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, os.ErrInvalid
	}
	fh.Lock()
	defer fh.Unlock()
	if payload.Offset >= fh.fileSize {
		syslog.L.Debug().WithMessage("handleReadAt: offset beyond EOF, sending empty").WithField("handle_id", payload.HandleID).WithField("offset", payload.Offset).WithField("size", fh.fileSize).Write()
		emptyReader := bytes.NewReader(nil)
		return arpc.Response{
			Status: 213,
			RawStream: func(stream *quic.Stream) {
				defer stream.Close()
				if err := binarystream.SendDataFromReader(emptyReader, 0, stream); err != nil {
					syslog.L.Error(err).WithMessage("handleReadAt: failed sending empty reader via binary stream").WithField("handle_id", payload.HandleID).Write()
				}
			},
		}, nil
	}
	maxEnd := fh.fileSize
	reqEnd := payload.Offset + int64(payload.Length)
	if reqEnd > maxEnd {
		reqEnd = maxEnd
	}
	if reqEnd <= payload.Offset {
		syslog.L.Debug().WithMessage("handleReadAt: empty range after clamp").WithField("handle_id", payload.HandleID).Write()
		emptyReader := bytes.NewReader(nil)
		return arpc.Response{
			Status: 213,
			RawStream: func(stream *quic.Stream) {
				defer stream.Close()
				if err := binarystream.SendDataFromReader(emptyReader, 0, stream); err != nil {
					syslog.L.Error(err).WithMessage("handleReadAt: failed sending empty reader via binary stream").WithField("handle_id", payload.HandleID).Write()
				}
			},
		}, nil
	}
	reqLen := int(reqEnd - payload.Offset)
	alignedOffset := payload.Offset - (payload.Offset % int64(s.allocGranularity))
	offsetDiff := int(payload.Offset - alignedOffset)
	viewSize := reqLen + offsetDiff
	if s.readMode == "mmap" {
		syslog.L.Debug().WithMessage("handleReadAt: attempting mmap read").WithField("handle_id", payload.HandleID).WithField("offset", payload.Offset).WithField("length", reqLen).Write()
		data, err := unix.Mmap(fh.fd, alignedOffset, viewSize, unix.PROT_READ, unix.MAP_SHARED)
		if err == nil {
			if offsetDiff+reqLen > len(data) {
				_ = unix.Munmap(data)
				syslog.L.Error(fmt.Errorf("mapping bounds")).WithMessage("handleReadAt: invalid file mapping boundaries").WithField("handle_id", payload.HandleID).Write()
				return arpc.Response{}, fmt.Errorf("invalid file mapping boundaries")
			}
			result := data[offsetDiff : offsetDiff+reqLen]
			reader := bytes.NewReader(result)
			syslog.L.Debug().WithMessage("handleReadAt: starting stream (mmap)").WithField("handle_id", payload.HandleID).WithField("offset", payload.Offset).WithField("length", reqLen).Write()
			streamCallback := func(stream *quic.Stream) {
				defer stream.Close()
				defer unix.Munmap(data)
				if err := binarystream.SendDataFromReader(reader, len(result), stream); err != nil {
					syslog.L.Error(err).WithMessage("handleReadAt: stream write data failed (mmap)").WithField("handle_id", payload.HandleID).Write()
					return
				}
				syslog.L.Debug().WithMessage("handleReadAt: stream completed").WithField("handle_id", payload.HandleID).WithField("bytes", len(result)).Write()
			}
			return arpc.Response{Status: 213, RawStream: streamCallback}, nil
		}
		syslog.L.Warn().WithMessage("handleReadAt: mmap failed, falling back to pread").WithField("handle_id", payload.HandleID).WithField("error", err.Error()).Write()
	}

	var buf []byte
	bptr := readBufPool.Get().(*[]byte) // This gets a pointer to a *slice*
	if len(*bptr) < reqLen {
		buf = make([]byte, reqLen) // Allocate a new slice
	} else {
		buf = *bptr // Use the slice from the pool
	}

	n, err := unix.Pread(fh.fd, buf[:reqLen], payload.Offset)
	if err != nil {
		if len(*bptr) >= reqLen { // Meaning, we used the pooled buffer
			readBufPool.Put(bptr)
		}
		syslog.L.Error(err).WithMessage("handleReadAt: pread failed").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, fmt.Errorf("pread failed: %w", err)
	}

	reader := bytes.NewReader(buf[:n])
	syslog.L.Debug().WithMessage("handleReadAt: starting stream").WithField("handle_id", payload.HandleID).WithField("offset", payload.Offset).WithField("length", n).Write()
	streamCallback := func(stream *quic.Stream) {
		defer stream.Close()
		readBufPool.Put(&buf)

		if err := binarystream.SendDataFromReader(reader, n, stream); err != nil {
			syslog.L.Error(err).WithMessage("handleReadAt: stream write data failed").WithField("handle_id", payload.HandleID).Write()
			return
		}
		syslog.L.Debug().WithMessage("handleReadAt: stream completed").WithField("handle_id", payload.HandleID).WithField("bytes", n).Write()
	}
	return arpc.Response{Status: 213, RawStream: streamCallback}, nil
}

func (s *AgentFSServer) handleLseek(req arpc.Request) (arpc.Response, error) {
	syslog.L.Debug().WithMessage("handleLseek: decoding request").Write()
	var payload types.LseekReq
	if err := payload.Decode(req.Payload); err != nil {
		syslog.L.Error(err).WithMessage("handleLseek: decode failed").Write()
		return arpc.Response{}, err
	}
	if payload.Whence != io.SeekStart &&
		payload.Whence != io.SeekCurrent &&
		payload.Whence != io.SeekEnd &&
		payload.Whence != SeekData &&
		payload.Whence != SeekHole {
		syslog.L.Warn().WithMessage("handleLseek: invalid whence").WithField("whence", payload.Whence).Write()
		return arpc.Response{}, os.ErrInvalid
	}
	fh, exists := s.handles.Get(uint64(payload.HandleID))
	if !exists {
		syslog.L.Warn().WithMessage("handleLseek: handle not found").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, os.ErrNotExist
	}
	if fh.isDir {
		syslog.L.Warn().WithMessage("handleLseek: attempted lseek on directory").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, os.ErrInvalid
	}
	fh.Lock()
	defer fh.Unlock()
	if payload.Whence == SeekHole || payload.Whence == SeekData {
		syslog.L.Warn().WithMessage("handleLseek: SEEK_HOLE/SEEK_DATA unsupported").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, os.ErrInvalid
	}
	var newOffset int64
	switch payload.Whence {
	case io.SeekStart:
		if payload.Offset < 0 {
			syslog.L.Warn().WithMessage("handleLseek: negative offset with SeekStart").WithField("offset", payload.Offset).Write()
			return arpc.Response{}, os.ErrInvalid
		}
		newOffset = payload.Offset
	case io.SeekCurrent:
		if fh.curOffset+payload.Offset < 0 {
			syslog.L.Warn().WithMessage("handleLseek: negative resulting offset with SeekCurrent").WithField("current", fh.curOffset).WithField("delta", payload.Offset).Write()
			return arpc.Response{}, os.ErrInvalid
		}
		newOffset = fh.curOffset + payload.Offset
	case io.SeekEnd:
		if fh.fileSize+payload.Offset < 0 {
			syslog.L.Warn().WithMessage("handleLseek: negative resulting offset with SeekEnd").WithField("file_size", fh.fileSize).WithField("delta", payload.Offset).Write()
			return arpc.Response{}, os.ErrInvalid
		}
		newOffset = fh.fileSize + payload.Offset
	default:
		syslog.L.Warn().WithMessage("handleLseek: invalid whence in default").Write()
		return arpc.Response{}, os.ErrInvalid
	}
	if newOffset > fh.fileSize {
		syslog.L.Warn().WithMessage("handleLseek: newOffset beyond EOF").WithField("new_offset", newOffset).WithField("file_size", fh.fileSize).Write()
		return arpc.Response{}, os.ErrInvalid
	}
	fh.curOffset = newOffset
	resp := types.LseekResp{NewOffset: newOffset}
	respBytes, err := resp.Encode()
	if err != nil {
		syslog.L.Error(err).WithMessage("handleLseek: encode failed").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, err
	}
	syslog.L.Debug().WithMessage("handleLseek: success").WithField("handle_id", payload.HandleID).WithField("new_offset", newOffset).Write()
	return arpc.Response{Status: 200, Data: respBytes}, nil
}

func (s *AgentFSServer) handleClose(req arpc.Request) (arpc.Response, error) {
	syslog.L.Debug().WithMessage("handleClose: decoding request").Write()
	var payload types.CloseReq
	if err := payload.Decode(req.Payload); err != nil {
		syslog.L.Error(err).WithMessage("handleClose: decode failed").Write()
		return arpc.Response{}, err
	}
	handle, exists := s.handles.Get(uint64(payload.HandleID))
	if !exists {
		syslog.L.Warn().WithMessage("handleClose: handle not found").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, os.ErrNotExist
	}
	handle.Lock()
	defer handle.Unlock()
	if handle.file != nil {
		_ = handle.file.Close()
		syslog.L.Debug().WithMessage("handleClose: file handle closed").WithField("handle_id", payload.HandleID).Write()
	} else if handle.dirReader != nil {
		_ = handle.dirReader.Close()
		syslog.L.Debug().WithMessage("handleClose: dir reader closed").WithField("handle_id", payload.HandleID).Write()
	}
	s.handles.Del(uint64(payload.HandleID))
	closed := arpc.StringMsg("closed")
	data, err := closed.Encode()
	if err != nil {
		syslog.L.Error(err).WithMessage("handleClose: encode close message failed").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{}, err
	}
	syslog.L.Debug().WithMessage("handleClose: success").WithField("handle_id", payload.HandleID).Write()
	return arpc.Response{Status: 200, Data: data}, nil
}
