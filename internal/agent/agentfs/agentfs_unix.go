//go:build unix

package agentfs

import (
	"bytes"
	"fmt"
	"io"
	"os"
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
	fd          int
	dirPath     string
	fileSize    int64
	isDir       bool
	dirReader   *DirReaderUnix
	cachedInode uint64
	curOffset   int64
}

var readBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 1024*1024)
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

	path, err := s.abs(payload.Path)
	if err != nil {
		return arpc.Response{}, err
	}

	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleOpenFile: open failed").WithField("path", path).Write()
		return arpc.Response{}, fmt.Errorf("open failed: %w", err)
	}

	var stx unix.Statx_t
	mask := uint32(unix.STATX_TYPE | unix.STATX_SIZE)
	if err := unix.Statx(fd, "", unix.AT_EMPTY_PATH|unix.AT_STATX_DONT_SYNC, int(mask), &stx); err != nil {
		_ = unix.Close(fd)
		syslog.L.Error(err).WithMessage("handleOpenFile: statx failed").Write()
		return arpc.Response{}, err
	}

	handleId := s.handleIdGen.NextID()
	var fh *FileHandle
	isDir := (stx.Mode & unix.S_IFMT) == unix.S_IFDIR

	if isDir {
		reader, err := NewDirReaderUnix(fd, path)
		if err != nil {
			_ = unix.Close(fd)
			return arpc.Response{}, err
		}
		fh = &FileHandle{dirReader: reader, isDir: true, fd: fd}
	} else {
		fh = &FileHandle{
			file:     os.NewFile(uintptr(fd), path),
			fd:       fd,
			fileSize: int64(stx.Size),
			isDir:    false,
		}
	}

	s.handles.Set(handleId, fh)
	dataBytes, _ := cbor.Marshal(types.FileHandleId(handleId))

	syslog.L.Debug().WithMessage("handleOpenFile: success").
		WithField("handle_id", handleId).
		WithField("is_dir", isDir).Write()

	return arpc.Response{Status: 200, Data: dataBytes}, nil
}

func (s *AgentFSServer) handleAttr(req *arpc.Request) (arpc.Response, error) {
	syslog.L.Debug().WithMessage("handleAttr: decoding request").Write()
	var payload types.StatReq
	if err := cbor.Unmarshal(req.Payload, &payload); err != nil {
		syslog.L.Error(err).WithMessage("handleAttr: decode failed").Write()
		return arpc.Response{}, err
	}

	fullPath, err := s.abs(payload.Path)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleAttr: abs failed").WithField("path", payload.Path).Write()
		return arpc.Response{}, err
	}

	var stx unix.Statx_t
	mask := uint32(unix.STATX_MODE | unix.STATX_SIZE | unix.STATX_MTIME)
	flags := unix.AT_SYMLINK_NOFOLLOW | unix.AT_STATX_DONT_SYNC

	if err := unix.Statx(unix.AT_FDCWD, fullPath, flags, int(mask), &stx); err != nil {
		if !strings.HasSuffix(fullPath, ".pxarexclude") {
			syslog.L.Error(err).WithMessage("handleAttr: statx failed").WithField("path", fullPath).Write()
		}
		return arpc.Response{}, err
	}

	blockSize := s.statFs.Bsize
	if blockSize == 0 {
		blockSize = 4096
	}

	isDir := (stx.Mode & unix.S_IFMT) == unix.S_IFDIR
	var blocks uint64
	if !isDir {
		blocks = uint64((int64(stx.Size) + int64(blockSize) - 1) / int64(blockSize))
	}

	info := types.AgentFileInfo{
		Name:    lastPathElem(fullPath),
		Size:    int64(stx.Size),
		Mode:    uint32(modeFromUnix(uint32(stx.Mode))),
		ModTime: time.Unix(int64(stx.Mtime.Sec), int64(stx.Mtime.Nsec)).UnixNano(),
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
	fullPath, err := s.abs(payload.Path)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleXattr: abs failed").WithField("path", payload.Path).Write()
		return arpc.Response{}, err
	}

	var stx unix.Statx_t
	// STATX_BTIME is 'birth time' (creation time), supported on newer kernels/filesystems (ext4, btrfs, xfs)
	mask := uint32(unix.STATX_UID | unix.STATX_GID | unix.STATX_ATIME | unix.STATX_MTIME | unix.STATX_BTIME)
	flags := unix.AT_SYMLINK_NOFOLLOW | unix.AT_STATX_DONT_SYNC

	if err := unix.Statx(unix.AT_FDCWD, fullPath, flags, int(mask), &stx); err != nil {
		syslog.L.Error(err).WithMessage("handleXattr: statx failed").WithField("path", fullPath).Write()
		return arpc.Response{}, err
	}

	owner := getIDString(stx.Uid)
	group := getIDString(stx.Gid)

	fileAttributes := make(map[string]bool)
	if (stx.Attributes_mask & unix.STATX_ATTR_IMMUTABLE) != 0 {
		fileAttributes["immutable"] = (stx.Attributes & unix.STATX_ATTR_IMMUTABLE) != 0
	}

	info := types.AgentFileInfo{
		// CreationTime (Btime) is 0 if not supported by the filesystem
		CreationTime:   time.Unix(int64(stx.Btime.Sec), int64(stx.Btime.Nsec)).Unix(),
		LastAccessTime: time.Unix(int64(stx.Atime.Sec), int64(stx.Atime.Nsec)).Unix(),
		LastWriteTime:  time.Unix(int64(stx.Mtime.Sec), int64(stx.Mtime.Nsec)).Unix(),
		Owner:          owner,
		Group:          group,
		FileAttributes: fileAttributes,
	}

	data, err := cbor.Marshal(info)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleXattr: encode failed").WithField("path", fullPath).Write()
		return arpc.Response{}, err
	}

	syslog.L.Debug().WithMessage("handleXattr: success").WithField("path", fullPath).Write()
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
	encodedBatch, err := fh.dirReader.NextBatch()
	if err != nil {
		return arpc.Response{}, err
	}
	syslog.L.Debug().WithMessage("handleReadDir: sending batch").WithField("handle_id", payload.HandleID).WithField("bytes", len(encodedBatch)).Write()
	byteReader := bytes.NewReader(encodedBatch)
	streamCallback := func(stream *smux.Stream) {
		if err := binarystream.SendDataFromReader(byteReader, len(encodedBatch), stream); err != nil {
			syslog.L.Error(err).WithMessage("handleReadDir: failed sending data from reader").WithField("handle_id", payload.HandleID).Write()
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
	fh.Lock()
	defer fh.Unlock()
	if payload.Offset >= fh.fileSize {
		syslog.L.Debug().WithMessage("handleReadAt: offset beyond EOF").WithField("handle_id", payload.HandleID).Write()
		return arpc.Response{Status: 213, RawStream: func(stream *smux.Stream) {
			_ = binarystream.SendDataFromReader(bytes.NewReader(nil), 0, stream)
		}}, nil
	}
	if s.readMode == "mmap" {
		syslog.L.Debug().WithMessage("handleReadAt: attempting mmap read").WithField("handle_id", payload.HandleID).Write()
		alignedOffset := payload.Offset - (payload.Offset % int64(s.allocGranularity))
		offsetDiff := int(payload.Offset - alignedOffset)
		viewSize := payload.Length + offsetDiff
		data, err := unix.Mmap(fh.fd, alignedOffset, viewSize, unix.PROT_READ, unix.MAP_SHARED)
		if err == nil {
			result := data[offsetDiff : offsetDiff+payload.Length]
			return arpc.Response{Status: 213, RawStream: func(stream *smux.Stream) {
				defer unix.Munmap(data)
				_ = binarystream.SendDataFromReader(bytes.NewReader(result), len(result), stream)
				syslog.L.Debug().WithMessage("handleReadAt: stream completed (mmap)").WithField("bytes", len(result)).Write()
			}}, nil
		}
		syslog.L.Warn().WithMessage("handleReadAt: mmap failed, falling back to pread").Write()
	}
	bptr := readBufPool.Get().(*[]byte)
	buf := *bptr
	if len(buf) < payload.Length {
		buf = make([]byte, payload.Length)
	}
	n, err := unix.Pread(fh.fd, buf[:payload.Length], payload.Offset)
	if err != nil {
		readBufPool.Put(bptr)
		syslog.L.Error(err).WithMessage("handleReadAt: pread failed").Write()
		return arpc.Response{}, err
	}
	return arpc.Response{Status: 213, RawStream: func(stream *smux.Stream) {
		defer readBufPool.Put(bptr)
		_ = binarystream.SendDataFromReader(bytes.NewReader(buf[:n]), n, stream)
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
	handle.Lock()
	defer handle.Unlock()
	if handle.file != nil {
		_ = handle.file.Close()
	} else if handle.dirReader != nil {
		_ = handle.dirReader.Close()
	}
	s.handles.Del(uint64(payload.HandleID))
	data, _ := cbor.Marshal("closed")
	syslog.L.Debug().WithMessage("handleClose: success").WithField("handle_id", payload.HandleID).Write()
	return arpc.Response{Status: 200, Data: data}, nil
}
