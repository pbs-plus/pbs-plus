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
	"github.com/xtaci/smux"
	"golang.org/x/sys/unix"
)

type FileHandle struct {
	sync.Mutex
	file        *os.File
	fd          int // raw fd for faster unix ops
	dirPath     string
	fileSize    int64
	isDir       bool
	dirReader   *DirReaderUnix
	cachedInode uint64
	curOffset   int64
}

// small pooled buffers for ReadAt
var readBufPool = sync.Pool{
	New: func() interface{} {
		// Default chunk; many readAt requests are small. Adjust as needed.
		b := make([]byte, 512*1024)
		return &b
	},
}

func (s *AgentFSServer) abs(filename string) (string, error) {
	if filename == "" || filename == "." || filename == "/" {
		return s.snapshot.Path, nil
	}

	path := pathjoin.Join(s.snapshot.Path, filename)
	return path, nil
}

func (s *AgentFSServer) closeFileHandles() {
	s.handles.ForEach(func(u uint64, fh *FileHandle) bool {
		fh.Lock()
		if fh.file != nil {
			fh.file.Close()
		}
		if fh.dirReader != nil {
			fh.dirReader.Close()
		}
		fh.Unlock()

		return true
	})

	s.handles.Clear()
}

func (s *AgentFSServer) initializeStatFS() error {
	var err error

	s.statFs, err = getStatFS(s.snapshot.SourcePath)
	if err != nil {
		return err
	}

	return nil
}

func getStatFS(path string) (types.StatFS, error) {
	// Clean and validate the path
	path = strings.TrimSpace(path)
	if path == "" {
		return types.StatFS{}, fmt.Errorf("path cannot be empty")
	}

	// Use unix.Statfs to get filesystem statistics
	var statfs unix.Statfs_t
	err := unix.Statfs(path, &statfs)
	if err != nil {
		return types.StatFS{}, fmt.Errorf(
			"failed to get filesystem stats for path %s: %w",
			path,
			err,
		)
	}

	// Map the unix.Statfs_t fields to the types.StatFS structure
	// Use explicit type conversions to handle different field types across Unix systems
	stat := types.StatFS{
		Bsize:  uint64(statfs.Bsize),  // Block size
		Blocks: uint64(statfs.Blocks), // Total number of blocks
		Bfree:  uint64(statfs.Bfree),  // Free blocks
		Bavail: uint64(statfs.Bavail), // Available blocks to unprivileged users
		Files:  uint64(statfs.Files),  // Total number of inodes
		Ffree:  uint64(statfs.Ffree),  // Free inodes
	}

	// Handle NameLen field which varies across Unix systems
	// Linux: Namelen, FreeBSD: doesn't exist, macOS: varies
	stat.NameLen = getNameLen(statfs)

	return stat, nil
}

// getNameLen extracts the maximum filename length in a cross-platform way
func getNameLen(statfs unix.Statfs_t) uint64 {
	// Use build tags or runtime detection for different systems
	return getNameLenPlatform(statfs)
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

	// Disallow write operations
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

	// Open with unix.Open to minimize overhead and then fstat
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return arpc.Response{}, fmt.Errorf("open failed '%s': %w", path, err)
	}

	// Determine if directory via fstat once
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		unix.Close(fd)
		return arpc.Response{}, fmt.Errorf("fstat failed '%s': %w", path, err)
	}

	handleId := s.handleIdGen.NextID()
	var fh *FileHandle

	if (st.Mode & unix.S_IFMT) == unix.S_IFDIR {
		// Re-open with directory flags to support getdents cleanly
		unix.Close(fd)
		reader, err := NewDirReaderUnix(path)
		if err != nil {
			return arpc.Response{}, err
		}
		fh = &FileHandle{
			dirReader: reader,
			isDir:     true,
		}
	} else {
		// Create *os.File from fd for compatibility with code using fh.file.
		file := os.NewFile(uintptr(fd), path)
		if file == nil {
			unix.Close(fd)
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

	fullPath, err := s.abs(payload.Path)
	if err != nil {
		return arpc.Response{}, err
	}

	// Prefer fstatat (no open) for path-based stat, and reuse fh if already open.
	var st unix.Stat_t
	// Try Fstatat without following symlinks
	if err := unix.Fstatat(unix.AT_FDCWD, fullPath, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return arpc.Response{}, err
	}

	blocks := uint64(0)
	if (st.Mode&unix.S_IFMT) != unix.S_IFDIR && s.statFs.Bsize != 0 {
		blocks = uint64((st.Size + int64(s.statFs.Bsize) - 1) / int64(s.statFs.Bsize))
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
		return arpc.Response{}, err
	}
	return arpc.Response{Status: 200, Data: data}, nil
}

func (s *AgentFSServer) handleXattr(req arpc.Request) (arpc.Response, error) {
	var payload types.StatReq
	if err := payload.Decode(req.Payload); err != nil {
		return arpc.Response{}, err
	}

	fullPath, err := s.abs(payload.Path)
	if err != nil {
		return arpc.Response{}, err
	}

	var st unix.Stat_t
	if err := unix.Fstatat(unix.AT_FDCWD, fullPath, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return arpc.Response{}, err
	}

	// Times: use st.Atim, st.Mtim, st.Ctim; Creation time often unavailable on Linux
	creationTime := int64(0)
	lastAccessTime := time.Unix(int64(st.Atim.Sec), int64(st.Atim.Nsec)).Unix()
	lastWriteTime := time.Unix(int64(st.Mtim.Sec), int64(st.Mtim.Nsec)).Unix()

	uidStr := strconv.Itoa(int(st.Uid))
	gidStr := strconv.Itoa(int(st.Gid))

	owner := uidStr
	group := gidStr

	fileAttributes := make(map[string]bool)

	posixAcls, _ := getPosixACL(fullPath) // ignore error; optional

	info := types.AgentFileInfo{
		CreationTime:   creationTime,
		LastAccessTime: lastAccessTime,
		LastWriteTime:  lastWriteTime,
		FileAttributes: fileAttributes,
		Owner:          owner,
		Group:          group,
		PosixACLs:      posixAcls,
	}

	data, err := info.Encode()
	if err != nil {
		return arpc.Response{}, err
	}
	return arpc.Response{Status: 200, Data: data}, nil
}

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

	// If offset >= EOF, send empty
	if payload.Offset >= fh.fileSize {
		emptyReader := bytes.NewReader(nil)
		streamCallback := func(stream *smux.Stream) {
			if err := binarystream.SendDataFromReader(emptyReader, 0, stream); err != nil {
				syslog.L.Error(err).WithMessage("failed sending empty reader").Write()
			}
		}
		return arpc.Response{Status: 213, RawStream: streamCallback}, nil
	}

	// Clamp length if beyond EOF
	if payload.Offset+int64(payload.Length) > fh.fileSize {
		payload.Length = int(fh.fileSize - payload.Offset)
	}
	if payload.Length == 0 {
		emptyReader := bytes.NewReader(nil)
		streamCallback := func(stream *smux.Stream) {
			_ = binarystream.SendDataFromReader(emptyReader, 0, stream)
		}
		return arpc.Response{Status: 213, RawStream: streamCallback}, nil
	}

	// Align and mmap if requested; else use pooled buffer + pread
	alignedOffset := payload.Offset - (payload.Offset % int64(s.allocGranularity))
	offsetDiff := int(payload.Offset - alignedOffset)
	viewSize := payload.Length + offsetDiff

	if s.readMode == "mmap" {
		data, err := unix.Mmap(fh.fd, alignedOffset, viewSize, unix.PROT_READ, unix.MAP_SHARED)
		if err == nil {
			// bounds check
			if offsetDiff+payload.Length > len(data) {
				unix.Munmap(data)
				return arpc.Response{}, fmt.Errorf("invalid file mapping boundaries")
			}
			result := data[offsetDiff : offsetDiff+payload.Length]
			reader := bytes.NewReader(result)
			streamCallback := func(stream *smux.Stream) {
				defer unix.Munmap(data)
				_ = binarystream.SendDataFromReader(reader, len(result), stream)
			}
			return arpc.Response{Status: 213, RawStream: streamCallback}, nil
		}
		// fall through to pread on mmap failure
	}

	// pooled buffer
	bptr := readBufPool.Get().(*[]byte)
	buf := *bptr
	if len(buf) < payload.Length {
		buf = make([]byte, payload.Length)
	}
	n, err := unix.Pread(fh.fd, buf[:payload.Length], payload.Offset)
	if err != nil {
		readBufPool.Put(bptr)
		return arpc.Response{}, fmt.Errorf("pread failed: %w", err)
	}
	// Capture n and buffer for the stream; return buffer to pool after send.
	reader := bytes.NewReader(buf[:n])
	streamCallback := func(stream *smux.Stream) {
		_ = binarystream.SendDataFromReader(reader, n, stream)
		readBufPool.Put(bptr)
	}
	return arpc.Response{Status: 213, RawStream: streamCallback}, nil
}

func (s *AgentFSServer) handleLseek(req arpc.Request) (arpc.Response, error) {
	var payload types.LseekReq
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

	fh.Lock()
	defer fh.Unlock()

	// Unsupported
	if payload.Whence == SeekHole || payload.Whence == SeekData {
		return arpc.Response{}, os.ErrInvalid
	}

	var newOffset int64
	switch payload.Whence {
	case io.SeekStart:
		newOffset = payload.Offset
	case io.SeekCurrent:
		newOffset = fh.curOffset + payload.Offset
	case io.SeekEnd:
		newOffset = fh.fileSize + payload.Offset
	default:
		return arpc.Response{}, fmt.Errorf("invalid whence: %d", payload.Whence)
	}

	// disallow seeks outside [0, fileSize]
	if newOffset < 0 {
		return arpc.Response{}, fmt.Errorf("seeking before start is not allowed")
	}
	if newOffset > fh.fileSize {
		return arpc.Response{}, fmt.Errorf("seeking beyond EOF is not allowed")
	}

	fh.curOffset = newOffset

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

	if handle.file != nil {
		handle.file.Close()
	} else if handle.dirReader != nil {
		handle.dirReader.Close()
	}

	s.handles.Del(uint64(payload.HandleID))

	closed := arpc.StringMsg("closed")
	data, err := closed.Encode()
	if err != nil {
		return arpc.Response{}, err
	}

	return arpc.Response{Status: 200, Data: data}, nil
}
