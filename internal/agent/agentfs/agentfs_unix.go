//go:build unix

package agentfs

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/user"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

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
	dirPath     string
	fileSize    int64
	isDir       bool
	dirReturned atomic.Bool
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
		fh.file.Close()
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

	path, err := s.abs(payload.Path)
	if err != nil {
		return arpc.Response{}, err
	}

	// Check file status to mark directories.
	stat, err := os.Stat(path)
	if err != nil {
		return arpc.Response{}, err
	}

	handleId := s.handleIdGen.NextID()

	var fh *FileHandle

	if !stat.IsDir() {
		file, err := os.Open(path)
		if err != nil {
			return arpc.Response{}, err
		}

		fh = &FileHandle{
			file:     file,
			fileSize: stat.Size(),
			isDir:    false,
		}
	} else {
		fh = &FileHandle{
			dirPath: path,
			isDir:   true,
		}
	}

	s.handles.Set(handleId, fh)

	// Return the handle ID to the client.
	fhId := types.FileHandleId(handleId)
	dataBytes, err := fhId.Encode()
	if err != nil {
		if !fh.isDir {
			fh.file.Close()
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

	rawInfo, err := os.Stat(fullPath)
	if err != nil {
		return arpc.Response{}, err
	}

	blocks := uint64(0)
	if !rawInfo.IsDir() && s.statFs.Bsize != 0 {
		blocks = uint64((rawInfo.Size() + int64(s.statFs.Bsize) - 1) / int64(s.statFs.Bsize))
	}

	info := types.AgentFileInfo{
		Name:    rawInfo.Name(),
		Size:    rawInfo.Size(),
		Mode:    uint32(rawInfo.Mode()),
		ModTime: rawInfo.ModTime(),
		IsDir:   rawInfo.IsDir(),
		Blocks:  blocks,
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

func (s *AgentFSServer) handleXattr(req arpc.Request) (arpc.Response, error) {
	var payload types.StatReq
	if err := payload.Decode(req.Payload); err != nil {
		return arpc.Response{}, err
	}

	fullPath, err := s.abs(payload.Path)
	if err != nil {
		return arpc.Response{}, err
	}

	rawInfo, err := os.Stat(fullPath)
	if err != nil {
		return arpc.Response{}, err
	}

	blocks := uint64(0)
	if !rawInfo.IsDir() && s.statFs.Bsize != 0 {
		blocks = uint64((rawInfo.Size() + int64(s.statFs.Bsize) - 1) / int64(s.statFs.Bsize))
	}

	// Initialize default values.
	creationTime := int64(0)
	lastAccessTime := int64(0)
	lastWriteTime := int64(0)
	fileAttributes := make(map[string]bool)
	owner := ""
	group := ""

	if stat, ok := rawInfo.Sys().(*syscall.Stat_t); ok {
		uidStr := strconv.Itoa(int(stat.Uid))
		groupStr := strconv.Itoa(int(stat.Gid))
		usr, err := user.LookupId(uidStr)
		if err == nil {
			owner = usr.Username
		} else {
			owner = uidStr
		}
		grp, err := user.LookupGroupId(groupStr)
		if err == nil {
			group = grp.Name
		} else {
			group = groupStr
		}
		// Use the file's modification time as a fallback.
		lastAccessTime = rawInfo.ModTime().Unix()
		lastWriteTime = rawInfo.ModTime().Unix()
	}

	// Get POSIX ACL entries.
	posixAcls, err := getPosixACL(fullPath)
	if err != nil {
		// Optionally log the error and continue.
	}

	info := types.AgentFileInfo{
		Name:           rawInfo.Name(),
		Size:           rawInfo.Size(),
		Mode:           uint32(rawInfo.Mode()),
		ModTime:        rawInfo.ModTime(),
		IsDir:          rawInfo.IsDir(),
		Blocks:         blocks,
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
	return arpc.Response{
		Status: 200,
		Data:   data,
	}, nil
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

	if fh.dirReturned.Load() {
		return arpc.Response{}, os.ErrProcessDone
	}

	entries, err := readDirBulk(fh.dirPath)
	if err != nil {
		return arpc.Response{}, err
	}

	fh.dirReturned.Store(true)

	reader := bytes.NewReader(entries)
	streamCallback := func(stream *smux.Stream) {
		if err := binarystream.SendDataFromReader(reader, int(len(entries)), stream); err != nil {
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

	fh, exists := s.handles.Get(uint64(payload.HandleID))
	if !exists {
		return arpc.Response{}, os.ErrNotExist
	}
	if fh.isDir {
		return arpc.Response{}, os.ErrInvalid
	}

	buf := make([]byte, payload.Length)
	n, err := fh.file.ReadAt(buf, payload.Offset)
	if err != nil && err != io.EOF {
		return arpc.Response{}, err
	}

	reader := bytes.NewReader(buf[:n])
	streamCallback := func(stream *smux.Stream) {
		err := binarystream.SendDataFromReader(reader, n, stream)
		if err != nil {
			syslog.L.Error(err).WithMessage("failed sending data from reader via binary stream").Write()
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

	fh, exists := s.handles.Get(uint64(payload.HandleID))
	if !exists {
		return arpc.Response{}, os.ErrNotExist
	}
	if fh.isDir {
		return arpc.Response{}, os.ErrInvalid
	}

	fh.Lock()
	defer fh.Unlock()

	// Handle SEEK_HOLE and SEEK_DATA explicitly
	// TODO: linux implementation
	if payload.Whence == SeekHole || payload.Whence == SeekData {
		return arpc.Response{}, os.ErrInvalid
	}

	// Get the file size
	fileInfo, err := fh.file.Stat()
	if err != nil {
		return arpc.Response{}, err
	}
	fileSize := fileInfo.Size()

	// Validate seeking beyond EOF
	if payload.Whence == io.SeekStart && payload.Offset > fileSize {
		return arpc.Response{}, fmt.Errorf("seeking beyond EOF is not allowed")
	}

	// Perform the seek operation for other cases
	newOffset, err := fh.file.Seek(payload.Offset, payload.Whence)
	if err != nil {
		return arpc.Response{}, err
	}

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

	if handle.file != nil {
		handle.file.Close()
	}

	s.handles.Del(uint64(payload.HandleID))

	closed := arpc.StringMsg("closed")
	data, err := closed.Encode()
	if err != nil {
		return arpc.Response{}, err
	}

	return arpc.Response{Status: 200, Data: data}, nil
}
