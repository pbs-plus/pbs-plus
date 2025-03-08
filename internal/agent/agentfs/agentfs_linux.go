//go:build linux

package agentfs

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	securejoin "github.com/cyphar/filepath-securejoin"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/agentfs/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	binarystream "github.com/sonroyaalmerol/pbs-plus/internal/arpc/binary"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils/idgen"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils/safemap"
	"github.com/xtaci/smux"
	"golang.org/x/sys/unix"
)

type FileHandle struct {
	file     *os.File
	fileSize int64
	isDir    bool
}

type AgentFSServer struct {
	ctx         context.Context
	ctxCancel   context.CancelFunc
	jobId       string
	snapshot    snapshots.Snapshot
	handleIdGen *idgen.IDGenerator
	handles     *safemap.Map[uint64, *FileHandle]
	arpcRouter  *arpc.Router
	statFs      types.StatFS
}

func NewAgentFSServer(jobId string, snapshot snapshots.Snapshot) *AgentFSServer {
	ctx, cancel := context.WithCancel(context.Background())

	s := &AgentFSServer{
		snapshot:    snapshot,
		jobId:       jobId,
		handles:     safemap.New[uint64, *FileHandle](),
		ctx:         ctx,
		ctxCancel:   cancel,
		handleIdGen: idgen.NewIDGenerator(),
	}

	if err := s.initializeStatFS(); err != nil && syslog.L != nil {
		syslog.L.Error(err).WithMessage("failed to initialize statfs").Write()
	}

	return s
}

func safeHandler(fn func(req arpc.Request) (arpc.Response, error)) func(req arpc.Request) (arpc.Response, error) {
	return func(req arpc.Request) (res arpc.Response, err error) {
		defer func() {
			if r := recover(); r != nil {
				syslog.L.Error(fmt.Errorf("panic in handler: %v", r)).
					WithField("payload", req.Payload).
					Write()
				err = os.ErrInvalid
			}
		}()
		return fn(req)
	}
}

func (s *AgentFSServer) RegisterHandlers(r *arpc.Router) {
	r.Handle(s.jobId+"/OpenFile", safeHandler(s.handleOpenFile))
	r.Handle(s.jobId+"/Stat", safeHandler(s.handleStat))
	r.Handle(s.jobId+"/ReadDir", safeHandler(s.handleReadDir))
	r.Handle(s.jobId+"/ReadAt", safeHandler(s.handleReadAt))
	r.Handle(s.jobId+"/Lseek", safeHandler(s.handleLseek))
	r.Handle(s.jobId+"/Close", safeHandler(s.handleClose))
	r.Handle(s.jobId+"/StatFS", safeHandler(s.handleStatFS))

	s.arpcRouter = r
}

func (s *AgentFSServer) Close() {
	if s.arpcRouter != nil {
		r := s.arpcRouter
		r.CloseHandle(s.jobId + "/OpenFile")
		r.CloseHandle(s.jobId + "/Stat")
		r.CloseHandle(s.jobId + "/ReadDir")
		r.CloseHandle(s.jobId + "/ReadAt")
		r.CloseHandle(s.jobId + "/Lseek")
		r.CloseHandle(s.jobId + "/Close")
		r.CloseHandle(s.jobId + "/StatFS")
	}

	s.handles.ForEach(func(u uint64, fh *FileHandle) bool {
		fh.file.Close()
		return true
	})

	s.handles.Clear()

	s.ctxCancel()
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
		return types.StatFS{}, fmt.Errorf("failed to get filesystem stats for path %s: %w", path, err)
	}

	// Map the unix.Statfs_t fields to the types.StatFS structure
	stat := types.StatFS{
		Bsize:   uint64(statfs.Bsize),   // Block size
		Blocks:  statfs.Blocks,          // Total number of blocks
		Bfree:   statfs.Bfree,           // Free blocks
		Bavail:  statfs.Bavail,          // Available blocks to unprivileged users
		Files:   statfs.Files,           // Total number of inodes
		Ffree:   statfs.Ffree,           // Free inodes
		NameLen: uint64(statfs.Namelen), // Maximum filename length
	}

	return stat, nil
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

	file, err := os.Open(path)
	if err != nil {
		return arpc.Response{}, err
	}

	handleId := s.handleIdGen.NextID()
	fh := &FileHandle{
		file:     file,
		fileSize: stat.Size(),
		isDir:    stat.IsDir(),
	}
	s.handles.Set(handleId, fh)

	// Return the handle ID to the client.
	fhId := types.FileHandleId(handleId)
	dataBytes, err := fhId.Encode()
	if err != nil {
		file.Close()
		return arpc.Response{}, err
	}

	return arpc.Response{
		Status: 200,
		Data:   dataBytes,
	}, nil
}

func (s *AgentFSServer) handleStat(req arpc.Request) (arpc.Response, error) {
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

func (s *AgentFSServer) handleReadDir(req arpc.Request) (arpc.Response, error) {
	var payload types.ReadDirReq
	if err := payload.Decode(req.Payload); err != nil {
		return arpc.Response{}, err
	}

	fullDirPath, err := s.abs(payload.Path)
	if err != nil {
		return arpc.Response{}, err
	}

	entries, err := readDirBulk(fullDirPath)
	if err != nil {
		return arpc.Response{}, err
	}

	return arpc.Response{
		Status: 200,
		Data:   entries,
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

	reader := io.NewSectionReader(fh.file, payload.Offset, int64(payload.Length))

	streamCallback := func(stream *smux.Stream) {
		err := binarystream.SendDataFromReader(reader, payload.Length, stream)
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

	handle.file.Close()
	s.handles.Del(uint64(payload.HandleID))

	closed := arpc.StringMsg("closed")
	data, err := closed.Encode()
	if err != nil {
		return arpc.Response{}, err
	}

	return arpc.Response{Status: 200, Data: data}, nil
}

func (s *AgentFSServer) abs(filename string) (string, error) {
	if filename == "" || filename == "." {
		return s.snapshot.Path, nil
	}
	path, err := securejoin.SecureJoin(s.snapshot.Path, filename)
	if err != nil {
		return "", err
	}
	return path, nil
}
