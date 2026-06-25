package agentfs

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/log"
)

type FileHandle struct {
	file          *os.File
	fileSize      int64
	isDir         bool
	dirReader     *DirReader
	mapping       uintptr
	logicalOffset int64

	mu        sync.Mutex
	activeOps int32
	closing   bool
	closeDone chan struct{}
}

func NewFileHandle(handle *os.File) *FileHandle {
	return &FileHandle{
		file: handle,
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
		// Close channel outside the lock
		go func() { close(fh.closeDone) }()
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

func (s *AgentFSServer) handleStatFS(req *arpc.Request) (arpc.Response, error) {
	log.Debug("handleStatFS: encoding statFs")
	enc, err := cbor.Marshal(s.statFs)
	if err != nil {
		log.Error(err, "handleStatFS: encode failed")
		return arpc.Response{}, err
	}
	log.Debug("handleStatFS: success")
	return arpc.Response{Status: 200, Data: enc}, nil
}

func (s *AgentFSServer) handleOpenFile(req *arpc.Request) (arpc.Response, error) {
	log.Debug("handleOpenFile: decoding request")
	var payload types.OpenFileReq
	if err := cbor.Unmarshal(req.Payload, &payload); err != nil {
		log.Error(err, "handleOpenFile: decode failed")
		return arpc.Response{}, err
	}

	if payload.Flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC) != 0 {
		log.Warn("handleOpenFile: write operation blocked", "path", payload.Path)
		errBytes, err := cbor.Marshal("write operations not allowed")
		if err != nil {
			log.Error(err, "")
		}
		return arpc.Response{Status: 403, Data: errBytes}, nil
	}

	path := s.abs(payload.Path)
	fh, err := s.platformOpen(path)
	if err != nil {
		return arpc.Response{}, wrapPathError("open", payload.Path, err)
	}

	handleId := s.handleIdGen.NextID()
	s.handles.Set(handleId, fh)
	dataBytes, err := cbor.Marshal(types.FileHandleID(handleId))
	if err != nil {
		log.Error(err, "")
	}
	log.Debug("handleOpenFile: success",

		"is_dir", fh.isDir, "handle_id", handleId)

	return arpc.Response{Status: 200, Data: dataBytes}, nil
}

func (s *AgentFSServer) handleAttr(req *arpc.Request) (arpc.Response, error) {
	log.Debug("handleAttr: decoding request")
	var payload types.StatReq
	if err := cbor.Unmarshal(req.Payload, &payload); err != nil {
		log.Error(err, "handleAttr: decode failed")
		return arpc.Response{}, err
	}

	fullPath := s.abs(payload.Path)
	info, err := s.platformStat(fullPath)
	if err != nil {
		return arpc.Response{}, wrapPathError("stat", payload.Path, err)
	}

	data, err := cbor.Marshal(info)
	if err != nil {
		log.Error(err, "handleAttr: encode failed")
		return arpc.Response{}, err
	}
	log.Debug("handleAttr: success", "path", fullPath)
	return arpc.Response{Status: 200, Data: data}, nil
}

func (s *AgentFSServer) handleXattr(req *arpc.Request) (arpc.Response, error) {
	log.Debug("handleXattr: decoding request")
	var payload types.StatReq
	if err := cbor.Unmarshal(req.Payload, &payload); err != nil {
		log.Error(err, "handleXattr: decode failed")
		return arpc.Response{}, err
	}

	fullPath := s.abs(payload.Path)
	info, err := s.platformXstat(fullPath, payload.AclOnly)
	if err != nil {
		return arpc.Response{}, wrapPathError("xstat", payload.Path, err)
	}

	data, err := cbor.Marshal(info)
	if err != nil {
		log.Error(err, "handleXattr: encode failed")
		return arpc.Response{}, err
	}
	log.Debug("handleXattr: success", "path", fullPath)
	return arpc.Response{Status: 200, Data: data}, nil
}

func (s *AgentFSServer) handleReadDir(req *arpc.Request) (arpc.Response, error) {
	log.Debug("handleReadDir: decoding request")
	var payload types.ReadDirReq
	if err := cbor.Unmarshal(req.Payload, &payload); err != nil {
		log.Error(err, "handleReadDir: decode failed")
		return arpc.Response{}, err
	}

	fh, exists := s.handles.Get(uint64(payload.HandleID))
	if !exists {
		log.Warn("handleReadDir: handle not found", "handle_id", payload.HandleID)
		return arpc.Response{}, os.ErrNotExist
	}

	if !fh.acquireOp() {
		log.Debug("handleReadDir: handle is closing", "handle_id", payload.HandleID)
		return arpc.Response{}, os.ErrClosed
	}

	fh.mu.Lock()
	reader := fh.dirReader
	fh.mu.Unlock()

	encodedBatch, err := reader.NextBatch(req.Context, s.statFs.Bsize)
	if err != nil {
		fh.releaseOp()
		return arpc.Response{}, err
	}
	log.Debug("handleReadDir: sending batch", "bytes", len(encodedBatch), "handle_id", payload.HandleID)

	return arpc.Response{
		Status: 213,
		RawStream: func(stream arpc.ARPCStream) {
			defer fh.releaseOp()
			if err := arpc.SendDataFromReader(bytes.NewReader(encodedBatch), len(encodedBatch), stream); err != nil {
				log.Error(err, "")
			}
		},
	}, nil
}

func (s *AgentFSServer) handleReadAt(req *arpc.Request) (arpc.Response, error) {
	log.Debug("handleReadAt: decoding request")
	var payload types.ReadAtReq
	if err := cbor.Unmarshal(req.Payload, &payload); err != nil {
		log.Error(err, "handleReadAt: decode failed")
		return arpc.Response{}, err
	}

	if payload.Length < 0 {
		return arpc.Response{}, fmt.Errorf("invalid length: %d", payload.Length)
	}

	fh, exists := s.handles.Get(uint64(payload.HandleID))
	if !exists {
		log.Warn("handleReadAt: handle not found", "handle_id", payload.HandleID)
		return arpc.Response{}, os.ErrNotExist
	}

	if !fh.acquireOp() {
		log.Debug("handleReadAt: handle is closing", "handle_id", payload.HandleID)
		return arpc.Response{}, os.ErrClosed
	}

	fh.mu.Lock()
	fileSize := fh.fileSize
	f := fh.file
	fh.mu.Unlock()

	if payload.Offset >= fileSize {
		fh.releaseOp()
		return arpc.Response{Status: 213, RawStream: func(stream arpc.ARPCStream) {
			if err := arpc.SendDataFromReader(bytes.NewReader(nil), 0, stream); err != nil {
				log.Error(err, "")
			}
		}}, nil
	}

	reqLen := payload.Length
	if payload.Offset+int64(reqLen) > fileSize {
		reqLen = int(fileSize - payload.Offset)
	}

	if s.readMode == "mmap" && reqLen > 0 {
		if data, cleanup, ok := s.platformMmap(fh, payload.Offset, reqLen); ok {
			return arpc.Response{Status: 213, RawStream: func(stream arpc.ARPCStream) {
				defer fh.releaseOp()
				defer cleanup()
				if err := arpc.SendDataFromReader(bytes.NewReader(data), len(data), stream); err != nil {
					log.Error(err, "")
				}
			}}, nil
		}
	}

	return arpc.Response{Status: 213, RawStream: func(stream arpc.ARPCStream) {
		defer fh.releaseOp()
		sr := io.NewSectionReader(f, payload.Offset, int64(reqLen))
		if err := arpc.SendDataFromReader(sr, reqLen, stream); err != nil {
			log.Error(err, "")
		}
	}}, nil
}

func (s *AgentFSServer) handleLseek(req *arpc.Request) (arpc.Response, error) {
	log.Debug("handleLseek: decoding request")
	var payload types.LseekReq
	if err := cbor.Unmarshal(req.Payload, &payload); err != nil {
		log.Error(err, "handleLseek: decode failed")
		return arpc.Response{}, err
	}

	fh, exists := s.handles.Get(uint64(payload.HandleID))
	if !exists {
		log.Warn("handleLseek: handle not found", "handle_id", payload.HandleID)
		return arpc.Response{}, os.ErrNotExist
	}

	if !fh.acquireOp() {
		return arpc.Response{}, os.ErrClosed
	}
	defer fh.releaseOp()

	fh.mu.Lock()
	defer fh.mu.Unlock()

	newOffset, err := s.platformLseek(fh, payload.Offset, payload.Whence)
	if err != nil {
		log.Error(err, "handleLseek: seek failed")
		return arpc.Response{}, err
	}

	fh.logicalOffset = newOffset
	respBytes, err := cbor.Marshal(types.LseekResp{NewOffset: newOffset})
	if err != nil {
		log.Error(err, "")
	}
	return arpc.Response{Status: 200, Data: respBytes}, nil
}

func (s *AgentFSServer) handleClose(req *arpc.Request) (arpc.Response, error) {
	log.Debug("handleClose: decoding request")
	var payload types.CloseReq
	if err := cbor.Unmarshal(req.Payload, &payload); err != nil {
		log.Error(err, "handleClose: decode failed")
		return arpc.Response{}, err
	}

	fh, exists := s.handles.Get(uint64(payload.HandleID))
	if !exists {
		return arpc.Response{}, os.ErrNotExist
	}

	if !fh.beginClose() {
		return arpc.Response{Status: 200}, nil
	}

	if !fh.waitForOps(30 * time.Second) {
		log.Warn("handleClose: timeout, forcing", "handle_id", payload.HandleID)
	}

	fh.mu.Lock()
	s.platformCloseResources(fh)
	if fh.dirReader != nil {
		if err := fh.dirReader.Close(); err != nil {
			log.Error(err, "")
		}
	}
	if fh.file != nil {
		if err := fh.file.Close(); err != nil {
			log.Debug("handleClose: file close error", "error", err)
		}
	}
	fh.mu.Unlock()

	s.handles.Del(uint64(payload.HandleID))
	data, err := cbor.Marshal("closed")
	if err != nil {
		log.Error(err, "")
	}
	return arpc.Response{Status: 200, Data: data}, nil
}

func wrapPathError(op, path string, err error) error {
	if _, ok := err.(*os.PathError); ok {
		return err
	}
	if errno, ok := err.(syscall.Errno); ok {
		return &os.PathError{Op: op, Path: path, Err: errno}
	}
	return &os.PathError{Op: op, Path: path, Err: err}
}
