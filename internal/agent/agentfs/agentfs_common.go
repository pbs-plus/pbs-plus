package agentfs

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	binarystream "github.com/pbs-plus/pbs-plus/internal/arpc/binary"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/xtaci/smux"
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

	path := s.abs(payload.Path)
	fh, err := s.platformOpen(path)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleOpenFile: open failed").WithField("path", path).Write()
		return arpc.Response{}, err
	}

	handleId := s.handleIdGen.NextID()
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

	fullPath := s.abs(payload.Path)
	info, err := s.platformStat(fullPath)
	if err != nil {
		if !s.isExpectedMissingFile(fullPath) {
			syslog.L.Error(err).WithMessage("handleAttr: stat failed").WithField("path", fullPath).Write()
		}
		return arpc.Response{}, err
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

	fullPath := s.abs(payload.Path)
	info, err := s.platformXstat(fullPath, payload.AclOnly)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleXattr: xstat failed").WithField("path", fullPath).Write()
		return arpc.Response{}, err
	}

	data, err := cbor.Marshal(info)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleXattr: encode failed").Write()
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

	if !fh.acquireOp() {
		syslog.L.Debug().WithMessage("handleReadDir: handle is closing").WithField("handle_id", payload.HandleID).Write()
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

	syslog.L.Debug().WithMessage("handleReadDir: sending batch").WithField("handle_id", payload.HandleID).WithField("bytes", len(encodedBatch)).Write()

	return arpc.Response{
		Status: 213,
		RawStream: func(stream *smux.Stream) {
			defer fh.releaseOp()
			_ = binarystream.SendDataFromReader(bytes.NewReader(encodedBatch), len(encodedBatch), stream)
		},
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
		return arpc.Response{}, fmt.Errorf("invalid length: %d", payload.Length)
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

	fh.mu.Lock()
	fileSize := fh.fileSize
	f := fh.file
	fh.mu.Unlock()

	if payload.Offset >= fileSize {
		fh.releaseOp()
		return arpc.Response{Status: 213, RawStream: func(stream *smux.Stream) {
			_ = binarystream.SendDataFromReader(bytes.NewReader(nil), 0, stream)
		}}, nil
	}

	reqLen := payload.Length
	if payload.Offset+int64(reqLen) > fileSize {
		reqLen = int(fileSize - payload.Offset)
	}

	if s.readMode == "mmap" && reqLen > 0 {
		if data, cleanup, ok := s.platformMmap(fh, payload.Offset, reqLen); ok {
			return arpc.Response{Status: 213, RawStream: func(stream *smux.Stream) {
				defer fh.releaseOp()
				defer cleanup()
				_ = binarystream.SendDataFromReader(bytes.NewReader(data), len(data), stream)
			}}, nil
		}
	}

	bptr := readBufPool.Get().(*[]byte)
	workBuf := *bptr
	isTemp := false
	if len(workBuf) < reqLen {
		workBuf = make([]byte, reqLen)
		isTemp = true
	}

	n, err := s.platformPread(f, workBuf[:reqLen], payload.Offset)
	if err != nil && err != io.EOF {
		if !isTemp {
			readBufPool.Put(bptr)
		}
		fh.releaseOp()
		syslog.L.Error(err).WithMessage("handleReadAt: read failed").Write()
		return arpc.Response{}, err
	}

	return arpc.Response{Status: 213, RawStream: func(stream *smux.Stream) {
		defer fh.releaseOp()
		if !isTemp {
			defer readBufPool.Put(bptr)
		}
		_ = binarystream.SendDataFromReader(bytes.NewReader(workBuf[:n]), n, stream)
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
		return arpc.Response{}, os.ErrClosed
	}
	defer fh.releaseOp()

	fh.mu.Lock()
	defer fh.mu.Unlock()

	newOffset, err := s.platformLseek(fh, payload.Offset, payload.Whence)
	if err != nil {
		syslog.L.Error(err).WithMessage("handleLseek: seek failed").Write()
		return arpc.Response{}, err
	}

	fh.logicalOffset = newOffset
	respBytes, _ := cbor.Marshal(types.LseekResp{NewOffset: newOffset})
	return arpc.Response{Status: 200, Data: respBytes}, nil
}

func (s *AgentFSServer) handleClose(req *arpc.Request) (arpc.Response, error) {
	syslog.L.Debug().WithMessage("handleClose: decoding request").Write()
	var payload types.CloseReq
	if err := cbor.Unmarshal(req.Payload, &payload); err != nil {
		syslog.L.Error(err).WithMessage("handleClose: decode failed").Write()
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
		syslog.L.Warn().WithMessage("handleClose: timeout, forcing").WithField("handle_id", payload.HandleID).Write()
	}

	fh.mu.Lock()
	s.platformCloseResources(fh)
	if fh.dirReader != nil {
		fh.dirReader.Close()
	}
	if fh.file != nil {
		fh.file.Close()
	}
	fh.mu.Unlock()

	s.handles.Del(uint64(payload.HandleID))
	data, _ := cbor.Marshal("closed")
	return arpc.Response{Status: 200, Data: data}, nil
}

func (s *AgentFSServer) isExpectedMissingFile(p string) bool {
	return (len(p) >= 12 && p[len(p)-12:] == ".pxarexclude")
}
