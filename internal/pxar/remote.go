//go:build linux

package pxar

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/safemap"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

var readBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 4<<20)
		return &b
	},
}

type contentHandle struct {
	rc       io.ReadCloser
	fileSize uint64
	mu       sync.Mutex
}

type readContentReq struct {
	ContentStart uint64 `cbor:"content_start"`
	ContentEnd   uint64 `cbor:"content_end"`
	FileSize     uint64 `cbor:"file_size"`
	Length       int    `cbor:"length"`
}

type readContentAtReq struct {
	HandleID uint64 `cbor:"handle_id"`
	Offset   int64  `cbor:"offset"`
	Length   int    `cbor:"length"`
}

type closeContentReq struct {
	HandleID uint64 `cbor:"handle_id"`
}

type handleIDResp struct {
	HandleID uint64 `cbor:"handle_id"`
}

type lookupByPathReq struct {
	Path string `cbor:"path"`
}

type readDirReq struct {
	EntryEnd uint64 `cbor:"entry_end"`
}

type getAttrReq struct {
	EntryStart uint64 `cbor:"entry_start"`
	EntryEnd   uint64 `cbor:"entry_end"`
}

type readLinkReq struct {
	EntryStart uint64 `cbor:"entry_start"`
	EntryEnd   uint64 `cbor:"entry_end"`
}

type listXAttrsReq struct {
	EntryStart uint64 `cbor:"entry_start"`
	EntryEnd   uint64 `cbor:"entry_end"`
}

type errorReq struct {
	Error string `cbor:"error"`
}

type RemoteServer struct {
	reader         *PxarReader
	router         *arpc.Router
	isDone         atomic.Bool
	DoneCh         chan struct{}
	errCh          chan error
	closed         atomic.Bool
	handleCounter  uint64
	contentHandles *safemap.Map[uint64, *contentHandle]
}

func NewRemoteServer(reader *PxarReader) (*RemoteServer, chan error) {
	router := arpc.NewRouter()
	errChan := make(chan error, 16)
	s := &RemoteServer{
		reader:         reader,
		router:         &router,
		DoneCh:         make(chan struct{}, 1),
		errCh:          errChan,
		contentHandles: safemap.New[uint64, *contentHandle](),
	}
	s.registerHandlers()
	return s, errChan
}

func (s *RemoteServer) Close() error {
	if s.closed.Swap(true) {
		return nil
	}

	s.contentHandles.ForEach(func(id uint64, h *contentHandle) bool {
		h.rc.Close()
		return true
	})
	s.contentHandles.Clear()

	return s.reader.Close()
}

func (s *RemoteServer) Router() *arpc.Router {
	return s.router
}

func (s *RemoteServer) registerHandlers() {
	s.router.Handle("pxar.GetRoot", s.handleGetRoot)
	s.router.Handle("pxar.LookupByPath", s.handleLookupByPath)
	s.router.Handle("pxar.ReadDir", s.handleReadDir)
	s.router.Handle("pxar.GetAttr", s.handleGetAttr)
	s.router.Handle("pxar.ReadContent", s.handleReadContent)
	s.router.Handle("pxar.ReadContentAt", s.handleReadContentAt)
	s.router.Handle("pxar.CloseContent", s.handleCloseContent)
	s.router.Handle("pxar.ReadLink", s.handleReadLink)
	s.router.Handle("pxar.ListXAttrs", s.handleListXAttrs)
	s.router.Handle("pxar.Done", s.handleDone)
	s.router.Handle("pxar.Error", s.handleError)
}

func (s *RemoteServer) handleError(req *arpc.Request) (arpc.Response, error) {
	var params errorReq
	if err := cbor.Unmarshal(req.Payload, &params); err != nil {
		return arpc.Response{}, err
	}

	err := fmt.Errorf("client error: %s", params.Error)
	syslog.L.Error(err).Write()
	select {
	case s.errCh <- err:
	default:
	}

	return arpc.Response{Status: 200}, nil
}

func (s *RemoteServer) handleDone(req *arpc.Request) (arpc.Response, error) {
	if !s.isDone.Swap(true) {
		close(s.DoneCh)
	}
	return arpc.Response{Status: 200}, nil
}

func (s *RemoteServer) handleGetRoot(req *arpc.Request) (arpc.Response, error) {
	info, err := s.reader.GetRoot(req.Context)
	if err != nil {
		return makeErrorResponse(err)
	}

	data, err := cbor.Marshal(info)
	if err != nil {
		return arpc.Response{}, err
	}
	return arpc.Response{Status: 200, Data: data}, nil
}

func (s *RemoteServer) handleLookupByPath(req *arpc.Request) (arpc.Response, error) {
	var params lookupByPathReq
	if err := cbor.Unmarshal(req.Payload, &params); err != nil {
		return arpc.Response{}, err
	}

	info, err := s.reader.LookupByPath(req.Context, params.Path)
	if err != nil {
		return makeErrorResponse(err)
	}

	data, err := cbor.Marshal(info)
	if err != nil {
		return arpc.Response{}, err
	}
	return arpc.Response{Status: 200, Data: data}, nil
}

func (s *RemoteServer) handleReadDir(req *arpc.Request) (arpc.Response, error) {
	var params readDirReq
	if err := cbor.Unmarshal(req.Payload, &params); err != nil {
		return arpc.Response{}, err
	}

	entries, err := s.reader.ReadDir(req.Context, params.EntryEnd)
	if err != nil {
		return makeErrorResponse(err)
	}

	data, err := cbor.Marshal(entries)
	if err != nil {
		return arpc.Response{}, err
	}
	return arpc.Response{Status: 200, Data: data}, nil
}

func (s *RemoteServer) handleGetAttr(req *arpc.Request) (arpc.Response, error) {
	var params getAttrReq
	if err := cbor.Unmarshal(req.Payload, &params); err != nil {
		return arpc.Response{}, err
	}

	info, err := s.reader.GetAttr(req.Context, params.EntryStart, params.EntryEnd)
	if err != nil {
		return makeErrorResponse(err)
	}

	data, err := cbor.Marshal(info)
	if err != nil {
		return arpc.Response{}, err
	}
	return arpc.Response{Status: 200, Data: data}, nil
}

func (s *RemoteServer) handleReadContent(req *arpc.Request) (arpc.Response, error) {
	var params readContentReq
	if err := cbor.Unmarshal(req.Payload, &params); err != nil {
		return arpc.Response{}, err
	}

	rc, err := s.reader.ReadFileContentReader(req.Context, params.ContentStart, params.ContentEnd)
	if err != nil {
		return makeErrorResponse(err)
	}

	handleID := atomic.AddUint64(&s.handleCounter, 1)

	reqLen := params.Length
	if reqLen <= 0 {
		reqLen = 4 << 20
	}
	if int64(reqLen) > int64(params.FileSize) {
		reqLen = int(params.FileSize)
	}

	bptr := readBufPool.Get().(*[]byte)
	buf := *bptr
	tempBuf := len(buf) < reqLen
	if tempBuf {
		buf = make([]byte, reqLen)
	}

	n, readErr := io.ReadFull(rc, buf[:reqLen])
	if readErr != nil && readErr != io.ErrUnexpectedEOF && readErr != io.EOF {
		if !tempBuf {
			readBufPool.Put(bptr)
		}
		rc.Close()
		return arpc.Response{}, fmt.Errorf("read content: %w", readErr)
	}

	if uint64(n) >= params.FileSize {
		rc.Close()
	} else {
		s.contentHandles.Set(handleID, &contentHandle{rc: rc, fileSize: params.FileSize})
	}

	respData, _ := cbor.Marshal(handleIDResp{HandleID: handleID})
	return arpc.Response{Status: 213, Data: respData, RawStream: func(stream arpc.ARPCStream) {
		if !tempBuf {
			defer readBufPool.Put(bptr)
		}
		_ = arpc.SendDataFromReader(bytes.NewReader(buf[:n]), n, stream)
	}}, nil
}

func (s *RemoteServer) handleReadContentAt(req *arpc.Request) (arpc.Response, error) {
	var params readContentAtReq
	if err := cbor.Unmarshal(req.Payload, &params); err != nil {
		return arpc.Response{}, err
	}

	if params.Length < 0 {
		return arpc.Response{}, fmt.Errorf("invalid length: %d", params.Length)
	}

	h, ok := s.contentHandles.Get(params.HandleID)
	if !ok {
		return arpc.Response{}, fmt.Errorf("handle %d not found", params.HandleID)
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if params.Offset >= int64(h.fileSize) {
		return arpc.Response{Status: 213, RawStream: func(stream arpc.ARPCStream) {
			_ = arpc.SendDataFromReader(bytes.NewReader(nil), 0, stream)
		}}, nil
	}

	reqLen := params.Length
	if params.Offset+int64(reqLen) > int64(h.fileSize) {
		reqLen = int(int64(h.fileSize) - params.Offset)
	}

	if _, err := h.rc.(io.Seeker).Seek(params.Offset, io.SeekStart); err != nil {
		return arpc.Response{}, fmt.Errorf("seek to %d: %w", params.Offset, err)
	}

	bptr := readBufPool.Get().(*[]byte)
	buf := *bptr
	tempBuf := len(buf) < reqLen
	if tempBuf {
		buf = make([]byte, reqLen)
	}

	n, err := io.ReadFull(h.rc, buf[:reqLen])
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		if !tempBuf {
			readBufPool.Put(bptr)
		}
		return arpc.Response{}, fmt.Errorf("read content at %d: %w", params.Offset, err)
	}

	return arpc.Response{Status: 213, RawStream: func(stream arpc.ARPCStream) {
		if !tempBuf {
			defer readBufPool.Put(bptr)
		}
		_ = arpc.SendDataFromReader(bytes.NewReader(buf[:n]), n, stream)
	}}, nil
}

func (s *RemoteServer) handleCloseContent(req *arpc.Request) (arpc.Response, error) {
	var params closeContentReq
	if err := cbor.Unmarshal(req.Payload, &params); err != nil {
		return arpc.Response{}, err
	}

	h, ok := s.contentHandles.Get(params.HandleID)
	if !ok {
		return arpc.Response{}, fmt.Errorf("handle %d not found", params.HandleID)
	}

	h.rc.Close()
	s.contentHandles.Del(params.HandleID)
	return arpc.Response{Status: 200}, nil
}

func (s *RemoteServer) handleReadLink(req *arpc.Request) (arpc.Response, error) {
	var params readLinkReq
	if err := cbor.Unmarshal(req.Payload, &params); err != nil {
		return arpc.Response{}, err
	}

	target, err := s.reader.ReadLink(req.Context, params.EntryStart, params.EntryEnd)
	if err != nil {
		return makeErrorResponse(err)
	}
	return arpc.Response{Status: 200, Data: target}, nil
}

func (s *RemoteServer) handleListXAttrs(req *arpc.Request) (arpc.Response, error) {
	var params listXAttrsReq
	if err := cbor.Unmarshal(req.Payload, &params); err != nil {
		return arpc.Response{}, err
	}

	xattrs, err := s.reader.ListXAttrs(req.Context, params.EntryStart, params.EntryEnd)
	if err != nil {
		return makeErrorResponse(err)
	}

	data, err := cbor.Marshal(xattrs)
	if err != nil {
		return arpc.Response{}, err
	}
	return arpc.Response{Status: 200, Data: data}, nil
}

func makeErrorResponse(err error) (arpc.Response, error) {
	if errno, ok := err.(syscall.Errno); ok {
		errData, _ := json.Marshal(map[string]any{"errno": int64(errno)})
		return arpc.Response{Status: 500, Message: err.Error(), Data: errData}, nil
	}
	return arpc.Response{}, err
}
