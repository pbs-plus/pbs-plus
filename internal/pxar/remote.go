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
		b := make([]byte, 4<<20) // 4 MB
		return &b
	},
}

type contentHandle struct {
	rc       io.ReadCloser
	fileSize uint64
	mu       sync.Mutex
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
	s.router.Handle("pxar.OpenContent", s.handleOpenContent)
	s.router.Handle("pxar.ReadContentAt", s.handleReadContentAt)
	s.router.Handle("pxar.CloseContent", s.handleCloseContent)
	s.router.Handle("pxar.ReadLink", s.handleReadLink)
	s.router.Handle("pxar.ListXAttrs", s.handleListXAttrs)
	s.router.Handle("pxar.Done", s.handleDone)
	s.router.Handle("pxar.Error", s.handleError)
}

func (s *RemoteServer) handleError(req *arpc.Request) (arpc.Response, error) {
	var params struct {
		Error string `cbor:"error"`
	}
	if err := cbor.Unmarshal(req.Payload, &params); err != nil {
		return arpc.Response{}, err
	}

	err := fmt.Errorf("client error: %s", params.Error)
	syslog.L.Error(err).Write()

	s.errCh <- err

	return arpc.Response{
		Status: 200,
		Data:   nil,
	}, nil
}

func (s *RemoteServer) handleDone(req *arpc.Request) (arpc.Response, error) {
	if !s.isDone.Swap(true) {
		close(s.DoneCh)
	}

	return arpc.Response{
		Status: 200,
		Data:   nil,
	}, nil
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

	return arpc.Response{
		Status: 200,
		Data:   data,
	}, nil
}

func (s *RemoteServer) handleLookupByPath(req *arpc.Request) (arpc.Response, error) {
	var params struct {
		Path string `cbor:"path"`
	}
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

	return arpc.Response{
		Status: 200,
		Data:   data,
	}, nil
}

func (s *RemoteServer) handleReadDir(req *arpc.Request) (arpc.Response, error) {
	var params struct {
		EntryEnd uint64 `cbor:"entry_end"`
	}
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

	return arpc.Response{
		Status: 200,
		Data:   data,
	}, nil
}

func (s *RemoteServer) handleGetAttr(req *arpc.Request) (arpc.Response, error) {
	var params struct {
		EntryStart uint64 `cbor:"entry_start"`
		EntryEnd   uint64 `cbor:"entry_end"`
	}
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

	return arpc.Response{
		Status: 200,
		Data:   data,
	}, nil
}

func (s *RemoteServer) handleOpenContent(req *arpc.Request) (arpc.Response, error) {
	var params struct {
		ContentStart uint64 `cbor:"content_start"`
		ContentEnd   uint64 `cbor:"content_end"`
	}
	if err := cbor.Unmarshal(req.Payload, &params); err != nil {
		return arpc.Response{}, err
	}

	entry, err := s.reader.GetAttr(req.Context, params.ContentStart, params.ContentEnd)
	if err != nil {
		return makeErrorResponse(err)
	}

	rc, err := s.reader.ReadFileContentReader(req.Context, params.ContentStart, params.ContentEnd)
	if err != nil {
		return makeErrorResponse(err)
	}

	handleID := atomic.AddUint64(&s.handleCounter, 1)
	s.contentHandles.Set(handleID, &contentHandle{
		rc:       rc,
		fileSize: uint64(entry.RawSize),
	})

	respData, _ := cbor.Marshal(map[string]uint64{
		"handle_id": handleID,
		"file_size": uint64(entry.RawSize),
	})
	return arpc.Response{Status: 200, Data: respData}, nil
}

func (s *RemoteServer) handleReadContentAt(req *arpc.Request) (arpc.Response, error) {
	var params struct {
		HandleID uint64 `cbor:"handle_id"`
		Offset   int64  `cbor:"offset"`
		Length   int    `cbor:"length"`
	}
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
	workBuf := *bptr
	isTemp := false
	if len(workBuf) < reqLen {
		workBuf = make([]byte, reqLen)
		isTemp = true
	}

	n, err := io.ReadFull(h.rc, workBuf[:reqLen])
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		if !isTemp {
			readBufPool.Put(bptr)
		}
		return arpc.Response{}, fmt.Errorf("read content at %d: %w", params.Offset, err)
	}

	return arpc.Response{Status: 213, RawStream: func(stream arpc.ARPCStream) {
		if !isTemp {
			defer readBufPool.Put(bptr)
		}
		_ = arpc.SendDataFromReader(bytes.NewReader(workBuf[:n]), n, stream)
	}}, nil
}

func (s *RemoteServer) handleCloseContent(req *arpc.Request) (arpc.Response, error) {
	var params struct {
		HandleID uint64 `cbor:"handle_id"`
	}
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
	var params struct {
		EntryStart uint64 `cbor:"entry_start"`
		EntryEnd   uint64 `cbor:"entry_end"`
	}
	if err := cbor.Unmarshal(req.Payload, &params); err != nil {
		return arpc.Response{}, err
	}

	target, err := s.reader.ReadLink(req.Context, params.EntryStart, params.EntryEnd)
	if err != nil {
		return makeErrorResponse(err)
	}

	return arpc.Response{
		Status: 200,
		Data:   target,
	}, nil
}

func (s *RemoteServer) handleListXAttrs(req *arpc.Request) (arpc.Response, error) {
	var params struct {
		EntryStart uint64 `cbor:"entry_start"`
		EntryEnd   uint64 `cbor:"entry_end"`
	}
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

	return arpc.Response{
		Status: 200,
		Data:   data,
	}, nil
}

func makeErrorResponse(err error) (arpc.Response, error) {
	if errno, ok := err.(syscall.Errno); ok {
		errData, _ := json.Marshal(map[string]any{
			"errno": int64(errno),
		})
		return arpc.Response{
			Status:  500,
			Message: err.Error(),
			Data:    errData,
		}, nil
	}
	return arpc.Response{}, err
}
