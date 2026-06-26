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
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/safemap"
)

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
	errChan := make(chan error, 256)
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
		if err := h.rc.Close(); err != nil {
			log.Error(err, "")
		}
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

// handleError forwards an agent-side error to the job's task log. It blocks
// never silently hidden from the operator.
func (s *RemoteServer) handleError(req *arpc.Request) (arpc.Response, error) {
	var params errorReq
	if err := cbor.Unmarshal(req.Payload, &params); err != nil {
		return arpc.Response{}, err
	}

	err := fmt.Errorf("client error: %s", params.Error)
	log.Error(err, "")
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
	s.contentHandles.Set(handleID, &contentHandle{rc: rc, fileSize: params.FileSize})

	reqLen := params.Length
	if reqLen <= 0 {
		reqLen = 4 << 20
	}
	if int64(reqLen) > int64(params.FileSize) {
		reqLen = int(params.FileSize)
	}

	respData, err := cbor.Marshal(handleIDResp{HandleID: handleID})
	if err != nil {
		return arpc.Response{}, err
	}

	lr := io.LimitReader(rc, int64(reqLen))
	return arpc.Response{Status: 213, Data: respData, RawStream: func(stream arpc.ARPCStream) {
		if err := arpc.SendDataFromReader(lr, reqLen, stream); err != nil {
			log.Error(err, "")
		}
		if uint64(reqLen) >= params.FileSize {
			if err := rc.Close(); err != nil {
				log.Error(err, "")
			}
		}
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
			if err := arpc.SendDataFromReader(bytes.NewReader(nil), 0, stream); err != nil {
				log.Error(err, "")
			}
		}}, nil
	}

	reqLen := params.Length
	if params.Offset+int64(reqLen) > int64(h.fileSize) {
		reqLen = int(int64(h.fileSize) - params.Offset)
	}

	if _, err := h.rc.(io.Seeker).Seek(params.Offset, io.SeekStart); err != nil {
		return arpc.Response{}, fmt.Errorf("seek to %d: %w", params.Offset, err)
	}

	lr := io.LimitReader(h.rc, int64(reqLen))
	return arpc.Response{Status: 213, RawStream: func(stream arpc.ARPCStream) {
		if err := arpc.SendDataFromReader(lr, reqLen, stream); err != nil {
			log.Error(err, "")
		}
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

	if err := h.rc.Close(); err != nil {
		log.Error(err, "")
	}
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
		errData, err := json.Marshal(map[string]any{"errno": int64(errno)})
		if err != nil {
			return arpc.Response{}, err
		}
		return arpc.Response{Status: 500, Message: err.Error(), Data: errData}, nil
	}
	return arpc.Response{}, err
}
