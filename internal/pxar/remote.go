//go:build linux

package pxar

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"syscall"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	binarystream "github.com/pbs-plus/pbs-plus/internal/arpc/binary"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/xtaci/smux"
)

type RemoteServer struct {
	reader *PxarReader
	router *arpc.Router
	isDone atomic.Bool
	DoneCh chan struct{}
	errCh  chan error
}

func NewRemoteServer(reader *PxarReader) (*RemoteServer, chan error) {
	router := arpc.NewRouter()
	errChan := make(chan error, 16)
	s := &RemoteServer{
		reader: reader,
		router: &router,
		DoneCh: make(chan struct{}, 1),
		errCh:  errChan,
	}
	s.registerHandlers()
	return s, errChan
}

func (s *RemoteServer) Close() error {
	close(s.errCh)

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
	s.router.Handle("pxar.Read", s.handleRead)
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
	info, err := s.reader.GetRoot()
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

	info, err := s.reader.LookupByPath(params.Path)
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

	entries, err := s.reader.ReadDir(params.EntryEnd)
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

	info, err := s.reader.GetAttr(params.EntryStart, params.EntryEnd)
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

func (s *RemoteServer) handleRead(req *arpc.Request) (arpc.Response, error) {
	var params struct {
		ContentStart uint64 `cbor:"content_start"`
		ContentEnd   uint64 `cbor:"content_end"`
		Offset       uint64 `cbor:"offset"`
		Size         uint   `cbor:"size"`
	}
	if err := cbor.Unmarshal(req.Payload, &params); err != nil {
		return arpc.Response{}, err
	}

	data, err := s.reader.Read(params.ContentStart, params.ContentEnd, params.Offset, params.Size)
	if err != nil {
		return makeErrorResponse(err)
	}

	return arpc.Response{Status: 213, RawStream: func(stream *smux.Stream) {
		_ = binarystream.SendDataFromReader(bytes.NewReader(data), len(data), stream)
	}}, nil
}

func (s *RemoteServer) handleReadLink(req *arpc.Request) (arpc.Response, error) {
	var params struct {
		EntryStart uint64 `cbor:"entry_start"`
		EntryEnd   uint64 `cbor:"entry_end"`
	}
	if err := cbor.Unmarshal(req.Payload, &params); err != nil {
		return arpc.Response{}, err
	}

	target, err := s.reader.ReadLink(params.EntryStart, params.EntryEnd)
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

	xattrs, err := s.reader.ListXAttrs(params.EntryStart, params.EntryEnd)
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
