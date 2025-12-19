package pxar

import (
	"context"
	"encoding/json"
	"syscall"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
)

type RemoteServer struct {
	reader *PxarReader
	router *arpc.Router
}

func NewRemoteServer(reader *PxarReader) *RemoteServer {
	router := arpc.NewRouter()
	s := &RemoteServer{
		reader: reader,
		router: &router,
	}
	s.registerHandlers()
	return s
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

	return arpc.Response{
		Status: 200,
		Data:   data,
	}, nil
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

type RemoteClient struct {
	pipe *arpc.StreamPipe
}

func NewRemoteClient(pipe *arpc.StreamPipe) *RemoteClient {
	return &RemoteClient{pipe: pipe}
}

func (c *RemoteClient) GetRoot(ctx context.Context) (EntryInfo, error) {
	var info EntryInfo
	if err := c.pipe.Call(ctx, "pxar.GetRoot", nil, &info); err != nil {
		return EntryInfo{}, err
	}
	return info, nil
}

func (c *RemoteClient) LookupByPath(ctx context.Context, path string) (EntryInfo, error) {
	params := map[string]any{
		"path": path,
	}
	var info EntryInfo
	if err := c.pipe.Call(ctx, "pxar.LookupByPath", params, &info); err != nil {
		return EntryInfo{}, err
	}
	return info, nil
}

func (c *RemoteClient) ReadDir(ctx context.Context, entryEnd uint64) ([]EntryInfo, error) {
	params := map[string]any{
		"entry_end": entryEnd,
	}
	var entries []EntryInfo
	if err := c.pipe.Call(ctx, "pxar.ReadDir", params, &entries); err != nil {
		return nil, err
	}
	return entries, nil
}

func (c *RemoteClient) GetAttr(ctx context.Context, entryStart, entryEnd uint64) (EntryInfo, error) {
	params := map[string]any{
		"entry_start": entryStart,
		"entry_end":   entryEnd,
	}
	var info EntryInfo
	if err := c.pipe.Call(ctx, "pxar.GetAttr", params, &info); err != nil {
		return EntryInfo{}, err
	}
	return info, nil
}

func (c *RemoteClient) Read(ctx context.Context, contentStart, contentEnd, offset uint64, size uint) ([]byte, error) {
	params := map[string]any{
		"content_start": contentStart,
		"content_end":   contentEnd,
		"offset":        offset,
		"size":          size,
	}
	var data []byte
	if err := c.pipe.Call(ctx, "pxar.Read", params, &data); err != nil {
		return nil, err
	}
	return data, nil
}

func (c *RemoteClient) ReadLink(ctx context.Context, entryStart, entryEnd uint64) ([]byte, error) {
	params := map[string]any{
		"entry_start": entryStart,
		"entry_end":   entryEnd,
	}
	var target []byte
	if err := c.pipe.Call(ctx, "pxar.ReadLink", params, &target); err != nil {
		return nil, err
	}
	return target, nil
}

func (c *RemoteClient) ListXAttrs(ctx context.Context, entryStart, entryEnd uint64) (map[string][]byte, error) {
	params := map[string]any{
		"entry_start": entryStart,
		"entry_end":   entryEnd,
	}
	var xattrs map[string][]byte
	if err := c.pipe.Call(ctx, "pxar.ListXAttrs", params, &xattrs); err != nil {
		return nil, err
	}
	return xattrs, nil
}

func (c *RemoteClient) Close() error {
	if c.pipe != nil {
		c.pipe.Close()
	}
	return nil
}
