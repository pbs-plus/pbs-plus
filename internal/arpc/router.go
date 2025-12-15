package arpc

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/utils/safemap"
	"github.com/quic-go/quic-go"
)

type HandlerFunc func(req *Request) (Response, error)

type Router struct {
	handlers *safemap.Map[string, HandlerFunc]
}

func NewRouter() Router {
	return Router{handlers: safemap.New[string, HandlerFunc]()}
}

func (r *Router) Handle(method string, handler HandlerFunc) {
	r.handlers.Set(method, handler)
}

func (r *Router) CloseHandle(method string) {
	r.handlers.Del(method)
}

func (r *Router) ServeStream(stream *quic.Stream) {
	defer stream.Close()
	defer stream.CancelRead(0)

	_ = stream.SetReadDeadline(time.Now().Add(5 * time.Second))
	dec := cbor.NewDecoder(stream)
	var req Request
	if err := dec.Decode(&req); err != nil {
		_ = stream.SetReadDeadline(time.Time{})
		writeErrorResponse(stream, http.StatusBadRequest, err)
		return
	}
	_ = stream.SetReadDeadline(time.Time{})

	if req.Method == "" {
		writeErrorResponse(stream, http.StatusBadRequest, errors.New("missing method field"))
		return
	}

	handler, ok := r.handlers.Get(req.Method)
	if !ok {
		writeErrorResponse(stream, http.StatusNotFound, fmt.Errorf("method not found: %s", req.Method))
		return
	}

	req.Context = stream.Context()
	resp, err := handler(&req)
	if err != nil {
		writeErrorResponse(stream, http.StatusInternalServerError, err)
		return
	}

	respBytes, err := cborEncMode.Marshal(&resp)
	if err != nil {
		writeErrorResponse(stream, http.StatusInternalServerError, err)
		return
	}

	if _, err := stream.Write(respBytes); err != nil {
		stream.CancelWrite(quicErrWriteResponse)
		return
	}

	if resp.Status == 213 && resp.RawStream != nil {
		deadline := time.Now().Add(5 * time.Second)
		_ = stream.SetReadDeadline(deadline)
		var b [1]byte
		if _, err := io.ReadFull(stream, b[:]); err != nil || b[0] != 0x01 {
			stream.CancelWrite(quicErrRawHandshakeFail)
			_ = stream.SetReadDeadline(time.Time{})
			return
		}
		_ = stream.SetReadDeadline(time.Time{})
		resp.RawStream(stream)
	}
}
