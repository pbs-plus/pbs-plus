package arpc

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/sonroyaalmerol/pbs-plus/internal/utils/safemap"
	"github.com/valyala/bytebufferpool"
	"github.com/xtaci/smux"
)

// HandlerFunc handles an RPC Request and returns a Response.
type HandlerFunc func(req Request) (Response, error)

// Router holds a map from method names to handler functions.
type Router struct {
	handlers *safemap.Map[string, HandlerFunc]
}

// NewRouter creates a new Router instance.
func NewRouter() Router {
	return Router{handlers: safemap.New[string, HandlerFunc]()}
}

// Handle registers a handler for a given method name.
func (r *Router) Handle(method string, handler HandlerFunc) {
	r.handlers.Set(method, handler)
}

// CloseHandle removes a handler.
func (r *Router) CloseHandle(method string) {
	r.handlers.Del(method)
}

// ServeStream reads a single RPC request from the stream, routes it to the correct handler,
// and writes back the Response. In case of errors an error response is sent.
func (r *Router) ServeStream(stream *smux.Stream) {
	defer stream.Close()

	pm, err := readMsgpMsgPooled(stream)
	if err != nil {
		writeErrorResponse(stream, http.StatusBadRequest, err)
		return
	}
	defer bytebufferpool.Put(pm)

	var req Request
	if _, err := req.UnmarshalMsg(pm.B); err != nil {
		writeErrorResponse(stream, http.StatusBadRequest, err)
		return
	}

	if req.Method == "" {
		writeErrorResponse(stream, http.StatusBadRequest,
			errors.New("missing method field"))
		return
	}

	handler, ok := r.handlers.Get(req.Method)
	if !ok {
		writeErrorResponse(
			stream,
			http.StatusNotFound,
			fmt.Errorf("method not found: %s", req.Method),
		)
		return
	}

	resp, err := handler(req)
	if err != nil {
		writeErrorResponse(stream, http.StatusInternalServerError, err)
		return
	}

	// Write response status first
	respBytes, err := marshalWithPool(&resp)
	if err != nil {
		writeErrorResponse(stream, http.StatusInternalServerError, err)
		return
	}
	defer bytebufferpool.Put(respBytes)

	if err := writeMsgpMsg(stream, respBytes.B); err != nil {
		return
	}

	// If this is a streaming response, execute the callback
	if resp.Status == 213 && resp.RawStream != nil {
		resp.RawStream(stream)
	}
}
