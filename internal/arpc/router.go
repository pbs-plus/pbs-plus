package arpc

import (
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/hashicorp/yamux"
	"github.com/pbs-plus/pbs-plus/internal/utils/safemap"
)

// Buffer pool for reusing buffers across requests and responses.
var bufferPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 4096) // Default buffer size
	},
}

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
// and writes back the Response. In case of errors, an error response is sent.
func (r *Router) ServeStream(stream *yamux.Stream) {
	defer stream.Close()

	// Get a buffer from the pool for the request
	reqBuf := bufferPool.Get().([]byte)
	defer bufferPool.Put(reqBuf)

	// Read the request from the stream
	n, err := stream.Read(reqBuf)
	if err != nil {
		writeErrorResponse(stream, http.StatusBadRequest, err)
		return
	}

	// Decode the request
	var req Request
	if err := req.Decode(reqBuf[:n]); err != nil {
		writeErrorResponse(stream, http.StatusBadRequest, err)
		return
	}

	// Validate the method field
	if req.Method == "" {
		writeErrorResponse(stream, http.StatusBadRequest, errors.New("missing method field"))
		return
	}

	// Find the handler for the method
	handler, ok := r.handlers.Get(req.Method)
	if !ok {
		writeErrorResponse(stream, http.StatusNotFound, fmt.Errorf("method not found: %s", req.Method))
		return
	}

	// Call the handler
	resp, err := handler(req)
	if err != nil {
		writeErrorResponse(stream, http.StatusInternalServerError, err)
		return
	}

	// Encode and write the response
	respBytes, err := resp.Encode()
	if err != nil {
		writeErrorResponse(stream, http.StatusInternalServerError, err)
		return
	}

	if _, err := stream.Write(respBytes); err != nil {
		return
	}

	// If this is a streaming response, execute the callback
	if resp.Status == 213 && resp.RawStream != nil {
		resp.RawStream(stream)
	}
}
