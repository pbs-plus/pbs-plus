package arpc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/utils/safemap"
)

// Buffer pool for reusing buffers across requests and responses
var bufferPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 4096)
	},
}

// HandlerFunc handles an RPC Request and returns a Response
type HandlerFunc func(req Request) (Response, error)

// Router holds a map from method names to handler functions
type Router struct {
	handlers *safemap.Map[string, HandlerFunc]
}

// NewRouter creates a new Router instance
func NewRouter() Router {
	return Router{handlers: safemap.New[string, HandlerFunc]()}
}

// Handle registers a handler for a given method name
func (r *Router) Handle(method string, handler HandlerFunc) {
	r.handlers.Set(method, handler)
}

// CloseHandle removes a handler
func (r *Router) CloseHandle(method string) {
	r.handlers.Del(method)
}

// ServeConn reads a single RPC request from the connection, routes it,
// and writes back the Response
func (r *Router) ServeConn(conn net.Conn) error {
	// Read the length prefix (4 bytes)
	prefix := headerPool.Get().([]byte)
	defer headerPool.Put(prefix)

	if _, err := io.ReadFull(conn, prefix); err != nil {
		if err == io.EOF {
			return err // Clean connection close
		}
		writeErrorResponse(conn, http.StatusBadRequest, err)
		return err
	}

	totalLength := binary.LittleEndian.Uint32(prefix)
	if totalLength < 4 {
		err := fmt.Errorf("invalid total length %d", totalLength)
		writeErrorResponse(conn, http.StatusBadRequest, err)
		return err
	}

	// Use pool buffer for small messages, allocate for larger ones
	var reqBuf []byte
	var usePool bool

	if totalLength <= 4096 {
		reqBuf = bufferPool.Get().([]byte)[:totalLength]
		usePool = true
	} else {
		reqBuf = make([]byte, totalLength)
	}

	if usePool {
		defer bufferPool.Put(reqBuf[:cap(reqBuf)])
	}

	// Copy the prefix we already read
	copy(reqBuf, prefix)

	// Read the remaining bytes
	if _, err := io.ReadFull(conn, reqBuf[4:]); err != nil {
		writeErrorResponse(conn, http.StatusBadRequest, err)
		return err
	}

	// Decode the request
	var req Request
	if err := req.Decode(reqBuf); err != nil {
		writeErrorResponse(conn, http.StatusBadRequest, err)
		return err
	}

	// Validate the method field
	if req.Method == "" {
		err := errors.New("missing method field")
		writeErrorResponse(conn, http.StatusBadRequest, err)
		return err
	}

	// Find the handler for the method
	handler, ok := r.handlers.Get(req.Method)
	if !ok {
		err := fmt.Errorf("method not found: %s", req.Method)
		writeErrorResponse(conn, http.StatusNotFound, err)
		return err
	}

	// Call the handler
	resp, err := handler(req)
	if err != nil {
		writeErrorResponse(conn, http.StatusInternalServerError, err)
		return err
	}

	// Encode and write the response
	respBytes, err := resp.Encode()
	if err != nil {
		writeErrorResponse(conn, http.StatusInternalServerError, err)
		return err
	}

	if _, err := conn.Write(respBytes); err != nil {
		return err
	}

	// If this is a streaming response, execute the callback
	if resp.Status == 213 && resp.RawStream != nil {
		resp.RawStream(conn)
	}

	return nil
}
