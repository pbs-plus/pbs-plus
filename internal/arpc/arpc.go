package arpc

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	json "github.com/goccy/go-json"
	"github.com/valyala/fastjson"
	"github.com/xtaci/smux"
)

var arenaPool = sync.Pool{
	New: func() interface{} {
		return &fastjson.Arena{}
	},
}

// Request defines the JSON request format sent over a stream.
type Request struct {
	Method  string          `json:"method"`
	Payload json.RawMessage `json:"payload"`
	Headers http.Header     `json:"headers,omitempty"`
}

// Response defines the JSON response format.
type Response struct {
	Status  int         `json:"status"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

// HandlerFunc is the type of function that can handle a Request.
type HandlerFunc func(req Request) (Response, error)

// Router holds a mapping of method names to handlers.
type Router struct {
	handlers   map[string]HandlerFunc
	handlersMu sync.RWMutex
}

// NewRouter creates and returns a new Router.
func NewRouter() *Router {
	return &Router{handlers: make(map[string]HandlerFunc)}
}

// Handle registers a new handler for the given method.
func (r *Router) Handle(method string, handler HandlerFunc) {
	r.handlersMu.Lock()
	defer r.handlersMu.Unlock()

	r.handlers[method] = handler
}

func (r *Router) CloseHandle(method string) {
	r.handlersMu.Lock()
	defer r.handlersMu.Unlock()

	delete(r.handlers, method)
}

// ServeStream reads one JSON-encoded Request from the given stream,
// dispatches it to the appropriate handler, and writes back a JSON response.
func (r *Router) ServeStream(stream *smux.Stream) {
	defer func() {
		if err := stream.Close(); err != nil {
		}
	}()

	reader := bufio.NewReader(stream)
	// Read until a newline; since json.Encoder.Encode always appends '\n'
	dataBytes, err := reader.ReadBytes('\n')
	if err != nil && err != io.EOF {
		writeErrorResponse(stream, http.StatusBadRequest, "failed to read request: "+err.Error())
		return
	}
	// Trim any trailing newline or whitespace.
	dataBytes = bytes.TrimSpace(dataBytes)

	// Obtain a fastjson arena from the pool for parsing.
	arena := arenaPool.Get().(*fastjson.Arena)
	defer func() {
		arena.Reset()
		arenaPool.Put(arena)
	}()

	var parser fastjson.Parser
	v, err := parser.ParseBytes(dataBytes)
	if err != nil {
		writeErrorResponse(stream, http.StatusBadRequest, "invalid request JSON: "+err.Error())
		return
	}

	// Build the Request.
	reqMethod := string(v.GetStringBytes("method"))
	if reqMethod == "" {
		writeErrorResponse(stream, http.StatusBadRequest, "missing method field")
		return
	}

	req := Request{
		Method: reqMethod,
	}
	if payload := v.Get("payload"); payload != nil {
		req.Payload = payload.MarshalTo(nil)
	}

	r.handlersMu.RLock()
	handler, ok := r.handlers[req.Method]
	r.handlersMu.RUnlock()
	if !ok {
		writeErrorResponse(stream, http.StatusNotFound, "method not found: "+req.Method)
		return
	}

	resp, err := handler(req)
	if err != nil {
		writeErrorResponse(stream, http.StatusInternalServerError, err.Error())
		return
	}

	// Prepare the response using another arena from the pool.
	respArena := arenaPool.Get().(*fastjson.Arena)
	defer func() {
		respArena.Reset()
		arenaPool.Put(respArena)
	}()

	respObj := respArena.NewObject()
	respObj.Set("status", respArena.NewNumberInt(resp.Status))
	if resp.Message != "" {
		respObj.Set("message", respArena.NewString(resp.Message))
	}
	if resp.Data != nil {
		dataBytes, err := json.Marshal(resp.Data)
		if err != nil {
			writeErrorResponse(stream, http.StatusInternalServerError, "failed to marshal response data: "+err.Error())
			return
		}
		var p fastjson.Parser
		dataVal, err := p.ParseBytes(dataBytes)
		if err != nil {
			writeErrorResponse(stream, http.StatusInternalServerError, "failed to parse response data: "+err.Error())
			return
		}
		respObj.Set("data", dataVal)
	}

	respBytes := respObj.MarshalTo(nil)
	if _, err := stream.Write(respBytes); err != nil {
	}
}

// Helper function to write error responses
func writeErrorResponse(stream *smux.Stream, status int, message string) {
	arena := arenaPool.Get().(*fastjson.Arena)
	defer func() {
		arena.Reset()
		arenaPool.Put(arena)
	}()
	obj := arena.NewObject()
	obj.Set("status", arena.NewNumberInt(status))
	obj.Set("message", arena.NewString(message))
	respBytes := obj.MarshalTo(nil)
	if _, err := stream.Write(respBytes); err != nil {
	}
}

// ReconnectConfig holds parameters for automatic reconnection.
type ReconnectConfig struct {
	// AutoReconnect must be true to enable automatic reconnection.
	AutoReconnect bool

	// DialFunc is a function that establishes a new raw connection.
	DialFunc func() (net.Conn, error)

	// UpgradeFunc upgrades a raw connection (e.g. performing HTTP upgrade)
	// and returns a new Session.
	UpgradeFunc func(net.Conn) (*Session, error)

	// InitialBackoff is the backoff duration for the first reconnect attempt.
	InitialBackoff time.Duration

	// MaxBackoff is the maximum allowed backoff duration.
	MaxBackoff time.Duration

	// ReconnectCtx is the context used during reconnection; if cancelled, reconnection aborts.
	ReconnectCtx context.Context
}

// Session wraps an underlying smux.Session.
// It now also holds an optional reconnect configuration and a mutex for protecting
// the underlying session pointer.
type Session struct {
	mu                  sync.RWMutex
	reconnectMu         sync.Mutex
	sess                *smux.Session
	reconnectConfig     *ReconnectConfig
	reconnectInProgress bool
}

// NewServerSession creates a new multiplexer session for the server side.
// The config parameter can be nil to use the default smux configuration.
func NewServerSession(conn net.Conn, config *smux.Config) (*Session, error) {
	s, err := smux.Server(conn, config)
	if err != nil {
		return nil, err
	}
	return &Session{sess: s}, nil
}

// NewClientSession creates a new multiplexer session for the client side.
// The config parameter can be nil to use the default smux configuration.
func NewClientSession(conn net.Conn, config *smux.Config) (*Session, error) {
	s, err := smux.Client(conn, config)
	if err != nil {
		return nil, err
	}
	return &Session{sess: s}, nil
}

// EnableAutoReconnect enables automatic reconnection on this session.
// The supplied ReconnectConfig is used to reconnect if the underlying session disconnects.
func (s *Session) EnableAutoReconnect(rc *ReconnectConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reconnectConfig = rc
}

// internal helper. attemptReconnect uses DialWithBackoff to re-establish the underlying session.
// It is expected to be called with s.mu locked.
func (s *Session) attemptReconnect() error {
	s.reconnectMu.Lock()
	defer s.reconnectMu.Unlock()

	// Double-check condition in case another goroutine already reconnected.
	if s.sess != nil && s.sess.IsClosed() == false {
		return nil
	}

	if s.reconnectConfig == nil || !s.reconnectConfig.AutoReconnect {
		return fmt.Errorf("auto reconnect not configured")
	}

	newSess, err := DialWithBackoff(
		s.reconnectConfig.ReconnectCtx,
		s.reconnectConfig.DialFunc,
		s.reconnectConfig.UpgradeFunc,
		s.reconnectConfig.InitialBackoff,
		s.reconnectConfig.MaxBackoff,
	)
	if err != nil {
		return err
	}

	newSess.reconnectConfig = s.reconnectConfig
	s.mu.Lock()
	s.sess = newSess.sess
	s.mu.Unlock()
	return nil
}

// Call is a helper method for initiating a request/response conversation
// on a new stream. It marshals the provided payload, sends the request, and
// waits for the JSON response.
func (s *Session) Call(method string, payload interface{}) (*Response, error) {
	return s.CallContext(context.Background(), method, payload)
}

// CallContext performs an RPC call over a new stream using a JSON request.
// It builds the request using fastjson (with a pooled Arena), writes it using a buffered writer,
// applies any context deadline, reads the complete response using a buffered reader, and finally
// parses the response JSON using fastjson. If a stream cannot be opened and auto‑reconnect is enabled,
// this method will try to reconnect.
func (s *Session) CallContext(
	ctx context.Context,
	method string,
	payload interface{},
) (*Response, error) {
	// Obtain the current session and reconnect configuration.
	s.mu.RLock()
	curSession := s.sess
	rc := s.reconnectConfig
	s.mu.RUnlock()

	// Try to open a new stream.
	stream, err := curSession.OpenStream()
	if err != nil {
		if rc != nil && rc.AutoReconnect {
			if err2 := s.attemptReconnect(); err2 != nil {
				return nil, err2
			}
			s.mu.RLock()
			curSession = s.sess
			s.mu.RUnlock()
			stream, err = curSession.OpenStream()
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	// Ensure stream closure.
	defer func() {
		if cerr := stream.Close(); cerr != nil {
		}
	}()

	// Apply context timeouts to I/O if a deadline is set.
	if deadline, ok := ctx.Deadline(); ok {
		stream.SetWriteDeadline(deadline)
		stream.SetReadDeadline(deadline)
	}

	// Wrap the stream in buffered I/O objects.
	writer := bufio.NewWriter(stream)
	reader := bufio.NewReader(stream)

	// --- Build the JSON Request ---
	reqBytes, err := buildRequestJSON(method, payload, nil)
	if err != nil {
		return nil, err
	}

	// Write the request (which already appends a newline)
	if _, err := writer.Write(reqBytes); err != nil {
		return nil, err
	}
	if err := writer.Flush(); err != nil {
		return nil, err
	}

	// --- Read the Response ---
	respBytes, err := reader.ReadBytes('\n')
	// Consider EOF acceptable if we received some bytes.
	if err != nil && err != io.EOF {
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			return nil, context.DeadlineExceeded
		}
		return nil, err
	}
	respBytes = bytes.TrimSpace(respBytes)
	if len(respBytes) == 0 {
		return nil, fmt.Errorf("empty response")
	}

	// Parse the response JSON using fastjson.
	var respParser fastjson.Parser
	v, err := respParser.ParseBytes(respBytes)
	if err != nil {
		return nil, err
	}

	// Build and return the Response struct.
	var resp Response
	resp.Status = v.GetInt("status")
	if msg := v.Get("message"); msg != nil {
		resp.Message = string(msg.GetStringBytes())
	}
	if data := v.Get("data"); data != nil {
		dataBytes := data.MarshalTo(nil)
		if err := json.Unmarshal(dataBytes, &resp.Data); err != nil {
			return nil, err
		}
	}

	return &resp, nil
}

// CallJSON is a helper that performs an RPC call (via CallContext) and then unmarshals
// the returned response data directly into the provided interface v. It returns an error
// if the RPC call fails or if the response indicates an error.
func (s *Session) CallJSON(
	ctx context.Context,
	method string,
	payload interface{},
	v interface{},
) error {
	resp, err := s.CallContext(ctx, method, payload)
	if err != nil {
		return err
	}

	// Check for error status.
	if resp.Status != http.StatusOK && resp.Status != 200 {
		return fmt.Errorf("RPC error: %s (status %d)", resp.Message, resp.Status)
	}

	// If there is data, marshal it back to JSON and unmarshal into v.
	if resp.Data != nil {
		dataBytes, err := json.Marshal(resp.Data)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(dataBytes, v); err != nil {
			return err
		}
	}
	return nil
}

// CallJSONWithBuffer performs an RPC call where the response data is written
// directly into the provided buffer. It writes a request with a direct-buffer header,
// reads a metadata response (which includes the available byte length and an EOF flag),
// and then reads the binary data from the stream into the buffer using buffered I/O.
func (s *Session) CallJSONWithBuffer(
	ctx context.Context,
	method string,
	payload interface{},
	buffer []byte,
) (int, bool, error) {

	// Retrieve current smux session and its reconnection configuration.
	s.mu.RLock()
	curSession := s.sess
	rc := s.reconnectConfig
	s.mu.RUnlock()

	// Open a new stream.
	stream, err := curSession.OpenStream()
	if err != nil {
		if rc != nil && rc.AutoReconnect {
			err2 := s.attemptReconnect()
			if err2 != nil {
				return 0, false, err2
			}
			s.mu.RLock()
			curSession = s.sess
			s.mu.RUnlock()
			stream, err = curSession.OpenStream()
			if err != nil {
				return 0, false, err
			}
		} else {
			return 0, false, err
		}
	}
	// Ensure the stream is closed.
	defer func() {
		if cerr := stream.Close(); cerr != nil {
		}
	}()

	// If the context has a deadline, set it on the connection.
	if deadline, ok := ctx.Deadline(); ok {
		stream.SetWriteDeadline(deadline)
		stream.SetReadDeadline(deadline)
	}

	// Wrap stream with buffered I/O wrappers.
	writer := bufio.NewWriter(stream)
	reader := bufio.NewReader(stream)

	// --- Build the RPC Request using a pooled fastjson.Arena ---
	reqBytes, err := buildRequestJSON(method, payload, map[string]string{"X-Direct-Buffer": "true"})
	if err != nil {
		return 0, false, err
	}
	if _, err := writer.Write(reqBytes); err != nil {
		return 0, false, err
	}
	if err := writer.Flush(); err != nil {
		return 0, false, err
	}

	// --- Read the metadata response ---
	metaData, err := reader.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return 0, false, err
	}
	metaData = bytes.TrimSpace(metaData)
	if len(metaData) == 0 {
		return 0, false, fmt.Errorf("no metadata received")
	}

	// Parse the metadata response with another pooled arena.
	metaArena := arenaPool.Get().(*fastjson.Arena)
	defer func() {
		metaArena.Reset()
		arenaPool.Put(metaArena)
	}()
	var metaParser fastjson.Parser
	metaVal, err := metaParser.ParseBytes(metaData)
	if err != nil {
		return 0, false, err
	}
	status := metaVal.GetInt("status")
	if status != http.StatusOK {
		message := string(metaVal.GetStringBytes("message"))
		return 0, false, fmt.Errorf("RPC error: %s (status %d)", message, status)
	}

	// Retrieve available binary data length and the EOF flag from metadata.
	dataObj := metaVal.Get("data")
	contentLength := 0
	isEOF := false
	if dataObj != nil {
		contentLength = dataObj.GetInt("bytes_available")
		isEOF = dataObj.GetBool("eof")
	}
	if contentLength <= 0 {
		return 0, isEOF, nil
	}

	// --- Read the binary data into the provided buffer ---
	bytesRead := 0
	for bytesRead < contentLength && bytesRead < len(buffer) {
		n, err := reader.Read(buffer[bytesRead:])
		if err != nil {
			return bytesRead, isEOF, err
		}
		if n == 0 {
			break
		}
		bytesRead += n
	}

	return bytesRead, isEOF, nil
}

// Serve continuously accepts incoming streams on the session.
// Each incoming stream is dispatched to the provided router.
// If an error occurs (typically due to disconnection), and auto-reconnect
// is enabled, it will attempt to re-establish the underlying session and continue.
func (s *Session) Serve(router *Router) error {
	for {
		s.mu.RLock()
		curSession := s.sess
		rc := s.reconnectConfig
		s.mu.RUnlock()

		stream, err := curSession.AcceptStream()
		if err != nil {
			if rc != nil && rc.AutoReconnect {
				s.mu.Lock()
				err2 := s.attemptReconnect()
				s.mu.Unlock()
				if err2 != nil {
					return err2
				}
				// Continue the loop to retry AcceptStream on the new session.
				continue
			} else {
				return err
			}
		}
		go router.ServeStream(stream)
	}
}

// Close shuts down the underlying smux session.
func (s *Session) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sess.Close()
}

// DialWithBackoff repeatedly attempts to establish a connection by calling dialFunc
// and then upgrade it using upgradeFn. It uses exponential backoff between attempts,
// starting with `initial` duration and capping at `max`. The process respects the provided ctx.
func DialWithBackoff(
	ctx context.Context,
	dialFunc func() (net.Conn, error),
	upgradeFn func(conn net.Conn) (*Session, error),
	initial, max time.Duration,
) (*Session, error) {
	delay := initial
	for {
		conn, err := dialFunc()
		if err == nil {
			session, err2 := upgradeFn(conn)
			if err2 == nil {
				return session, nil
			}
			conn.Close()
			err = err2
		}
		// Wait before retrying, or exit if context is cancelled.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
		delay *= 2
		if delay > max {
			delay = max
		}
	}
}

// HijackUpgradeHTTP is a helper for server-side HTTP hijacking.
// It attempts to hijack the HTTP connection from the ResponseWriter,
// writes the 101 Switching Protocols handshake, and then creates and
// returns a new server-side Session using the underlying connection.
// The config parameter is passed to smux.Server (or nil for default config).
func HijackUpgradeHTTP(w http.ResponseWriter, r *http.Request, config *smux.Config) (*Session, error) {
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		return nil, fmt.Errorf("response writer does not support hijacking")
	}

	conn, rw, err := hijacker.Hijack()
	if err != nil {
		return nil, err
	}

	// Write the handshake response.
	_, err = rw.WriteString("HTTP/1.1 101 Switching Protocols\r\n\r\n")
	if err != nil {
		conn.Close()
		return nil, err
	}
	if err = rw.Flush(); err != nil {
		conn.Close()
		return nil, err
	}

	return NewServerSession(conn, config)
}

// UpgradeHTTPClient is a helper for client-side HTTP upgrade.
// Given an established connection, it writes an HTTP GET request to the
// specified requestPath and host, adding custom headers from the provided
// http.Header, along with the necessary Upgrade and Connection headers.
// It then reads and verifies the 101 Switching Protocols response and drains
// the remaining headers. Finally, it creates and returns a new client-side
// Session using the same connection.
// The config parameter is passed to smux.Client (or nil for default config).
func UpgradeHTTPClient(conn net.Conn, requestPath, host string, headers http.Header,
	config *smux.Config) (*Session, error) {

	// Build the HTTP request lines.
	// Start with the Request-Line.
	reqLines := []string{
		fmt.Sprintf("GET %s HTTP/1.1", requestPath),
		fmt.Sprintf("Host: %s", host),
	}

	// Add custom headers (if any).
	if headers != nil {
		for key, values := range headers {
			for _, value := range values {
				reqLines = append(reqLines, fmt.Sprintf("%s: %s", key, value))
			}
		}
	}

	// Ensure the Upgrade and Connection headers are present.
	reqLines = append(reqLines,
		"Upgrade: tcp",
		"Connection: Upgrade",
		"", // empty line to denote end of headers
		"",
	)
	reqStr := strings.Join(reqLines, "\r\n")

	// Write the request to the connection.
	if _, err := conn.Write([]byte(reqStr)); err != nil {
		return nil, err
	}

	// Create a buffered reader to read the HTTP response.
	reader := bufio.NewReader(conn)
	statusLine, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if !strings.Contains(statusLine, "101") {
		return nil, fmt.Errorf("expected status 101, got: %s", statusLine)
	}

	// Drain all remaining header lines.
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		if strings.TrimSpace(line) == "" {
			break
		}
	}

	return NewClientSession(conn, config)
}
