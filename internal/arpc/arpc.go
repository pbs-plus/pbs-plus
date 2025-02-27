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

	"github.com/goccy/go-json"
	"github.com/valyala/fastjson"
	"github.com/xtaci/smux"
)

// arenaPool is a pool for fastjson.Arena objects so we can reuse scratch memory.
var arenaPool = sync.Pool{
	New: func() interface{} {
		return &fastjson.Arena{}
	},
}

// Request defines the JSON request format sent over a stream.
// (Note that the Payload field is now a pointer to fastjson.Value.)
type Request struct {
	Method  string          `json:"method"`
	Payload *fastjson.Value `json:"payload"`
	Headers http.Header     `json:"headers,omitempty"`
}

// Response defines the JSON response format.
// The Data field is now a pointer to a fastjson.Value.
// (If you need to convert data into a Go struct, you can write a helper that
// uses fastjson’s getters.)
type Response struct {
	Status  int             `json:"status"`
	Message string          `json:"message,omitempty"`
	Data    *fastjson.Value `json:"data,omitempty"`
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

// CloseHandle removes the handler for the given method.
func (r *Router) CloseHandle(method string) {
	r.handlersMu.Lock()
	defer r.handlersMu.Unlock()

	delete(r.handlers, method)
}

// ServeStream reads one JSON‑encoded Request from the given stream,
// dispatches it to the appropriate handler, and writes back a JSON response.
func (r *Router) ServeStream(stream *smux.Stream) {
	defer stream.Close()

	reader := bufio.NewReader(stream)
	// Read until the newline (Encode always appends '\n')
	dataBytes, err := reader.ReadBytes('\n')
	if err != nil && err != io.EOF {
		writeErrorResponse(
			stream,
			http.StatusBadRequest,
			"failed to read request: "+err.Error(),
		)
		return
	}
	dataBytes = bytes.TrimSpace(dataBytes)
	var parser fastjson.Parser
	root, err := parser.ParseBytes(dataBytes)
	if err != nil {
		writeErrorResponse(
			stream,
			http.StatusBadRequest,
			"invalid request JSON: "+err.Error(),
		)
		return
	}

	// Build the Request.
	reqMethod := string(root.GetStringBytes("method"))
	if reqMethod == "" {
		writeErrorResponse(
			stream,
			http.StatusBadRequest,
			"missing method field",
		)
		return
	}
	req := Request{
		Method:  reqMethod,
		Payload: root.Get("payload"),
		// (You can also extract headers, if desired.)
	}

	r.handlersMu.RLock()
	handler, ok := r.handlers[req.Method]
	r.handlersMu.RUnlock()
	if !ok {
		writeErrorResponse(
			stream,
			http.StatusNotFound,
			"method not found: "+req.Method,
		)
		return
	}
	resp, err := handler(req)
	if err != nil {
		writeErrorResponse(
			stream,
			http.StatusInternalServerError,
			err.Error(),
		)
		return
	}

	// Build the JSON response using a pooled arena.
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
		// Use encodeValue so that if resp.Data is a Go type rather than a
		// *fastjson.Value, it gets encoded correctly.
		dataVal, err := encodeValue(respArena, resp.Data)
		if err != nil {
			writeErrorResponse(
				stream,
				http.StatusInternalServerError,
				"failed to encode response data: "+err.Error(),
			)
			return
		}
		respObj.Set("data", dataVal)
	}
	respBytes := respObj.MarshalTo(nil)
	_, _ = stream.Write(respBytes)
}

// writeErrorResponse writes a JSON error response using fastjson.
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
	_, _ = stream.Write(respBytes)
}

// --- Session and RPC Call Helpers ---

// ReconnectConfig holds parameters for automatic reconnection.
type ReconnectConfig struct {
	AutoReconnect  bool
	DialFunc       func() (net.Conn, error)
	UpgradeFunc    func(net.Conn) (*Session, error)
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	ReconnectCtx   context.Context
}

// Session wraps an underlying smux.Session.
type Session struct {
	mu                  sync.RWMutex
	reconnectMu         sync.Mutex
	sess                *smux.Session
	reconnectConfig     *ReconnectConfig
	reconnectInProgress bool
}

// NewServerSession creates a new multiplexer session for the server side.
func NewServerSession(conn net.Conn, config *smux.Config) (*Session, error) {
	s, err := smux.Server(conn, config)
	if err != nil {
		return nil, err
	}
	return &Session{sess: s}, nil
}

// NewClientSession creates a new multiplexer session for the client side.
func NewClientSession(conn net.Conn, config *smux.Config) (*Session, error) {
	s, err := smux.Client(conn, config)
	if err != nil {
		return nil, err
	}
	return &Session{sess: s}, nil
}

// EnableAutoReconnect enables automatic reconnection for this session.
func (s *Session) EnableAutoReconnect(rc *ReconnectConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reconnectConfig = rc
}

// attemptReconnect connects the session using backoff (call with s.mu locked).
func (s *Session) attemptReconnect() error {
	s.reconnectMu.Lock()
	defer s.reconnectMu.Unlock()

	if s.sess != nil && !s.sess.IsClosed() {
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

// Call initiates a request/response conversation on a new stream.
func (s *Session) Call(method string, payload interface{}) (*Response, error) {
	return s.CallContext(context.Background(), method, payload)
}

// CallContext performs an RPC call over a new stream. It builds the request
// using fastjson (with a pooled arena), writes it to the stream, reads the
// complete response (terminated by a newline) and parses it using fastjson.
// If a stream cannot be opened and auto‑reconnect is enabled, it will try to reconnect.
func (s *Session) CallContext(
	ctx context.Context,
	method string,
	payload interface{},
) (*Response, error) {
	s.mu.RLock()
	curSession := s.sess
	rc := s.reconnectConfig
	s.mu.RUnlock()

	// Open a new stream.
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
	defer stream.Close()

	// Apply context deadlines if set.
	if deadline, ok := ctx.Deadline(); ok {
		stream.SetWriteDeadline(deadline)
		stream.SetReadDeadline(deadline)
	}

	writer := bufio.NewWriter(stream)
	reader := bufio.NewReader(stream)

	reqBytes, err := buildRequestJSON(method, payload, nil)
	if err != nil {
		return nil, err
	}

	if _, err := writer.Write(reqBytes); err != nil {
		return nil, err
	}
	if err := writer.Flush(); err != nil {
		return nil, err
	}

	respBytes, err := reader.ReadBytes('\n')
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

	var parser fastjson.Parser
	root, err := parser.ParseBytes(respBytes)
	if err != nil {
		return nil, err
	}

	// Build the Response.
	var resp Response
	resp.Status = root.GetInt("status")
	if msg := root.Get("message"); msg != nil {
		resp.Message = string(msg.GetStringBytes())
	}
	resp.Data = root.Get("data")
	return &resp, nil
}

// CallJSON performs an RPC call using CallContext,
// then unmarshals the returned response data into v.
// It returns an error if the call fails or if the response indicates an error.
func (s *Session) CallJSON(ctx context.Context, method string, payload interface{}, v interface{}) error {
	resp, err := s.CallContext(ctx, method, payload)
	if err != nil {
		return err
	}

	// Check that the response status is OK.
	if resp.Status != http.StatusOK {
		return fmt.Errorf("RPC error: %s (status %d)", resp.Message, resp.Status)
	}

	// If no data is returned, nothing to unmarshal.
	if resp.Data == nil {
		return nil
	}

	// Convert the fastjson.Value to raw JSON bytes.
	dataBytes := resp.Data.MarshalTo(nil)
	// Unmarshal the bytes into v using your JSON library.
	if err := json.Unmarshal(dataBytes, v); err != nil {
		return err
	}
	return nil
}

// CallJSONDirect performs an RPC call using CallContext and then passes the
// response’s fastjson.Value to the provided decoder callback. If the
// response status is not OK, it returns an error.
func (s *Session) CallJSONDirect(
	ctx context.Context,
	method string,
	payload interface{},
	decoder func(*fastjson.Value) error,
) error {
	resp, err := s.CallContext(ctx, method, payload)
	if err != nil {
		return err
	}
	if resp.Status != http.StatusOK {
		return fmt.Errorf("RPC error: %s (status %d)", resp.Message, resp.Status)
	}
	if resp.Data == nil {
		return nil
	}
	return decoder(resp.Data)
}

// CallJSONWithBuffer performs an RPC call whose response data is directly written
// into the provided buffer.
func (s *Session) CallJSONWithBuffer(
	ctx context.Context,
	method string,
	payload interface{},
	buffer []byte,
) (int, bool, error) {
	s.mu.RLock()
	curSession := s.sess
	rc := s.reconnectConfig
	s.mu.RUnlock()

	stream, err := curSession.OpenStream()
	if err != nil {
		if rc != nil && rc.AutoReconnect {
			if err2 := s.attemptReconnect(); err2 != nil {
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
	defer stream.Close()

	if deadline, ok := ctx.Deadline(); ok {
		stream.SetWriteDeadline(deadline)
		stream.SetReadDeadline(deadline)
	}

	writer := bufio.NewWriter(stream)
	reader := bufio.NewReader(stream)

	reqBytes, err := buildRequestJSON(
		method,
		payload,
		map[string]string{"X-Direct-Buffer": "true"},
	)
	if err != nil {
		return 0, false, err
	}
	if _, err := writer.Write(reqBytes); err != nil {
		return 0, false, err
	}
	if err := writer.Flush(); err != nil {
		return 0, false, err
	}

	metaData, err := reader.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return 0, false, err
	}
	metaData = bytes.TrimSpace(metaData)
	if len(metaData) == 0 {
		return 0, false, fmt.Errorf("no metadata received")
	}

	metaArena := arenaPool.Get().(*fastjson.Arena)
	defer func() {
		metaArena.Reset()
		arenaPool.Put(metaArena)
	}()
	var metaParser fastjson.Parser
	metaRoot, err := metaParser.ParseBytes(metaData)
	if err != nil {
		return 0, false, err
	}
	status := metaRoot.GetInt("status")
	if status != http.StatusOK {
		message := string(metaRoot.GetStringBytes("message"))
		return 0, false, fmt.Errorf("RPC error: %s (status %d)", message, status)
	}

	dataObj := metaRoot.Get("data")
	contentLength := 0
	isEOF := false
	if dataObj != nil {
		contentLength = dataObj.GetInt("bytes_available")
		isEOF = dataObj.GetBool("eof")
	}
	if contentLength <= 0 {
		return 0, isEOF, nil
	}

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

// Serve continuously accepts incoming streams on the session and dispatches them
// to the provided router. On error, if auto‑reconnect is enabled it will try to
// reconnect and continue.
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

// DialWithBackoff repeatedly attempts to establish a connection by dialing and then upgrading it.
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
func HijackUpgradeHTTP(w http.ResponseWriter, r *http.Request, config *smux.Config) (*Session, error) {
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		return nil, fmt.Errorf("response writer does not support hijacking")
	}

	conn, rw, err := hijacker.Hijack()
	if err != nil {
		return nil, err
	}

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
func UpgradeHTTPClient(conn net.Conn, requestPath, host string, headers http.Header,
	config *smux.Config) (*Session, error) {

	reqLines := []string{
		fmt.Sprintf("GET %s HTTP/1.1", requestPath),
		fmt.Sprintf("Host: %s", host),
	}
	if headers != nil {
		for key, values := range headers {
			for _, value := range values {
				reqLines = append(reqLines, fmt.Sprintf("%s: %s", key, value))
			}
		}
	}
	reqLines = append(reqLines,
		"Upgrade: tcp",
		"Connection: Upgrade",
		"",
		"",
	)
	reqStr := strings.Join(reqLines, "\r\n")
	if _, err := conn.Write([]byte(reqStr)); err != nil {
		return nil, err
	}

	reader := bufio.NewReader(conn)
	statusLine, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if !strings.Contains(statusLine, "101") {
		return nil, fmt.Errorf("expected status 101, got: %s", statusLine)
	}

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
