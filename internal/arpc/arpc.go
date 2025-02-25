package arpc

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/xtaci/smux"
)

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
	defer stream.Close()

	decoder := json.NewDecoder(stream)
	encoder := json.NewEncoder(stream)

	var req Request
	if err := decoder.Decode(&req); err != nil {
		encoder.Encode(Response{
			Status:  400,
			Message: "invalid request: " + err.Error(),
		})
		return
	}

	r.handlersMu.RLock()
	handler, ok := r.handlers[req.Method]
	r.handlersMu.RUnlock()
	if !ok {
		encoder.Encode(Response{
			Status:  404,
			Message: "method not found: " + req.Method,
		})
		return
	}

	resp, err := handler(req)
	if err != nil {
		encoder.Encode(Response{
			Status:  500,
			Message: err.Error(),
		})
		return
	}
	encoder.Encode(resp)
}

//
// Reconnection Support
//

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

//
// Session and Auto-Reconnect Implementation
//

// Session wraps an underlying smux.Session.
// It now also holds an optional reconnect configuration and a mutex for protecting
// the underlying session pointer.
type Session struct {
	mu              sync.RWMutex
	sess            *smux.Session
	reconnectConfig *ReconnectConfig
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
	// Preserve the reconnect configuration.
	newSess.reconnectConfig = s.reconnectConfig
	s.sess = newSess.sess
	return nil
}

//
// Request/Response Calls with Auto-Reconnect and Timeout Support
//

// Call is a helper method for initiating a request/response conversation
// on a new stream. It marshals the provided payload, sends the request, and
// waits for the JSON response.
func (s *Session) Call(method string, payload interface{}) (*Response, error) {
	return s.CallContext(context.Background(), method, payload)
}

// CallContext is similar to Call but allows passing a context with a deadline or timeout.
// If the call does not complete before ctx is done, it aborts with the context's error.
// In case of an error that appears to be from a disconnected session,
// it will try to reconnect (if enabled) and then retry opening a stream.
func (s *Session) CallContext(ctx context.Context, method string, payload interface{}) (*Response, error) {
	// Attempt to open a stream.
	s.mu.RLock()
	curSession := s.sess
	rc := s.reconnectConfig
	s.mu.RUnlock()

	stream, err := curSession.OpenStream()
	if err != nil && rc != nil && rc.AutoReconnect {
		// Try to reconnect.
		s.mu.Lock()
		err2 := s.attemptReconnect()
		s.mu.Unlock()
		if err2 != nil {
			return nil, err2
		}
		// Retry with new session.
		s.mu.RLock()
		curSession = s.sess
		s.mu.RUnlock()
		stream, err = curSession.OpenStream()
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	// Use a channel to signal the result.
	type result struct {
		resp *Response
		err  error
	}
	resCh := make(chan result, 1)

	go func() {
		defer stream.Close()

		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			resCh <- result{nil, err}
			return
		}

		req := Request{
			Method:  method,
			Payload: json.RawMessage(payloadBytes),
		}

		encoder := json.NewEncoder(stream)
		if err := encoder.Encode(req); err != nil {
			resCh <- result{nil, err}
			return
		}

		decoder := json.NewDecoder(stream)
		var resp Response
		if err := decoder.Decode(&resp); err != nil {
			resCh <- result{nil, err}
			return
		}
		resCh <- result{&resp, nil}
	}()

	select {
	case <-ctx.Done():
		stream.Close() // ensure the stream is closed on timeout
		return nil, ctx.Err()
	case res := <-resCh:
		return res.resp, res.err
	}
}

// CallJSON performs the RPC call and decodes the JSON data into v.
// It is similar in spirit to http.Get followed by json.NewDecoder(resp.Body).Decode(&v).
func (s *Session) CallJSON(ctx context.Context, method string, payload interface{}, v interface{}) error {

	resp, err := s.CallContext(ctx, method, payload)
	if err != nil {
		return err
	}
	if resp.Status != http.StatusOK && resp.Status != 200 {
		return fmt.Errorf("server error: %s", resp.Message)
	}
	// Since resp.Data is an interface{},
	// we re-marshal it to JSON and then unmarshal into v.
	dataBytes, err := json.Marshal(resp.Data)
	if err != nil {
		return err
	}
	return json.Unmarshal(dataBytes, v)
}

// CallWithHeaders is similar to CallContext but allows passing custom http.Header with
// the request. The headers will be embedded in the Request.Headers field.
func (s *Session) CallWithHeaders(ctx context.Context, method string, payload interface{}, headers http.Header) (*Response, error) {
	s.mu.RLock()
	curSession := s.sess
	rc := s.reconnectConfig
	s.mu.RUnlock()

	stream, err := curSession.OpenStream()
	if err != nil && rc != nil && rc.AutoReconnect {
		s.mu.Lock()
		err2 := s.attemptReconnect()
		s.mu.Unlock()
		if err2 != nil {
			return nil, err2
		}
		s.mu.RLock()
		curSession = s.sess
		s.mu.RUnlock()
		stream, err = curSession.OpenStream()
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	type result struct {
		resp *Response
		err  error
	}
	resCh := make(chan result, 1)

	go func() {
		defer stream.Close()

		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			resCh <- result{nil, err}
			return
		}

		req := Request{
			Method:  method,
			Payload: json.RawMessage(payloadBytes),
			Headers: headers,
		}

		encoder := json.NewEncoder(stream)
		if err := encoder.Encode(req); err != nil {
			resCh <- result{nil, err}
			return
		}

		decoder := json.NewDecoder(stream)
		var resp Response
		if err := decoder.Decode(&resp); err != nil {
			resCh <- result{nil, err}
			return
		}
		resCh <- result{&resp, nil}
	}()

	select {
	case <-ctx.Done():
		stream.Close() // ensure the stream is closed on timeout
		return nil, ctx.Err()
	case res := <-resCh:
		return res.resp, res.err
	}
}

//
// Serve (Accepting Streams) with Auto-Reconnect Support
//

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

//
// Close
//

// Close shuts down the underlying smux session.
func (s *Session) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sess.Close()
}

//
// Reconnection Helper: DialWithBackoff
//

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

//
// HTTP Upgrade Helpers
//

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
