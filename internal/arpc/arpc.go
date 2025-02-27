package arpc

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/vmihailenco/msgpack/v5"
	"github.com/xtaci/smux"
)

// ----------
// MessagePack framing helper functions
// ----------

// readMsgpackMsg reads a framed MessagePack message from r.
// The protocol here is: a 4‑byte big‑endian length header followed by that many data bytes.
func readMsgpackMsg(r io.Reader) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	msgLen := binary.BigEndian.Uint32(lenBuf[:])
	msg := make([]byte, msgLen)
	_, err := io.ReadFull(r, msg)
	return msg, err
}

// writeMsgpackMsg writes msg to w using a 4‑byte length header.
func writeMsgpackMsg(w io.Writer, msg []byte) error {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(msg)))
	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err := w.Write(msg)
	return err
}

// ----------
// Data structures
// ----------

// Request defines the MessagePack‑encoded request format sent over a stream.
type Request struct {
	Method string `msgpack:"method"`
	// We use a raw []byte for the payload.
	Payload []byte      `msgpack:"payload"`
	Headers http.Header `msgpack:"headers,omitempty"`
}

// Response defines the MessagePack‑encoded response format.
type Response struct {
	Status  int    `msgpack:"status"`
	Message string `msgpack:"message,omitempty"`
	Data    []byte `msgpack:"data,omitempty"`
}

// ----------
// Router and stream handling
// ----------

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

// ServeStream reads one MessagePack‑encoded Request from the stream,
// dispatches it to the appropriate handler, and writes back a MessagePack‑encoded response.
// On error an error response is sent back.
func (r *Router) ServeStream(stream *smux.Stream) {
	defer stream.Close()

	// Read a full MessagePack‑encoded message.
	dataBytes, err := readMsgpackMsg(stream)
	if err != nil {
		writeErrorResponse(stream, http.StatusBadRequest, err)
		return
	}

	var req Request
	if err := msgpack.Unmarshal(dataBytes, &req); err != nil {
		writeErrorResponse(stream, http.StatusBadRequest, err)
		return
	}

	if req.Method == "" {
		writeErrorResponse(stream, http.StatusBadRequest,
			errors.New("missing method field"))
		return
	}

	r.handlersMu.RLock()
	handler, ok := r.handlers[req.Method]
	r.handlersMu.RUnlock()
	if !ok {
		writeErrorResponse(
			stream,
			http.StatusNotFound,
			errors.New("method not found: "+req.Method),
		)
		return
	}

	resp, err := handler(req)
	if err != nil {
		writeErrorResponse(stream, http.StatusInternalServerError, err)
		return
	}

	respBytes, err := msgpack.Marshal(resp)
	if err != nil {
		// Should not happen; use a fallback error response.
		writeErrorResponse(stream, http.StatusInternalServerError, err)
		return
	}

	_ = writeMsgpackMsg(stream, respBytes)
}

// writeErrorResponse writes an error response to the provided stream.
// It wraps the error (assumed to be converted to a SerializableError by WrapError)
// and writes it as the Data field of a Response.
func writeErrorResponse(stream *smux.Stream, status int, err error) {
	serErr := WrapError(err) // Assume WrapError produces a SerializableError.
	errorData := map[string]string{
		"error_type": serErr.ErrorType,
		"message":    serErr.Message,
	}
	if serErr.Op != "" {
		errorData["op"] = serErr.Op
	}
	if serErr.Path != "" {
		errorData["path"] = serErr.Path
	}

	resp := Response{
		Status: status,
		Data:   mustMarshalMsgpack(errorData),
	}
	respBytes, _ := msgpack.Marshal(resp)
	_ = writeMsgpackMsg(stream, respBytes)
}

// mustMarshalMsgpack is a helper that marshals v using msgpack.
// On error it returns an empty object.
func mustMarshalMsgpack(v interface{}) []byte {
	b, err := msgpack.Marshal(v)
	if err != nil {
		return []byte("{}")
	}
	return b
}

// ----------
// Session and RPC Call Helpers
// ----------

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

// CallContext performs an RPC call over a new stream.
// It builds the request with MessagePack, writes it to the stream using our framing protocol,
// reads the complete framed response, and unmarshals it into a Response struct.
// If a stream cannot be opened and auto‑reconnect is enabled, it reconnects.
func (s *Session) CallContext(ctx context.Context, method string,
	payload interface{}) (*Response, error) {

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

	// Build the MessagePack‑encoded request.
	reqBytes, err := buildRequestMsgpack(method, payload, nil)
	if err != nil {
		return nil, err
	}

	if err := writeMsgpackMsg(stream, reqBytes); err != nil {
		return nil, err
	}

	respBytes, err := readMsgpackMsg(stream)
	if err != nil {
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			return nil, context.DeadlineExceeded
		}
		return nil, err
	}
	if len(respBytes) == 0 {
		return nil, fmt.Errorf("empty response")
	}

	var resp Response
	if err := msgpack.Unmarshal(respBytes, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// CallMsg performs an RPC call using CallContext.
// If the response status is OK, it unmarshals the returned Data into v.
// Otherwise, it decodes the SerializableError from Data.
func (s *Session) CallMsg(ctx context.Context, method string,
	payload interface{}, v interface{}) error {
	resp, err := s.CallContext(ctx, method, payload)
	if err != nil {
		return err
	}
	if resp.Status != http.StatusOK {
		if resp.Data != nil {
			var serErr SerializableError
			if err := msgpack.Unmarshal(resp.Data, &serErr); err != nil {
				return fmt.Errorf("RPC error: %s (status %d)",
					resp.Message, resp.Status)
			}
			return UnwrapError(&serErr)
		}
		return fmt.Errorf("RPC error: %s (status %d)",
			resp.Message, resp.Status)
	}

	if resp.Data == nil {
		return nil
	}
	return msgpack.Unmarshal(resp.Data, v)
}

// CallMsgWithBuffer performs an RPC call in which the server first sends a
// metadata response (using MessagePack) that contains the available binary data length
// and an EOF flag, and then the binary payload is sent over the stream.
// It avoids extra re‑serialization overhead.
// If the metadata indicates an error (status != http.StatusOK), it decodes a SerializableError.
func (s *Session) CallMsgWithBuffer(ctx context.Context, method string,
	payload interface{}, buffer []byte) (int, bool, error) {
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

	// Build and send the request with an extra header indicating direct-buffer transfer.
	reqBytes, err := buildRequestMsgpack(method, payload,
		map[string]string{"X-Direct-Buffer": "true"})
	if err != nil {
		return 0, false, err
	}
	if err := writeMsgpackMsg(stream, reqBytes); err != nil {
		return 0, false, err
	}

	// --- Read metadata response ---
	metaBytes, err := readMsgpackMsg(stream)
	if err != nil {
		return 0, false, err
	}

	// Unmarshal metadata response.
	var meta struct {
		Status  int    `msgpack:"status"`
		Message string `msgpack:"message,omitempty"`
		Data    *struct {
			BytesAvailable int  `msgpack:"bytes_available"`
			EOF            bool `msgpack:"eof"`
		} `msgpack:"data"`
	}
	if err := msgpack.Unmarshal(metaBytes, &meta); err != nil {
		return 0, false, err
	}

	if meta.Status != http.StatusOK {
		var serErr SerializableError
		if err := msgpack.Unmarshal(metaBytes, &serErr); err == nil {
			return 0, false, UnwrapError(&serErr)
		}
		return 0, false, fmt.Errorf("RPC error: status %d", meta.Status)
	}

	contentLength := 0
	isEOF := false
	if meta.Data != nil {
		contentLength = meta.Data.BytesAvailable
		isEOF = meta.Data.EOF
	}
	if contentLength <= 0 {
		return 0, isEOF, nil
	}

	bytesRead := 0
	reader := bufio.NewReader(stream)
	// Read until we've read contentLength bytes or filled the buffer.
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
func DialWithBackoff(ctx context.Context, dialFunc func() (net.Conn, error),
	upgradeFn func(conn net.Conn) (*Session, error), initial, max time.Duration) (*Session, error) {
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
func HijackUpgradeHTTP(w http.ResponseWriter, r *http.Request,
	config *smux.Config) (*Session, error) {
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
func UpgradeHTTPClient(conn net.Conn, requestPath, host string,
	headers http.Header, config *smux.Config) (*Session, error) {

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
