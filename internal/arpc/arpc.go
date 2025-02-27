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
	"sync/atomic"
	"time"

	"github.com/vmihailenco/msgpack/v5"
	"github.com/xtaci/smux"
)

// --------------------------------------------------------
// Buffer pooling and a PooledMsg type for zero‑copy reads.
// --------------------------------------------------------

// We use a sync.Pool to return buffers for small MessagePack messages.
var msgpackBufferPool = sync.Pool{
	New: func() interface{} {
		// Start with a reasonable size for most requests.
		return make([]byte, 4096)
	},
}

// PooledMsg wraps a []byte that may come from the pool. If
// Pooled is true then the caller must call Release() once done.
type PooledMsg struct {
	Data   []byte
	pooled bool
}

// Release returns the underlying buffer to the pool if it was pooled.
func (pm *PooledMsg) Release() {
	if pm.pooled {
		// Reset length to full capacity
		msgpackBufferPool.Put(pm.Data[:cap(pm.Data)])
		pm.pooled = false
	}
}

// --------------------------------------------------------
// MessagePack framing helper functions
// --------------------------------------------------------

// readMsgpackMsgPooled reads a MessagePack‑encoded message from r using our framing protocol.
// It uses a 4‑byte big‑endian length header followed by that many bytes. For messages up to
// 4096 bytes it attempts to use a pooled buffer (avoiding an extra copy in hot paths).
// The caller is responsible for calling Release() on the returned *PooledMsg if pm.pooled is true.
func readMsgpackMsgPooled(r io.Reader) (*PooledMsg, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	msgLen := binary.BigEndian.Uint32(lenBuf[:])

	// Size limit to avoid abuse.
	const maxMessageSize = 10 * 1024 * 1024 // 10 MB
	if msgLen > maxMessageSize {
		return nil, fmt.Errorf("message too large: %d bytes", msgLen)
	}

	if msgLen <= 4096 {
		buf := msgpackBufferPool.Get().([]byte)
		// Ensure we have a buffer that is large enough.
		if cap(buf) < int(msgLen) {
			buf = make([]byte, msgLen)
		}
		// Use just the first msgLen bytes.
		msg := buf[:msgLen]
		if _, err := io.ReadFull(r, msg); err != nil {
			msgpackBufferPool.Put(buf)
			return nil, err
		}
		return &PooledMsg{Data: msg, pooled: true}, nil
	}

	// For larger messages we simply allocate the needed amount.
	msg := make([]byte, msgLen)
	_, err := io.ReadFull(r, msg)
	return &PooledMsg{Data: msg, pooled: false}, err
}

// For non–critical paths we still expose the simpler API that returns a []byte copy.
func readMsgpackMsg(r io.Reader) ([]byte, error) {
	pm, err := readMsgpackMsgPooled(r)
	if err != nil {
		return nil, err
	}
	// In the non‐pooled API we immediately copy the payload so that we can release the pooled buffer.
	data := make([]byte, len(pm.Data))
	copy(data, pm.Data)
	if pm.pooled {
		pm.Release()
	}
	return data, nil
}

// writeMsgpackMsg writes msg to w with a 4‑byte length header. We combine the header and msg
// into one write using net.Buffers so that we only incur one syscall when possible.
func writeMsgpackMsg(w io.Writer, msg []byte) error {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(msg)))
	nb := net.Buffers{lenBuf[:], msg}
	_, err := nb.WriteTo(w)
	return err
}

// --------------------------------------------------------
// Data structures for the RPC protocol
// --------------------------------------------------------

// Request defines the MessagePack‑encoded request format.
type Request struct {
	Method  string      `msgpack:"method"`
	Payload []byte      `msgpack:"payload"`
	Headers http.Header `msgpack:"headers,omitempty"`
}

// Response defines the MessagePack‑encoded response format.
type Response struct {
	Status  int    `msgpack:"status"`
	Message string `msgpack:"message,omitempty"`
	Data    []byte `msgpack:"data,omitempty"`
}

// --------------------------------------------------------
// Router and stream handling
// --------------------------------------------------------

// HandlerFunc handles an RPC Request and returns a Response.
type HandlerFunc func(req Request) (Response, error)

// Router holds a map from method names to handler functions.
type Router struct {
	handlers   map[string]HandlerFunc
	handlersMu sync.RWMutex
}

// NewRouter creates a new Router instance.
func NewRouter() *Router {
	return &Router{handlers: make(map[string]HandlerFunc)}
}

// Handle registers a handler for a given method name.
func (r *Router) Handle(method string, handler HandlerFunc) {
	r.handlersMu.Lock()
	defer r.handlersMu.Unlock()
	r.handlers[method] = handler
}

// CloseHandle removes a handler.
func (r *Router) CloseHandle(method string) {
	r.handlersMu.Lock()
	defer r.handlersMu.Unlock()
	delete(r.handlers, method)
}

// ServeStream reads a single RPC request from the stream, routes it to the correct handler,
// and writes back the Response. In case of errors an error response is sent.
func (r *Router) ServeStream(stream *smux.Stream) {
	defer stream.Close()

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
			fmt.Errorf("method not found: %s", req.Method),
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
		writeErrorResponse(stream, http.StatusInternalServerError, err)
		return
	}

	_ = writeMsgpackMsg(stream, respBytes)
}

// writeErrorResponse sends an error response over the stream.
func writeErrorResponse(stream *smux.Stream, status int, err error) {
	serErr := WrapError(err)
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

func mustMarshalMsgpack(v interface{}) []byte {
	b, err := msgpack.Marshal(v)
	if err != nil {
		return []byte("{}")
	}
	return b
}

// --------------------------------------------------------
// Session and RPC Call Helpers (using atomic session pointer)
// --------------------------------------------------------

// ReconnectConfig holds the parameters for automatic reconnection.
type ReconnectConfig struct {
	AutoReconnect  bool
	DialFunc       func() (net.Conn, error)
	UpgradeFunc    func(net.Conn) (*Session, error)
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	ReconnectCtx   context.Context
}

// Session wraps an underlying smux.Session. In order to avoid lock contention,
// // we store the current *smux.Session in an atomic.Value.
type Session struct {
	// muxSess holds a *smux.Session.
	muxSess atomic.Value

	// (Reconnect configuration is set rarely.)
	reconnectConfig *ReconnectConfig
	reconnectMu     sync.Mutex
}

// NewServerSession creates a new Session for a server connection.
func NewServerSession(conn net.Conn, config *smux.Config) (*Session, error) {
	s, err := smux.Server(conn, config)
	if err != nil {
		return nil, err
	}
	session := &Session{reconnectConfig: nil}
	session.muxSess.Store(s)
	return session, nil
}

// NewClientSession creates a new Session for a client connection.
func NewClientSession(conn net.Conn, config *smux.Config) (*Session, error) {
	s, err := smux.Client(conn, config)
	if err != nil {
		return nil, err
	}
	session := &Session{reconnectConfig: nil}
	session.muxSess.Store(s)
	return session, nil
}

// EnableAutoReconnect sets up the reconnection parameters.
func (s *Session) EnableAutoReconnect(rc *ReconnectConfig) {
	s.reconnectConfig = rc
}

// attemptReconnect tries to reconnect and update the underlying session.
// This method uses its own mutex so that only one reconnect is attempted at a time.
func (s *Session) attemptReconnect() error {
	s.reconnectMu.Lock()
	defer s.reconnectMu.Unlock()

	curSess := s.muxSess.Load().(*smux.Session)
	if curSess != nil && !curSess.IsClosed() {
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
	s.muxSess.Store(newSess.muxSess.Load())
	return nil
}

// Call initiates a request/response conversation on a new stream.
func (s *Session) Call(method string, payload interface{}) (*Response, error) {
	return s.CallContext(context.Background(), method, payload)
}

// CallContext performs an RPC call over a new stream.
// It applies any context deadlines to the smux stream.
func (s *Session) CallContext(ctx context.Context, method string,
	payload interface{}) (*Response, error) {

	// Use the atomic pointer to avoid holding a lock while reading.
	curSession := s.muxSess.Load().(*smux.Session)
	rc := s.reconnectConfig

	// Open a new stream.
	stream, err := curSession.OpenStream()
	if err != nil {
		if rc != nil && rc.AutoReconnect {
			if err2 := s.attemptReconnect(); err2 != nil {
				return nil, err2
			}
			curSession = s.muxSess.Load().(*smux.Session)
			stream, err = curSession.OpenStream()
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	defer stream.Close()

	// Propagate context deadlines to the stream.
	if deadline, ok := ctx.Deadline(); ok {
		stream.SetWriteDeadline(deadline)
		stream.SetReadDeadline(deadline)
	}

	// Build and send the RPC request.
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

// CallMsg performs an RPC call and unmarshals its Data into v on success,
// or decodes the error from Data if status != http.StatusOK.
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

// CallMsgWithBuffer performs an RPC call for file I/O-style operations in which the server
// first sends metadata about a binary transfer and then writes the payload directly.
func (s *Session) CallMsgWithBuffer(ctx context.Context, method string,
	payload interface{}, buffer []byte) (int, bool, error) {
	curSession := s.muxSess.Load().(*smux.Session)
	rc := s.reconnectConfig

	stream, err := curSession.OpenStream()
	if err != nil {
		if rc != nil && rc.AutoReconnect {
			if err2 := s.attemptReconnect(); err2 != nil {
				return 0, false, err2
			}
			curSession = s.muxSess.Load().(*smux.Session)
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

	// Build the request with an extra header requesting direct buffer transfer.
	reqBytes, err := buildRequestMsgpack(method, payload,
		map[string]string{"X-Direct-Buffer": "true"})
	if err != nil {
		return 0, false, err
	}
	if err := writeMsgpackMsg(stream, reqBytes); err != nil {
		return 0, false, err
	}

	// Read the metadata response.
	metaBytes, err := readMsgpackMsg(stream)
	if err != nil {
		return 0, false, err
	}

	// Unmarshal the metadata.
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

// Serve continuously accepts streams on the session and dispatches them via router.
// If a stream accept fails and auto‑reconnect is enabled, we attempt reconnect.
func (s *Session) Serve(router *Router) error {
	for {
		curSession := s.muxSess.Load().(*smux.Session)
		rc := s.reconnectConfig

		stream, err := curSession.AcceptStream()
		if err != nil {
			if rc != nil && rc.AutoReconnect {
				if err2 := s.attemptReconnect(); err2 != nil {
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
	curSession := s.muxSess.Load().(*smux.Session)
	return curSession.Close()
}

// DialWithBackoff repeatedly attempts to establish a connection.
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

// HijackUpgradeHTTP helps a server upgrade an HTTP connection.
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

// UpgradeHTTPClient helps a client upgrade an HTTP connection.
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
		"", "",
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
