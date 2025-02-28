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

	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/tinylib/msgp/msgp"
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

// Optimized serialization using msgp codegen
func marshalWithPool(v msgp.Marshaler) (*PooledMsg, error) {
	// Get a buffer from the pool.
	buf := msgpackBufferPool.Get().([]byte)
	// MarshalMsg appends to the provided slice.
	b, err := v.MarshalMsg(buf[:0])
	if err != nil {
		// Return the buffer to the pool on error.
		msgpackBufferPool.Put(buf)
		return nil, err
	}
	return &PooledMsg{
		Data:   b,
		pooled: true,
	}, nil
}

// --------------------------------------------------------
// MessagePack framing helper functions
// --------------------------------------------------------

// readMsgpMsgPooled reads a MessagePack‑encoded message from r using our framing protocol.
// It uses a 4‑byte big‑endian length header followed by that many bytes. For messages up to
// 4096 bytes it attempts to use a pooled buffer (avoiding an extra copy in hot paths).
// The caller is responsible for calling Release() on the returned *PooledMsg if pm.pooled is true.
func readMsgpMsgPooled(r io.Reader) (*PooledMsg, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	msgLen := binary.BigEndian.Uint32(lenBuf[:])

	const maxMessageSize = 10 * 1024 * 1024
	if msgLen > maxMessageSize {
		return nil, fmt.Errorf("message too large: %d bytes", msgLen)
	}

	if msgLen <= 4096 {
		buf := msgpackBufferPool.Get().([]byte)
		if cap(buf) < int(msgLen) {
			buf = make([]byte, msgLen)
		}
		msg := buf[:msgLen]
		if _, err := io.ReadFull(r, msg); err != nil {
			msgpackBufferPool.Put(buf)
			return nil, err
		}
		return &PooledMsg{Data: msg, pooled: true}, nil
	}

	msg := make([]byte, msgLen)
	_, err := io.ReadFull(r, msg)
	return &PooledMsg{Data: msg, pooled: false}, err
}

// For non–critical paths we still expose the simpler API that returns a []byte copy.
func readMsgpMsg(r io.Reader) ([]byte, error) {
	pm, err := readMsgpMsgPooled(r)
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

// writeMsgpMsg writes msg to w with a 4‑byte length header. We combine the header and msg
// into one write using net.Buffers so that we only incur one syscall when possible.
func writeMsgpMsg(w io.Writer, msg []byte) error {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(msg)))
	if bw, ok := w.(*bufio.Writer); ok {
		_, err := bw.Write(lenBuf[:])
		if err != nil {
			return err
		}
		_, err = bw.Write(msg)
		if err != nil {
			return err
		}
		return bw.Flush()
	}
	nb := net.Buffers{lenBuf[:], msg}
	_, err := nb.WriteTo(w)
	return err
}

// --------------------------------------------------------
// Router and stream handling
// --------------------------------------------------------

// HandlerFunc handles an RPC Request and returns a Response.
type HandlerFunc func(req Request) (*Response, error)

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

	pm, err := readMsgpMsgPooled(stream)
	if err != nil {
		writeErrorResponse(stream, http.StatusBadRequest, err)
		return
	}
	defer pm.Release()

	var req Request
	if _, err := req.UnmarshalMsg(pm.Data); err != nil {
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

	respBytes, err := marshalWithPool(resp)
	if err != nil {
		writeErrorResponse(stream, http.StatusInternalServerError, err)
		return
	}
	defer respBytes.Release()

	_ = writeMsgpMsg(stream, respBytes.Data)

	// Handle raw binary streaming if requested (status 213)
	if resp.Status == 213 && resp.RawStream != nil {
		// Call the raw stream handler
		resp.RawStream(stream)
	}
}

// writeErrorResponse sends an error response over the stream.
func writeErrorResponse(stream *smux.Stream, status int, err error) {
	serErr := WrapError(err)
	errBytes, mErr := marshalWithPool(serErr)
	if mErr != nil && syslog.L != nil {
		syslog.L.Errorf("[writeErrorResponse] %s", mErr.Error())
	}

	var respData []byte
	if errBytes != nil {
		respData = errBytes.Data
		defer errBytes.Release()
	}

	// Set the error message so that the client can fall back to it,
	// if msgpack decoding fails
	resp := Response{
		Status:  status,
		Message: serErr.Message,
		Data:    respData,
	}

	respBytes, mErr := marshalWithPool(&resp)
	if mErr != nil && syslog.L != nil {
		syslog.L.Errorf("[writeErrorResponse] %s", mErr.Error())
	}
	var respBytesData []byte
	if respBytes != nil {
		respBytesData = respBytes.Data
		defer respBytes.Release()
	}
	wErr := writeMsgpMsg(stream, respBytesData)
	if wErr != nil && syslog.L != nil {
		syslog.L.Errorf("[writeErrorResponse] %s", wErr.Error())
	}
}

// --------------------------------------------------------
// Session and RPC Call Helpers (using atomic session pointer)
// --------------------------------------------------------

// ReconnectConfig holds the parameters for automatic reconnection.
type ReconnectConfig struct {
	AutoReconnect     bool
	DialFunc          func() (net.Conn, error)
	UpgradeFunc       func(net.Conn) (*Session, error)
	InitialBackoff    time.Duration
	MaxBackoff        time.Duration
	MaxRetries        int // Added max retries field
	ReconnectCtx      context.Context
	reconnectAttempts int32 // Track current attempts
}

// Session wraps an underlying smux.Session. In order to avoid lock contention,
// we store the current *smux.Session in an atomic.Value.
type Session struct {
	// muxSess holds a *smux.Session.
	muxSess atomic.Value

	// (Reconnect configuration is set rarely.)
	reconnectConfig *ReconnectConfig
	reconnectMu     sync.Mutex

	// Add a flag to track if reconnection is in progress
	reconnecting int32
}

// NewServerSession creates a new Session for a server connection.
func NewServerSession(conn net.Conn, config *smux.Config) (*Session, error) {
	config.Version = 2
	config.KeepAliveInterval = 30 * time.Second
	config.KeepAliveTimeout = 90 * time.Second
	config.MaxFrameSize = 262144
	config.MaxReceiveBuffer = 8388608
	config.MaxStreamBuffer = 1048576

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
	config.Version = 2
	config.KeepAliveInterval = 30 * time.Second
	config.KeepAliveTimeout = 90 * time.Second
	config.MaxFrameSize = 262144
	config.MaxReceiveBuffer = 8388608
	config.MaxStreamBuffer = 1048576

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
	// Set default values if not provided
	if rc.MaxRetries <= 0 {
		rc.MaxRetries = 10 // Default to 10 max retries
	}
	if rc.InitialBackoff <= 0 {
		rc.InitialBackoff = 100 * time.Millisecond
	}
	if rc.MaxBackoff <= 0 {
		rc.MaxBackoff = 30 * time.Second
	}
	s.reconnectConfig = rc
}

// attemptReconnect tries to reconnect and update the underlying session.
// This method uses its own mutex so that only one reconnect is attempted at a time.
func (s *Session) attemptReconnect() error {
	// Use CAS to ensure only one goroutine attempts reconnection at a time
	if !atomic.CompareAndSwapInt32(&s.reconnecting, 0, 1) {
		// Another goroutine is already trying to reconnect
		// Wait for it to finish and check if we have a valid session now
		for atomic.LoadInt32(&s.reconnecting) == 1 {
			time.Sleep(10 * time.Millisecond)
		}

		// Check if reconnection was successful
		curSess := s.muxSess.Load().(*smux.Session)
		if curSess != nil && !curSess.IsClosed() {
			return nil
		}
		return errors.New("reconnection already attempted and failed")
	}

	// Make sure to reset the reconnecting flag when done
	defer atomic.StoreInt32(&s.reconnecting, 0)

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

	// Reset the attempt counter on success
	atomic.StoreInt32(&s.reconnectConfig.reconnectAttempts, 0)

	newSess.reconnectConfig = s.reconnectConfig
	s.muxSess.Store(newSess.muxSess.Load())
	return nil
}

// Call initiates a request/response conversation on a new stream.
func (s *Session) Call(method string, payload []byte) (*Response, error) {
	return s.CallContext(context.Background(), method, payload)
}

// CallContext performs an RPC call over a new stream.
// It applies any context deadlines to the smux stream.
func (s *Session) CallContext(ctx context.Context, method string, payload []byte) (*Response, error) {

	// Use the atomic pointer to avoid holding a lock while reading.
	curSession := s.muxSess.Load().(*smux.Session)

	// Open a new stream.
	stream, err := openStreamWithReconnect(s, curSession)
	if err != nil {
		return nil, err
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
	defer reqBytes.Release()
	if err := writeMsgpMsg(stream, reqBytes.Data); err != nil {
		return nil, err
	}

	respBytes, err := readMsgpMsg(stream)
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
	if _, err := resp.UnmarshalMsg(respBytes); err != nil {
		return nil, err
	}
	return &resp, nil
}

// CallMsg performs an RPC call and unmarshals its Data into v on success,
// or decodes the error from Data if status != http.StatusOK.
func (s *Session) CallMsg(ctx context.Context, method string, payload []byte) ([]byte, error) {
	resp, err := s.CallContext(ctx, method, payload)
	if err != nil {
		return nil, err
	}
	if resp.Status != http.StatusOK {
		if resp.Data != nil {
			var serErr SerializableError
			if _, err := serErr.UnmarshalMsg(resp.Data); err != nil {
				return nil, fmt.Errorf("RPC error: %s (status %d)", resp.Message, resp.Status)
			}
			return nil, UnwrapError(&serErr)
		}
		return nil, fmt.Errorf("RPC error: %s (status %d)", resp.Message, resp.Status)
	}

	if resp.Data == nil {
		return nil, nil
	}
	return resp.Data, nil
}

// CallMsgWithBuffer performs an RPC call for file I/O-style operations in which the server
// first sends metadata about a binary transfer and then writes the payload directly.
func (s *Session) CallMsgWithBuffer(ctx context.Context, method string, payload []byte, buffer []byte) (int, bool, error) {
	curSession := s.muxSess.Load().(*smux.Session)

	// Single stream opening attempt with potential reconnect
	stream, err := openStreamWithReconnect(s, curSession)
	if err != nil {
		return 0, false, err
	}
	defer stream.Close()

	// Set deadlines if provided in context
	if deadline, ok := ctx.Deadline(); ok {
		// Apply both deadlines with a single syscall where possible
		_ = stream.SetDeadline(deadline)
	}

	// Build the request with direct buffer header
	reqBytes, err := buildRequestMsgpack(method, payload, map[string]string{"X-Direct-Buffer": "true"})
	if err != nil {
		return 0, false, err
	}
	defer reqBytes.Release()

	// Write request
	if err := writeMsgpMsg(stream, reqBytes.Data); err != nil {
		return 0, false, err
	}

	// Read metadata response
	metaBytes, err := readMsgpMsg(stream)
	if err != nil {
		return 0, false, err
	}

	// Handle non-OK status quickly
	var resp Response
	if _, err := resp.UnmarshalMsg(metaBytes); err != nil {
		return 0, false, err
	}
	if resp.Status != 213 {
		var serErr SerializableError
		if _, err := serErr.UnmarshalMsg(metaBytes); err == nil {
			return 0, false, UnwrapError(&serErr)
		}
		return 0, false, fmt.Errorf("RPC error: status %d", resp.Status)
	}

	// Parse buffer metadata
	var meta BufferMetadata
	if _, err := meta.UnmarshalMsg(resp.Data); err != nil {
		return 0, false, err
	}

	// Early return for empty content
	if meta.BytesAvailable <= 0 {
		return 0, meta.EOF, nil
	}

	// Calculate how much we can read into the provided buffer
	bytesToRead := min(meta.BytesAvailable, len(buffer))

	// Read directly into the provided buffer
	bytesRead, err := io.ReadFull(stream, buffer[:bytesToRead])
	if err == io.ErrUnexpectedEOF {
		// Partial read is still valid data
		return bytesRead, meta.EOF, nil
	} else if err != nil {
		if ctx.Err() != nil {
			return bytesRead, false, ctx.Err() // Context timeout/cancellation takes precedence
		}
		return bytesRead, false, err
	}

	return bytesRead, meta.EOF, nil
}

// Helper function to open stream with reconnect logic
func openStreamWithReconnect(s *Session, curSession *smux.Session) (*smux.Stream, error) {
	stream, err := curSession.OpenStream()
	if err == nil {
		return stream, nil
	}

	// Only attempt reconnect if configured
	rc := s.reconnectConfig
	if rc == nil || !rc.AutoReconnect {
		return nil, err
	}

	// Try reconnection
	if err := s.attemptReconnect(); err != nil {
		return nil, err
	}

	// Get fresh session after reconnect
	curSession = s.muxSess.Load().(*smux.Session)
	return curSession.OpenStream()
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
func DialWithBackoff(ctx context.Context, dialFunc func() (net.Conn, error), upgradeFn func(conn net.Conn) (*Session, error), initial, max time.Duration) (*Session, error) {
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

// UpgradeHTTPClient helps a client upgrade an HTTP connection.
func UpgradeHTTPClient(conn net.Conn, requestPath, host string, headers http.Header, config *smux.Config) (*Session, error) {
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
