package arpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/xtaci/smux"
)

type StreamPipe struct {
	session *smux.Session // Changed from embedded to private field
	sync.RWMutex

	serverAddr string
	tlsConfig  *tls.Config
	headers    http.Header
	router     atomic.Pointer[Router]

	state atomic.Int32

	ctx        context.Context
	cancelFunc context.CancelFunc

	version string
	conn    net.Conn
}

type ConnectionState int32

const (
	StateConnected ConnectionState = iota
	StateDisconnected
)

func (s *StreamPipe) SetRouter(router Router) {
	s.router.Store(&router)
}

func (s *StreamPipe) GetRouter() *Router {
	return s.router.Load()
}

func (s *StreamPipe) GetVersion() string {
	return s.version
}

func (s *StreamPipe) AcceptStream() (*smux.Stream, error) {
	s.RLock()
	session := s.session
	ctx := s.ctx
	s.RUnlock()

	if session == nil {
		return nil, fmt.Errorf("session is nil")
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	return session.AcceptStream()
}

func (s *StreamPipe) OpenStream() (*smux.Stream, error) {
	s.RLock()
	session := s.session
	s.RUnlock()

	if session == nil {
		return nil, fmt.Errorf("session is nil")
	}
	return session.OpenStream()
}

func (s *StreamPipe) IsClosed() bool {
	s.RLock()
	session := s.session
	s.RUnlock()

	if session == nil {
		return true
	}
	return session.IsClosed()
}

func (s *StreamPipe) NumStreams() int {
	s.RLock()
	session := s.session
	s.RUnlock()

	if session == nil {
		return 0
	}
	return session.NumStreams()
}

func NewStreamPipe(session *smux.Session, conn net.Conn) (*StreamPipe, error) {
	if session == nil {
		return nil, fmt.Errorf("nil smux session")
	}

	ctx, cancel := context.WithCancel(context.Background())

	pipe := &StreamPipe{
		session:    session,
		conn:       conn,
		ctx:        ctx,
		cancelFunc: cancel,
	}

	go func() {
		select {
		case <-session.CloseChan():
			cancel()
		case <-ctx.Done():
		}
	}()

	return pipe, nil
}

func (s *StreamPipe) Serve() error {
	for {
		stream, err := s.AcceptStream()
		if err != nil {
			if s.IsClosed() {
				return fmt.Errorf("session closed: %w", err)
			}

			select {
			case <-s.ctx.Done():
				return s.ctx.Err()
			default:
			}

			continue
		}

		router := s.GetRouter()
		if router == nil {
			_ = stream.Close()
			continue
		}

		go func(st *smux.Stream) {
			defer func() {
				if r := recover(); r != nil {
					_ = st.Close()
				}
			}()
			router.serveStream(st)
		}(stream)
	}
}

func (s *StreamPipe) Close() {
	s.Lock()
	defer s.Unlock()

	s.cancelFunc()

	if s.session != nil && !s.session.IsClosed() {
		_ = s.session.Close()
	}

	if s.conn != nil {
		_ = s.conn.Close()
	}

	s.session = nil
	s.conn = nil
}

func (s *StreamPipe) GetState() ConnectionState {
	s.RLock()
	defer s.RUnlock()

	if s.session == nil || s.session.IsClosed() {
		return StateDisconnected
	}
	if s.ctx.Err() != nil {
		return StateDisconnected
	}
	return StateConnected
}

func (s *StreamPipe) Reconnect(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	s.Lock()
	defer s.Unlock()

	if s.tlsConfig == nil || s.serverAddr == "" {
		return fmt.Errorf("missing server address or TLS config for reconnect")
	}

	s.cancelFunc()

	if s.session != nil && !s.session.IsClosed() {
		_ = s.session.Close()
	}
	if s.conn != nil {
		_ = s.conn.Close()
	}

	conn, err := dialServer(s.serverAddr, s.tlsConfig)
	if err != nil {
		s.session = nil
		s.conn = nil
		return err
	}

	smuxConfig := smux.DefaultConfig()
	session, err := smux.Client(conn, smuxConfig)
	if err != nil {
		_ = conn.Close()
		s.session = nil
		s.conn = nil
		return fmt.Errorf("failed to create smux client: %w", err)
	}

	newCtx, cancel := context.WithCancel(context.Background())

	s.session = session
	s.conn = conn
	s.ctx = newCtx
	s.cancelFunc = cancel

	go func() {
		select {
		case <-session.CloseChan():
			cancel()
		case <-newCtx.Done():
		}
	}()

	if s.headers == nil {
		s.headers = make(http.Header)
	}

	stream, err := session.OpenStream()
	if err != nil {
		s.cancelFunc()
		_ = s.session.Close()
		_ = s.conn.Close()
		s.session = nil
		s.conn = nil
		return fmt.Errorf("failed to initialize header stream: %w", err)
	}

	if werr := writeHeadersFrame(stream, s.headers); werr != nil {
		_ = stream.Close()
		s.cancelFunc()
		_ = s.session.Close()
		_ = s.conn.Close()
		s.session = nil
		s.conn = nil
		return fmt.Errorf("failed to write headers: %w", werr)
	}

	if cerr := stream.Close(); cerr != nil {
		s.cancelFunc()
		_ = s.session.Close()
		_ = s.conn.Close()
		s.session = nil
		s.conn = nil
		return fmt.Errorf("failed to close header stream: %w", cerr)
	}

	return nil
}
