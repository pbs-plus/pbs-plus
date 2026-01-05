package arpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/xtaci/smux"
)

type StreamPipe struct {
	mu            sync.RWMutex
	tun           *smux.Session
	conn          net.Conn
	router        *Router
	generation    uint64
	state         ConnectionState
	reconnectOnce *sync.Once
	reconnecting  atomic.Bool

	serverAddr   string
	tlsConfig    *tls.Config
	headers      http.Header
	version      string
	isServerSide bool

	ctx        context.Context
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup
}

type ConnectionState int32

const (
	StateConnected ConnectionState = iota
	StateDisconnected
	StateReconnecting
	StateFailed
)

type sessionRef struct {
	session    *smux.Session
	generation uint64
}

func (s *StreamPipe) SetRouter(router Router) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.router = &router
}

func (s *StreamPipe) GetRouter() *Router {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.router
}

func (s *StreamPipe) GetVersion() string {
	return s.version
}

func (s *StreamPipe) getSession() *sessionRef {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.tun == nil || s.tun.IsClosed() {
		return nil
	}
	return &sessionRef{
		session:    s.tun,
		generation: s.generation,
	}
}

func (s *StreamPipe) OpenStream() (*smux.Stream, error) {
	ref := s.getSession()
	if ref != nil {
		stream, err := ref.session.OpenStream()
		if err == nil {
			return stream, nil
		}
		s.markDisconnected()
	}

	if s.isServerSide {
		return nil, fmt.Errorf("server-side pipe cannot reconnect")
	}

	if err := s.ensureConnected(); err != nil {
		return nil, err
	}

	ref = s.getSession()
	if ref == nil {
		return nil, fmt.Errorf("session unavailable after reconnection")
	}

	return ref.session.OpenStream()
}

func (s *StreamPipe) markDisconnected() {
	s.mu.Lock()
	if s.state == StateConnected {
		s.state = StateDisconnected
		if !s.isServerSide {
			s.reconnectOnce = &sync.Once{}
		}
	}
	s.mu.Unlock()
}

func (s *StreamPipe) ensureConnected() error {
	if s.isServerSide {
		return fmt.Errorf("server-side pipe cannot initiate reconnection")
	}

	s.mu.RLock()
	state := s.state
	once := s.reconnectOnce
	s.mu.RUnlock()

	if state == StateConnected {
		return nil
	}

	if state == StateFailed {
		return fmt.Errorf("connection failed")
	}

	errChan := make(chan error, 1)
	once.Do(func() {
		errChan <- s.reconnect()
	})

	select {
	case err := <-errChan:
		return err
	case <-time.After(10 * time.Second):
		return fmt.Errorf("timeout waiting for reconnection")
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *StreamPipe) reconnect() error {
	if s.isServerSide {
		return fmt.Errorf("server-side pipe cannot reconnect")
	}

	if !s.reconnecting.CompareAndSwap(false, true) {
		return fmt.Errorf("reconnection already in progress")
	}
	defer s.reconnecting.Store(false)

	s.mu.Lock()
	s.state = StateReconnecting
	s.mu.Unlock()

	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Minute)
	defer cancel()

	var conn net.Conn
	var err error
	backoff := 100 * time.Millisecond
	maxBackoff := 30 * time.Second

	for attempt := 0; attempt < 10; attempt++ {
		select {
		case <-ctx.Done():
			s.mu.Lock()
			s.state = StateFailed
			s.mu.Unlock()
			return ctx.Err()
		default:
		}

		if attempt > 0 {
			time.Sleep(backoff)
			backoff = min(backoff*2, maxBackoff)
		}

		conn, err = dialServer(s.serverAddr, s.tlsConfig)
		if err != nil {
			syslog.L.Error(err).WithField("attempt", attempt).WithField("stage", "dial").Write()
			continue
		}

		smuxC, err := smux.Client(conn, defaultConfig())
		if err != nil {
			conn.Close()
			syslog.L.Error(err).WithField("attempt", attempt).WithField("stage", "smux").Write()
			continue
		}

		stream, err := smuxC.OpenStream()
		if err != nil {
			smuxC.Close()
			conn.Close()
			syslog.L.Error(err).WithField("attempt", attempt).WithField("stage", "header-stream").Write()
			continue
		}

		if werr := writeHeadersFrame(stream, s.headers); werr != nil {
			stream.Close()
			smuxC.Close()
			conn.Close()
			syslog.L.Error(werr).WithField("attempt", attempt).WithField("stage", "write-headers").Write()
			continue
		}
		stream.Close()

		s.mu.Lock()
		s.closeOldConnectionLocked()
		s.tun = smuxC
		s.conn = conn
		s.generation++
		s.state = StateConnected
		s.reconnectOnce = &sync.Once{}
		s.mu.Unlock()

		syslog.L.Info().WithMessage("reconnection successful").Write()
		return nil
	}

	s.mu.Lock()
	s.state = StateFailed
	s.mu.Unlock()

	return fmt.Errorf("reconnection failed after retries: %w", err)
}

func (s *StreamPipe) closeOldConnectionLocked() {
	if s.tun != nil {
		if !s.tun.IsClosed() {
			s.tun.Close()
		}
		s.tun = nil
	}
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}
}

func (s *StreamPipe) connectionMonitor() {
	defer s.wg.Done()

	if s.isServerSide {
		return
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			ref := s.getSession()
			if ref == nil {
				s.mu.RLock()
				state := s.state
				s.mu.RUnlock()

				if state != StateReconnecting && state != StateFailed {
					s.markDisconnected()
					go func() {
						if err := s.ensureConnected(); err != nil {
							syslog.L.Error(err).WithMessage("background reconnection failed").Write()
						}
					}()
				}
			}
		}
	}
}

func ConnectToServer(ctx context.Context, serverAddr string, headers http.Header, tlsConfig *tls.Config) (*StreamPipe, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if tlsConfig == nil || len(tlsConfig.Certificates) == 0 {
		return nil, fmt.Errorf("TLS configuration must include client certificate")
	}

	arpcTls := tlsConfig.Clone()
	arpcTls.NextProtos = []string{"pbsarpc"}

	conn, err := dialServer(serverAddr, arpcTls)
	if err != nil {
		syslog.L.Info().WithField("NextProtos", arpcTls.NextProtos).WithField("serverAddr", serverAddr).Write()
		return nil, fmt.Errorf("server not reachable (%s): %w", serverAddr, err)
	}

	smuxC, err := smux.Client(conn, defaultConfig())
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to create smux client: %w", err)
	}

	pipe, err := newStreamPipe(ctx, smuxC, conn, serverAddr, arpcTls, false)
	if err != nil {
		syslog.L.Debug().WithMessage("closing tun and conn due to stream pipe err init").Write()
		_ = smuxC.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("failed to create pipe: %w", err)
	}

	if headers == nil {
		headers = make(http.Header)
	}

	headers.Add("ARPCVersion", "2")
	pipe.headers = headers
	pipe.version = "2"

	stream, err := pipe.OpenStream()
	if err != nil {
		pipe.Close()
		return nil, fmt.Errorf("failed to initialize header stream: %w", err)
	}
	defer stream.Close()

	if werr := writeHeadersFrame(stream, headers); werr != nil {
		pipe.Close()
		return nil, fmt.Errorf("failed to write headers: %w", werr)
	}

	pipe.wg.Add(1)
	go pipe.connectionMonitor()

	return pipe, nil
}

func AcceptConnection(ctx context.Context, tun *smux.Session, conn net.Conn) (*StreamPipe, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	pipe, err := newStreamPipe(ctx, tun, conn, "", nil, true)
	if err != nil {
		return nil, fmt.Errorf("failed to create server pipe: %w", err)
	}

	return pipe, nil
}

func newStreamPipe(ctx context.Context, tun *smux.Session, conn net.Conn, serverAddr string, tlsConfig *tls.Config, isServerSide bool) (*StreamPipe, error) {
	if tun == nil {
		return nil, fmt.Errorf("nil smux tunnel")
	}

	ctx, cancel := context.WithCancel(ctx)

	pipe := &StreamPipe{
		ctx:           ctx,
		cancelFunc:    cancel,
		tun:           tun,
		conn:          conn,
		serverAddr:    serverAddr,
		tlsConfig:     tlsConfig,
		state:         StateConnected,
		generation:    1,
		reconnectOnce: &sync.Once{},
		isServerSide:  isServerSide,
	}

	go func() {
		select {
		case <-tun.CloseChan():
			pipe.markDisconnected()
		case <-ctx.Done():
		}
	}()

	return pipe, nil
}

func (s *StreamPipe) Serve() error {
	for {
		select {
		case <-s.ctx.Done():
			syslog.L.Debug().WithMessage("closing pipe due to context cancellation").Write()
			return s.ctx.Err()
		default:
		}

		ref := s.getSession()
		if ref == nil {
			if s.isServerSide {
				return fmt.Errorf("server-side session closed")
			}

			if err := s.ensureConnected(); err != nil {
				return fmt.Errorf("session unavailable: %w", err)
			}
			ref = s.getSession()
			if ref == nil {
				return fmt.Errorf("session unavailable after reconnection")
			}
		}

		stream, err := ref.session.AcceptStream()
		if err != nil {
			if ref.session.IsClosed() {
				s.markDisconnected()
				if s.isServerSide {
					return fmt.Errorf("session closed: %w", err)
				}
				continue
			}
			return err
		}

		s.mu.RLock()
		router := s.router
		s.mu.RUnlock()

		if router == nil {
			syslog.L.Debug().WithMessage("closing stream due to invalid router").Write()
			_ = stream.Close()
			continue
		}

		go func(st *smux.Stream) {
			defer func() {
				if rec := recover(); rec != nil {
					syslog.L.Debug().
						WithField("panic", fmt.Sprintf("%v", rec)).
						WithMessage("recovered from panic in handler").
						Write()
				}
				st.Close()
			}()
			router.serveStream(st)
		}(stream)
	}
}

func (s *StreamPipe) Close() {
	s.cancelFunc()
	s.wg.Wait()

	s.mu.Lock()
	defer s.mu.Unlock()

	s.closeOldConnectionLocked()
	s.state = StateDisconnected
}

func (s *StreamPipe) GetState() ConnectionState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}
