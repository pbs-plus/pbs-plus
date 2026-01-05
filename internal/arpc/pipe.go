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
	tun    atomic.Pointer[smux.Session]
	conn   atomic.Pointer[net.Conn]
	router atomic.Pointer[Router]

	serverAddr string
	tlsConfig  *tls.Config
	headers    http.Header

	state          atomic.Int32
	reconnectState atomic.Int32
	reconnectChan  chan struct{}
	reconnectMu    sync.Mutex

	ctx        context.Context
	cancelFunc context.CancelFunc

	version string
}

type ConnectionState int32

const (
	StateConnected ConnectionState = iota
	StateDisconnected
	StateReconnecting
	StateFailed
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

func (s *StreamPipe) OpenStream() (*smux.Stream, error) {
	tun := s.tun.Load()
	if tun != nil && !(*tun).IsClosed() {
		stream, err := (*tun).OpenStream()
		if err == nil {
			return stream, nil
		}
		// Stream open failed, trigger reconnection
		if s.state.Load() == int32(StateConnected) {
			s.state.Store(int32(StateDisconnected))
		}
	}

	// Wait for reconnection
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()

	select {
	case <-s.reconnectChan:
		tun := s.tun.Load()
		if tun == nil {
			return nil, fmt.Errorf("session is nil after reconnection")
		}
		if (*tun).IsClosed() {
			return nil, fmt.Errorf("session closed after reconnection")
		}
		return (*tun).OpenStream()
	case <-timeout.C:
		return nil, fmt.Errorf("timeout waiting for reconnection")
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func (s *StreamPipe) Reconnect(ctx context.Context) error {
	if !s.reconnectState.CompareAndSwap(int32(StateDisconnected), int32(StateReconnecting)) {
		currentState := ConnectionState(s.reconnectState.Load())
		if currentState == StateConnected {
			return nil
		}
		return fmt.Errorf("reconnection already in progress")
	}

	defer func() {
		select {
		case s.reconnectChan <- struct{}{}:
		default:
		}
	}()

	if ctx == nil {
		ctx = context.Background()
	}

	// Dial with backoff
	var conn net.Conn
	var err error
	backoff := 100 * time.Millisecond
	maxBackoff := 30 * time.Second

	for attempt := 0; attempt < 10; attempt++ {
		select {
		case <-ctx.Done():
			s.reconnectState.Store(int32(StateFailed))
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

		// Initialize the new connection
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

		// Success - replace the old connection
		s.reconnectMu.Lock()
		oldTun := s.tun.Load()
		if oldTun != nil && !(*oldTun).IsClosed() {
			(*oldTun).Close()
		}
		oldConn := s.conn.Load()
		if oldConn != nil {
			(*oldConn).Close()
		}
		s.tun.Store(smuxC)
		s.conn.Store(&conn)
		s.reconnectMu.Unlock()

		s.state.Store(int32(StateConnected))
		s.reconnectState.Store(int32(StateConnected))
		syslog.L.Info().WithMessage("reconnection successful").Write()
		return nil
	}

	s.reconnectState.Store(int32(StateFailed))
	return fmt.Errorf("reconnection failed after retries: %w", err)
}

func (s *StreamPipe) connectionMonitor() {
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-timer.C:
			tun := s.tun.Load()
			if tun == nil || (*tun).IsClosed() {
				if s.reconnectState.Load() != int32(StateReconnecting) {
					s.state.Store(int32(StateDisconnected))
					go func() {
						if err := s.Reconnect(s.ctx); err != nil {
							syslog.L.Error(err).WithMessage("reconnection failed").Write()
						}
					}()
				}
			}
			timer.Reset(5 * time.Second)
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

	pipe, err := newStreamPipe(ctx, smuxC, conn)
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

	pipe.tlsConfig = arpcTls
	pipe.serverAddr = serverAddr
	pipe.headers = headers

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

	// Start connection monitor
	go pipe.connectionMonitor()

	return pipe, nil
}

func newStreamPipe(ctx context.Context, tun *smux.Session, conn net.Conn) (*StreamPipe, error) {
	if tun == nil {
		return nil, fmt.Errorf("nil smux tunnel")
	}

	ctx, cancel := context.WithCancel(ctx)

	pipe := &StreamPipe{
		ctx:           ctx,
		cancelFunc:    cancel,
		reconnectChan: make(chan struct{}, 1),
	}
	pipe.tun.Store(tun)
	pipe.conn.Store(&conn)
	pipe.state.Store(int32(StateConnected))
	pipe.reconnectState.Store(int32(StateConnected))

	go func() {
		select {
		case <-tun.CloseChan():
			if pipe.state.Load() == int32(StateConnected) {
				pipe.state.Store(int32(StateDisconnected))
			}
		case <-ctx.Done():
		}
	}()

	return pipe, nil
}

func (s *StreamPipe) Serve() error {
	tun := s.tun.Load()
	if tun == nil {
		return fmt.Errorf("nil smux tunnel")
	}

	for {
		select {
		case <-s.ctx.Done():
			syslog.L.Debug().WithMessage("closing pipe due to context cancellation").Write()
			return s.ctx.Err()
		default:
		}

		currentTun := s.tun.Load()
		if currentTun == nil || (*currentTun).IsClosed() {
			return fmt.Errorf("session closed")
		}

		stream, err := (*currentTun).AcceptStream()
		if err != nil {
			if (*currentTun).IsClosed() {
				syslog.L.Debug().WithMessage("closing pipe due to closed tun").Write()
				return fmt.Errorf("session closed: %w", err)
			}
			return err
		}

		router := s.GetRouter()
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
				stream.Close()
			}()
			router.serveStream(st)
		}(stream)
	}
}

func (s *StreamPipe) Close() {
	s.cancelFunc()

	s.reconnectMu.Lock()
	defer s.reconnectMu.Unlock()

	tun := s.tun.Load()
	if tun != nil && !(*tun).IsClosed() {
		syslog.L.Debug().WithMessage("closing tunnel due to pipe close").Write()
		_ = (*tun).Close()
	}

	conn := s.conn.Load()
	if conn != nil {
		syslog.L.Debug().WithMessage("closing conn due to pipe close").Write()
		_ = (*conn).Close()
	}
}

func (s *StreamPipe) GetState() ConnectionState {
	tun := s.tun.Load()
	if tun == nil || (*tun).IsClosed() {
		return StateDisconnected
	}
	if s.ctx.Err() != nil {
		return StateDisconnected
	}
	return ConnectionState(s.state.Load())
}
