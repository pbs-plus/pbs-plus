package arpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/xtaci/smux"
)

type StreamPipe struct {
	mu     sync.RWMutex
	tun    *smux.Session
	conn   net.Conn
	router *Router

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
)

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

	if err := readHandshakeResponse(stream); err != nil {
		pipe.Close()
		return nil, err
	}

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
		ctx:          ctx,
		cancelFunc:   cancel,
		tun:          tun,
		conn:         conn,
		serverAddr:   serverAddr,
		tlsConfig:    tlsConfig,
		isServerSide: isServerSide,
	}

	go func() {
		select {
		case <-tun.CloseChan():
			cancel()
		case <-ctx.Done():
		}
	}()

	return pipe, nil
}

func (s *StreamPipe) CloseChan() <-chan struct{} {
	return s.tun.CloseChan()
}

func (s *StreamPipe) OpenStream() (*smux.Stream, error) {
	if s.tun == nil {
		return nil, fmt.Errorf("nil smux tunnel")
	}
	return s.tun.OpenStream()
}

func (s *StreamPipe) Serve() error {
	if s.tun == nil {
		return fmt.Errorf("nil smux tunnel")
	}

	for {
		select {
		case <-s.ctx.Done():

			syslog.L.Debug().WithMessage("closing pipe due to context cancellation").Write()
			return s.ctx.Err()
		default:
		}

		stream, err := s.tun.AcceptStream()
		if err != nil {
			if s.tun.IsClosed() {
				syslog.L.Debug().WithMessage("closing pipe due to closed tun").Write()
				return fmt.Errorf("session closed: %w", err)
			}
			return err
		}

		s.mu.RLock()
		router := s.GetRouter()
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
				stream.Close()
			}()
			router.serveStream(st)
		}(stream)
	}
}

func (s *StreamPipe) Close() {
	s.cancelFunc()

	if s.tun != nil && !s.tun.IsClosed() {
		syslog.L.Debug().WithMessage("closing tunnel due to pipe close").Write()
		_ = s.tun.Close()
	}

	if s.conn != nil {
		syslog.L.Debug().WithMessage("closing conn due to pipe close").Write()
		_ = s.conn.Close()
	}
}

func (s *StreamPipe) GetState() ConnectionState {
	if s.tun == nil || s.tun.IsClosed() {
		return StateDisconnected
	}
	if s.ctx.Err() != nil {
		return StateDisconnected
	}
	return StateConnected
}
