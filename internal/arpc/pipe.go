package arpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/xtaci/smux"
)

type StreamPipe struct {
	tun *smux.Session

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

func (s *StreamPipe) OpenStream() (*smux.Stream, error) {
	if s.tun != nil {
		return s.tun.OpenStream()
	}

	return nil, fmt.Errorf("nil smux tunnel")
}

func (s *StreamPipe) Reconnect(ctx context.Context) (*StreamPipe, error) {
	s.Close()
	return ConnectToServer(ctx, s.serverAddr, s.headers, s.tlsConfig)
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
	if werr := writeHeadersFrame(stream, headers); werr != nil {
		_ = stream.Close()
		pipe.Close()
		return nil, fmt.Errorf("failed to write headers: %w", werr)
	}
	if cerr := stream.Close(); cerr != nil {
		pipe.Close()
		return nil, fmt.Errorf("failed to close header stream: %w", cerr)
	}

	return pipe, nil
}

func newStreamPipe(ctx context.Context, tun *smux.Session, conn net.Conn) (*StreamPipe, error) {
	if tun == nil {
		return nil, fmt.Errorf("nil smux tunnel")
	}

	ctx, cancel := context.WithCancel(ctx)

	pipe := &StreamPipe{
		tun:        tun,
		conn:       conn,
		ctx:        ctx,
		cancelFunc: cancel,
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

func (s *StreamPipe) Serve() error {
	if s.tun == nil {
		return fmt.Errorf("nil smux tunnel")
	}

	for {
		stream, err := s.tun.AcceptStream()
		if err != nil {
			if s.tun.IsClosed() {
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
	s.cancelFunc()

	if s.tun != nil && !s.tun.IsClosed() {
		_ = s.tun.Close()
	}

	if s.conn != nil {
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
