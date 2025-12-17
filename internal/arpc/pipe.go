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
	newS, err := ConnectToServer(ctx, s.serverAddr, s.headers, s.tlsConfig)
	if err != nil {
		return s, err
	}

	r := s.GetRouter()
	if r != nil {
		newS.SetRouter(*r)
	}

	return newS, nil
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
