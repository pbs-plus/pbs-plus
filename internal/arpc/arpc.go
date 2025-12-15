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
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/quicvarint"
)

type StreamPipe struct {
	*quic.Conn
	sync.Mutex

	serverAddr string
	tlsConfig  *tls.Config
	headers    http.Header
	router     atomic.Pointer[Router]

	state atomic.Int32

	ctx        context.Context
	cancelFunc context.CancelFunc

	version string
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

func NewStreamPipe(conn *quic.Conn) (*StreamPipe, error) {
	if conn == nil {
		return nil, fmt.Errorf("nil quic connection")
	}
	ctx, cancel := context.WithCancel(context.Background())
	pipe := &StreamPipe{
		Conn:       conn,
		ctx:        ctx,
		cancelFunc: cancel,
	}
	return pipe, nil
}

func (s *StreamPipe) Serve() error {
	for {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		default:
		}
		stream, err := s.AcceptStream(s.ctx)
		if err != nil {
			return err
		}
		router := s.GetRouter()
		if router == nil {
			stream.CancelWrite(quicErrServeNoRouter)
			_ = stream.Close()
			return fmt.Errorf("router is nil")
		}
		go func() {
			defer func() {
				if r := recover(); r != nil {
					stream.CancelWrite(quicErrServePanic)
					_ = stream.Close()
				}
			}()
			router.ServeStream(stream)
		}()
	}
}

func dialServer(serverAddr string, tlsConfig *tls.Config) (*quic.Conn, error) {
	if tlsConfig == nil {
		return nil, fmt.Errorf("missing tls config")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	quicConfig := quicServerLimitsAutoConfig()
	quicConfig.KeepAlivePeriod = time.Second * 5
	quicConfig.MaxIdleTimeout = time.Second * 20
	quicConfig.MaxIncomingStreams = quicvarint.Max

	conn, err := quic.DialAddr(ctx, serverAddr, tlsConfig, quicConfig)
	if err != nil {
		return nil, fmt.Errorf("QUIC dial failed: %w", err)
	}

	state := conn.ConnectionState().TLS
	syslog.L.Info().
		WithField("tls_version", state.Version).
		WithField("alpn", state.NegotiatedProtocol).
		WithMessage("QUIC connection established").
		Write()
	return conn, nil
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
		return nil, fmt.Errorf("server not reachable: %w", err)
	}

	pipe, err := NewStreamPipe(conn)
	if err != nil {
		_ = conn.CloseWithError(quicErrInitPipeFailed, "init failed")
		return nil, fmt.Errorf("failed to create pipe: %w", err)
	}

	if headers == nil {
		headers = make(http.Header)
	}

	headers.Add("ARPCVersion", "2")

	pipe.tlsConfig = arpcTls
	pipe.serverAddr = serverAddr
	pipe.headers = headers

	ustream, uerr := pipe.OpenUniStream()
	if uerr != nil {
		_ = pipe.CloseWithError(quicErrHeadersInitFailed, "failed to initialize header pipe")
		return nil, fmt.Errorf("failed to initialize header pipe: %w", uerr)
	}
	if werr := writeHeadersFrame(ustream, headers); werr != nil {
		_ = ustream.Close()
		_ = pipe.CloseWithError(quicErrHeadersWriteFailed, "failed to write headers")
		return nil, fmt.Errorf("failed to write headers: %w", werr)
	}
	if cerr := ustream.Close(); cerr != nil {
		_ = pipe.CloseWithError(quicErrHeadersCloseFailed, "failed to close header pipe")
		return nil, fmt.Errorf("failed to close header pipe: %w", cerr)
	}

	return pipe, nil
}

func (s *StreamPipe) Close() error {
	s.cancelFunc()
	return s.Conn.CloseWithError(quicErrClosePipe, "pipe closed")
}

func (s *StreamPipe) GetState() ConnectionState {
	if s.Conn == nil {
		return StateDisconnected
	}
	if s.Conn == nil || s.ConnectionState().TLS.HandshakeComplete == false && s.Context().Err() != nil {
		return StateDisconnected
	}
	if s.Context().Err() != nil {
		return StateDisconnected
	}
	return StateConnected
}

func (s *StreamPipe) openStream(_ context.Context) (*quic.Stream, error) {
	str, err := s.OpenStream()
	if err != nil {
		if s != nil && s.Conn != nil {
			_ = s.Conn.CloseWithError(quicErrOpenStreamFailed, "open stream failed")
		}
		return nil, err
	}
	return str, nil
}

func Listen(ctx context.Context, addr string, tlsConfig *tls.Config, quicConfig *quic.Config) (*quic.Listener, *net.UDPConn, error) {
	if tlsConfig == nil {
		return nil, nil, fmt.Errorf("missing tls config")
	}
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve addr failed: %w", err)
	}
	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("listen udp failed: %w", err)
	}

	arpcTls := tlsConfig.Clone()
	arpcTls.NextProtos = []string{"pbsarpc"}

	ql, err := quic.Listen(udpConn, arpcTls, quicConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("quic listen failed: %w", err)
	}

	return ql, udpConn, nil
}

func Serve(ctx context.Context, agentsManager *AgentsManager, ql *quic.Listener, router Router) error {
	for {
		conn, err := ql.Accept(ctx)
		if err != nil {
			return err
		}
		go func(c *quic.Conn) {
			if len(c.ConnectionState().TLS.PeerCertificates) == 0 {
				_ = c.CloseWithError(quicErrClientCertRequired, "client certificate required")
				return
			}

			var reqHeaders http.Header
			uh, uerr := c.AcceptUniStream(ctx)
			if uerr == nil {
				if hdrs, rerr := readHeadersFrame(uh); rerr == nil {
					reqHeaders = hdrs
				}
				uh.CancelRead(0)
			}

			pipe, id, err := agentsManager.CreateStreamPipe(c, reqHeaders)
			if err != nil {
				_ = c.CloseWithError(quicErrInitPipeFailed, fmt.Sprintf("init failed: %v", err))
				return
			}
			defer agentsManager.CloseStreamPipe(id)
			pipe.SetRouter(router)

			_ = pipe.Serve()
		}(conn)
	}
}

func ListenAndServe(ctx context.Context, addr string, agentsManager *AgentsManager, tlsConfig *tls.Config, router Router) error {
	quicConfig := quicServerLimitsAutoConfig()
	quicConfig.KeepAlivePeriod = time.Second * 5
	quicConfig.MaxIdleTimeout = time.Second * 20
	quicConfig.MaxIncomingStreams = quicvarint.Max

	ql, udpConn, err := Listen(ctx, addr, tlsConfig, quicConfig)
	if err != nil {
		return err
	}
	defer udpConn.Close()

	return Serve(ctx, agentsManager, ql, router)
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

	if s.Conn != nil {
		_ = s.Conn.CloseWithError(0, "reconnecting")
		s.Conn = nil
	}

	conn, err := dialServer(s.serverAddr, s.tlsConfig)
	if err != nil {
		return err
	}

	s.Conn = conn

	if s.headers == nil {
		s.headers = make(http.Header)
	}

	ustream, uerr := s.OpenUniStream()
	if uerr != nil {
		_ = s.CloseWithError(quicErrHeadersInitFailed, "failed to initialize header pipe")
		return fmt.Errorf("failed to initialize header pipe: %w", uerr)
	}
	if werr := writeHeadersFrame(ustream, s.headers); werr != nil {
		_ = ustream.Close()
		_ = s.CloseWithError(quicErrHeadersWriteFailed, "failed to write headers")
		return fmt.Errorf("failed to write headers: %w", werr)
	}
	if cerr := ustream.Close(); cerr != nil {
		_ = s.CloseWithError(quicErrHeadersCloseFailed, "failed to close header pipe")
		return fmt.Errorf("failed to close header pipe: %w", cerr)
	}

	return nil
}
