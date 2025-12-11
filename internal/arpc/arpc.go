package arpc

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils"
	"github.com/xtaci/smux"
)

type Session struct {
	serverAddr string
	tlsConfig  *tls.Config
	headers    http.Header
	muxSess    atomic.Pointer[smux.Session]
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

func (s *Session) SetRouter(router Router) {
	s.router.Store(&router)
}

func (s *Session) GetRouter() *Router {
	return s.router.Load()
}

func (s *Session) GetVersion() string {
	return s.version
}

func NewServerSession(conn net.Conn, config *smux.Config) (*Session, error) {
	if config == nil {
		config = defaultSmuxConfig()
	}
	s, err := smux.Server(conn, config)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	session := &Session{
		ctx:        ctx,
		cancelFunc: cancel,
	}
	session.muxSess.Store(s)
	session.state.Store(int32(StateConnected))
	return session, nil
}

func NewClientSession(conn net.Conn, config *smux.Config) (*Session, error) {
	if config == nil {
		config = defaultSmuxConfig()
	}
	s, err := smux.Client(conn, config)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	session := &Session{
		ctx:        ctx,
		cancelFunc: cancel,
	}
	session.muxSess.Store(s)
	session.state.Store(int32(StateConnected))
	return session, nil
}

func defaultSmuxConfig() *smux.Config {
	defaults := smux.DefaultConfig()
	defaults.Version = 2
	defaults.MaxReceiveBuffer = utils.MaxReceiveBuffer
	defaults.MaxStreamBuffer = utils.MaxStreamBuffer
	defaults.MaxFrameSize = 65535
	return defaults
}

func (s *Session) Serve() error {
	for {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		default:
		}
		curSession := s.muxSess.Load()
		if curSession == nil {
			return errors.New("session is nil")
		}
		stream, err := curSession.AcceptStream()
		if err != nil {
			s.state.Store(int32(StateDisconnected))
			return err
		}
		router := s.GetRouter()
		if router == nil {
			return fmt.Errorf("router is nil")
		}
		go func() {
			defer func() {
				if r := recover(); r != nil {
					_ = stream.Close()
				}
			}()
			router.ServeStream(stream)
		}()
	}
}

func dialServer(serverAddr string, tlsConfig *tls.Config) (net.Conn, error) {
	conn, err := tls.Dial("tcp", serverAddr, tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("TLS dial failed: %w", err)
	}
	if err := conn.Handshake(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("TLS handshake failed: %w", err)
	}
	state := conn.ConnectionState()
	syslog.L.Info().
		WithField("tls_version", state.Version).
		WithField("cipher_suite", state.CipherSuite).
		WithField("peer_certs_count", len(state.PeerCertificates)).
		WithMessage("TLS connection established").
		Write()
	conn.SetDeadline(time.Time{})
	return conn, nil
}

func ConnectToServer(ctx context.Context, _ bool, serverAddr string, headers http.Header, tlsConfig *tls.Config) (*Session, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if tlsConfig == nil || len(tlsConfig.Certificates) == 0 {
		return nil, fmt.Errorf("TLS configuration must include client certificate")
	}

	conn, err := dialServer(serverAddr, tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("server not reachable: %w", err)
	}

	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	defer conn.SetDeadline(time.Time{})
	session, err := upgradeHTTPClient(conn, "/plus/arpc", serverAddr, headers, nil)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to connect to server: %w", err)
	}

	session.tlsConfig = tlsConfig
	session.serverAddr = serverAddr
	session.headers = headers

	return session, nil
}

func (s *Session) Reconnect() error {
	if s.tlsConfig == nil {
		return fmt.Errorf("reconnect: tls config missing")
	}

	if s.serverAddr == "" {
		return fmt.Errorf("reconnect: server address missing")
	}

	conn, err := dialServer(s.serverAddr, s.tlsConfig)
	if err != nil {
		return fmt.Errorf("server not reachable: %w", err)
	}

	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	defer conn.SetDeadline(time.Time{})
	session, err := upgradeHTTPClient(conn, "/plus/arpc", s.serverAddr, s.headers, nil)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("failed to connect to server: %w", err)
	}

	newMuxSess := session.muxSess.Load()
	s.muxSess.Store(newMuxSess)

	return nil
}

func (s *Session) Close() error {
	s.cancelFunc()
	sess := s.muxSess.Load()
	if sess != nil {
		return sess.Close()
	}
	return nil
}

func (s *Session) GetState() ConnectionState {
	if s == nil {
		return StateDisconnected
	}
	cur := s.muxSess.Load()
	if cur == nil || cur.IsClosed() {
		return StateDisconnected
	}
	return StateConnected
}

func (s *Session) openStream() (*smux.Stream, error) {
	cur := s.muxSess.Load()
	if cur == nil || cur.IsClosed() {
		return nil, fmt.Errorf("tls mux is closed")
	}

	return cur.OpenStream()
}
