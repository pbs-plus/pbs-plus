package arpc

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/utils"
	"github.com/xtaci/smux"
)

type Session struct {
	muxSess atomic.Pointer[smux.Session]
	router  atomic.Pointer[Router]

	reconnectConfig ReconnectConfig
	reconnectMu     sync.Mutex

	state atomic.Int32

	circuitOpen    atomic.Bool
	circuitResetAt atomic.Int64
	reconnectChan  chan struct{}

	ctx        context.Context
	cancelFunc context.CancelFunc

	version string
}

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
		reconnectConfig: ReconnectConfig{},
		reconnectChan:   make(chan struct{}, 1),
		ctx:             ctx,
		cancelFunc:      cancel,
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
		reconnectConfig: ReconnectConfig{},
		reconnectChan:   make(chan struct{}, 1),
		ctx:             ctx,
		cancelFunc:      cancel,
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

		rc := s.reconnectConfig

		stream, err := curSession.AcceptStream()
		if err != nil {
			s.state.Store(int32(StateDisconnected))
			if rc.AutoReconnect {
				if err2 := s.attemptReconnect(); err2 != nil {
					return err2
				}
				continue
			}
			return err
		}
		router := s.GetRouter()
		if router == nil {
			return fmt.Errorf("router is nil")
		}
		go func() {
			defer func() {
				if r := recover(); r != nil {
					stream.Close()
				}
			}()
			router.ServeStream(stream)
		}()
	}
}

func ConnectToServer(ctx context.Context, autoReconnect bool, serverAddr string, headers http.Header, tlsConfig *tls.Config) (*Session, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	dialFunc := func() (net.Conn, error) {
		return tls.Dial("tcp", serverAddr, tlsConfig)
	}

	upgradeFunc := func(conn net.Conn) (*Session, error) {
		return upgradeHTTPClient(conn, "/plus/arpc", serverAddr, headers, nil)
	}

	var session *Session
	var err error
	if autoReconnect {
		session, err = dialWithBackoff(
			ctx,
			dialFunc,
			upgradeFunc,
			100*time.Millisecond,
			30*time.Second,
		)
	} else {
		conn, err := dialWithProbe(ctx, dialFunc)
		if err != nil {
			return nil, errors.New("server not reachable")
		}

		session, err = upgradeFunc(conn)
		if err != nil {
			conn.Close()
		}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %w", err)
	}

	if autoReconnect {
		session.EnableAutoReconnect(ReconnectConfig{
			AutoReconnect:    true,
			DialFunc:         dialFunc,
			UpgradeFunc:      upgradeFunc,
			InitialBackoff:   100 * time.Millisecond,
			MaxBackoff:       30 * time.Second,
			BackoffJitter:    0.2,
			CircuitBreakTime: 60 * time.Second,
			ReconnectCtx:     ctx,
		})
	}

	return session, nil
}

func (s *Session) Close() error {
	s.cancelFunc()

	sess := s.muxSess.Load()
	if sess != nil {
		return sess.Close()
	}
	return nil
}
