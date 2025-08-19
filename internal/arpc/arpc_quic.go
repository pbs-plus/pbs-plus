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

// QUICSession wraps an underlying smux.QUICSession with improved connection management.
type QUICSession struct {
	router atomic.Pointer[Router]

	// Connection state management
	reconnectConfig ReconnectConfig
	reconnectMu     sync.Mutex

	// Connection state tracking
	state atomic.Int32 // Stores ConnectionState

	// Circuit breaker and notification
	circuitOpen    atomic.Bool
	circuitResetAt atomic.Int64
	reconnectChan  chan struct{} // Notifies waiters when reconnection completes

	// Context for coordinating shutdown
	ctx        context.Context
	cancelFunc context.CancelFunc

	version string
}

func (s *QUICSession) SetRouter(router Router) {
	s.router.Store(&router) // Store a pointer to the value
}

func (s *QUICSession) GetRouter() *Router {
	return s.router.Load()
}

func (s *QUICSession) GetVersion() string {
	return s.version
}

// NewServerQUICSession creates a new QUICSession for a server connection.
func NewServerQUICSession(conn net.Conn, config *smux.Config) (*QUICSession, error) {
	if config == nil {
		config = defaultSmuxConfig()
	}

	s, err := smux.Server(conn, config)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	session := &QUICSession{
		reconnectConfig: ReconnectConfig{},
		reconnectChan:   make(chan struct{}, 1), // Buffer of 1 to prevent blocking
		ctx:             ctx,
		cancelFunc:      cancel,
	}
	session.muxSess.Store(s)
	session.state.Store(int32(StateConnected))

	return session, nil
}

// NewClientQUICSession creates a new QUICSession for a client connection.
func NewClientQUICSession(conn net.Conn, config *smux.Config) (*QUICSession, error) {
	if config == nil {
		config = defaultSmuxConfig()
	}

	s, err := smux.Client(conn, config)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	session := &QUICSession{
		reconnectConfig: ReconnectConfig{},
		reconnectChan:   make(chan struct{}, 1), // Buffer of 1 to prevent blocking
		ctx:             ctx,
		cancelFunc:      cancel,
	}
	session.muxSess.Store(s)
	session.state.Store(int32(StateConnected))

	return session, nil
}

// defaultSmuxConfig returns a default smux configuration
func defaultSmuxConfig() *smux.Config {
	defaults := smux.DefaultConfig()
	defaults.Version = 2
	defaults.MaxReceiveBuffer = utils.MaxReceiveBuffer
	defaults.MaxStreamBuffer = utils.MaxStreamBuffer
	defaults.MaxFrameSize = 65535

	return defaults
}

// If a stream accept fails and auto‑reconnect is enabled, it attempts to reconnect.
func (s *QUICSession) Serve() error {
	for {
		curQUICSession := s.muxSess.Load()
		rc := s.reconnectConfig

		stream, err := curQUICSession.AcceptStream()
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
		if router != nil {
			go router.ServeStream(stream)
		}
	}
}

func ConnectToQUICServer(ctx context.Context, autoReconnect bool, serverAddr string, headers http.Header, tlsConfig *tls.Config) (*QUICSession, error) {
	dialFunc := func() (net.Conn, error) {
		return tls.Dial("tcp", serverAddr, tlsConfig)
	}

	upgradeFunc := func(conn net.Conn) (*QUICSession, error) {
		return upgradeHTTPClient(conn, "/plus/arpc", serverAddr, headers, nil)
	}

	var session *QUICSession
	var err error
	if autoReconnect {
		// Use DialWithBackoff for the initial connection
		session, err = dialWithBackoff(
			ctx,
			dialFunc,
			upgradeFunc,
			100*time.Millisecond, // Initial backoff
			30*time.Second,       // Max backoff
		)
	} else {
		conn, err := dialWithProbe(ctx, dialFunc)
		if err != nil {
			return nil, errors.New("server not reachable")
		}

		session, err = upgradeFunc(conn)
		if err != nil {
			_ = conn.Close()
		}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %w", err)
	}

	if autoReconnect {
		// Configure auto-reconnect with the same parameters
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

// Close closes the session and stops the reconnection monitor
func (s *QUICSession) Close() error {
	s.cancelFunc() // Stop the connection monitor

	sess := s.muxSess.Load()
	if sess != nil {
		return sess.Close()
	}
	return nil
}
