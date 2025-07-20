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

	"github.com/hashicorp/yamux"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

// Session wraps an underlying yamux.Session with improved connection management.
type Session struct {
	// muxSess holds a *yamux.Session.
	muxSess atomic.Pointer[yamux.Session]
	router  atomic.Pointer[Router]

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

func (s *Session) SetRouter(router Router) {
	s.router.Store(&router) // Store a pointer to the value
}

func (s *Session) GetRouter() *Router {
	return s.router.Load()
}

func (s *Session) GetVersion() string {
	return s.version
}

// NewServerSession creates a new Session for a server connection.
func NewServerSession(conn net.Conn, config *yamux.Config) (*Session, error) {
	if config == nil {
		config = defaultYamuxConfig()
	}

	s, err := yamux.Server(conn, config)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	session := &Session{
		reconnectConfig: ReconnectConfig{},
		reconnectChan:   make(chan struct{}, 1), // Buffer of 1 to prevent blocking
		ctx:             ctx,
		cancelFunc:      cancel,
	}
	session.muxSess.Store(s)
	session.state.Store(int32(StateConnected))

	return session, nil
}

// NewClientSession creates a new Session for a client connection.
func NewClientSession(conn net.Conn, config *yamux.Config) (*Session, error) {
	if config == nil {
		config = defaultYamuxConfig()
	}

	s, err := yamux.Client(conn, config)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	session := &Session{
		reconnectConfig: ReconnectConfig{},
		reconnectChan:   make(chan struct{}, 1), // Buffer of 1 to prevent blocking
		ctx:             ctx,
		cancelFunc:      cancel,
	}
	session.muxSess.Store(s)
	session.state.Store(int32(StateConnected))

	return session, nil
}

// defaultYamuxConfig returns a default yamux configuration
func defaultYamuxConfig() *yamux.Config {
	return &yamux.Config{
		AcceptBacklog:          1024,             // Allow many concurrent streams
		EnableKeepAlive:        true,             // Detect dead peers
		KeepAliveInterval:      10 * time.Second, // More frequent keepalives
		ConnectionWriteTimeout: 30 * time.Second, // Allow for large writes
		MaxStreamWindowSize:    32 * 1024 * 1024, // 32 MiB window per stream
		StreamOpenTimeout:      60 * time.Second, // Allow time for slow opens
		StreamCloseTimeout:     2 * time.Minute,  // Allow time for slow closes
		Logger:                 &YamuxLoggerAdapter{Underlying: syslog.L},
	}
}

// If a stream accept fails and auto‑reconnect is enabled, it attempts to reconnect.
func (s *Session) Serve() error {
	for {
		curSession := s.muxSess.Load()
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
		if router != nil {
			go router.ServeStream(stream)
		}
	}
}

func ConnectToServer(ctx context.Context, autoReconnect bool, serverAddr string, headers http.Header, tlsConfig *tls.Config) (*Session, error) {
	dialFunc := func() (net.Conn, error) {
		return tls.Dial("tcp", serverAddr, tlsConfig)
	}

	upgradeFunc := func(conn net.Conn) (*Session, error) {
		return upgradeHTTPClient(conn, "/plus/arpc", serverAddr, headers, nil)
	}

	var session *Session
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
func (s *Session) Close() error {
	s.cancelFunc() // Stop the connection monitor

	sess := s.muxSess.Load()
	if sess != nil {
		return sess.Close()
	}
	return nil
}
