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

	"github.com/xtaci/smux"
)

// Session wraps an underlying smux.Session with improved connection management.
type Session struct {
	// muxSess holds a *smux.Session.
	muxSess atomic.Value

	// Connection state management
	reconnectConfig *ReconnectConfig
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
}

// NewServerSession creates a new Session for a server connection.
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
		reconnectConfig: nil,
		reconnectChan:   make(chan struct{}, 1), // Buffer of 1 to prevent blocking
		ctx:             ctx,
		cancelFunc:      cancel,
	}
	session.muxSess.Store(s)
	session.state.Store(int32(StateConnected))

	return session, nil
}

// NewClientSession creates a new Session for a client connection.
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
		reconnectConfig: nil,
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
	return defaults
}

// If a stream accept fails and auto‑reconnect is enabled, it attempts to reconnect.
func (s *Session) Serve(router *Router) error {
	for {
		curSession := s.muxSess.Load().(*smux.Session)
		rc := s.reconnectConfig

		stream, err := curSession.AcceptStream()
		if err != nil {
			if rc != nil && rc.AutoReconnect {
				if err2 := s.attemptReconnect(); err2 != nil {
					return err2
				}
				continue
			}
			return err
		}
		go router.ServeStream(stream)
	}
}

func ConnectToServer(ctx context.Context, serverAddr string, headers http.Header, tlsConfig *tls.Config) (*Session, error) {
	dialFunc := func() (net.Conn, error) {
		return tls.Dial("tcp", serverAddr, tlsConfig)
	}

	upgradeFunc := func(conn net.Conn) (*Session, error) {
		return UpgradeHTTPClient(conn, "/plus/arpc", serverAddr, headers, nil)
	}

	// Use DialWithBackoff for the initial connection
	session, err := dialWithBackoff(
		ctx,
		dialFunc,
		upgradeFunc,
		100*time.Millisecond, // Initial backoff
		30*time.Second,       // Max backoff
	)

	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %w", err)
	}

	// Configure auto-reconnect with the same parameters
	session.EnableAutoReconnect(&ReconnectConfig{
		AutoReconnect:    true,
		DialFunc:         dialFunc,
		UpgradeFunc:      upgradeFunc,
		InitialBackoff:   100 * time.Millisecond,
		MaxBackoff:       30 * time.Second,
		MaxRetries:       10,
		BackoffJitter:    0.2,
		CircuitBreakTime: 60 * time.Second,
		ReconnectCtx:     ctx,
	})

	return session, nil
}

// Close closes the session and stops the reconnection monitor
func (s *Session) Close() error {
	s.cancelFunc() // Stop the connection monitor

	sess := s.muxSess.Load().(*smux.Session)
	if sess != nil {
		return sess.Close()
	}
	return nil
}

