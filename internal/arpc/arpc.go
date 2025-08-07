package arpc

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/utils"
	"github.com/xtaci/smux"
)

// Session wraps an underlying smux.Session with improved connection management.
type Session struct {
	// muxSess holds a *smux.Session.
	muxSess atomic.Pointer[smux.Session]
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

	// Worker pool for stream processing
	streamChan  chan *smux.Stream
	workerCount int
	workersWg   sync.WaitGroup
	done        chan struct{}
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

// calculateWorkerCount determines optimal number of workers based on CPU count
func calculateWorkerCount() int {
	cpuCount := runtime.NumCPU()

	// Scale based on CPU count with reasonable bounds
	switch {
	case cpuCount <= 2:
		return 2 // Minimum 2 workers for responsiveness
	case cpuCount <= 4:
		return cpuCount
	case cpuCount <= 8:
		return cpuCount + 1 // Slight oversubscription for I/O bound work
	default:
		return cpuCount + 2 // More oversubscription for high-core systems
	}
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
	workerCount := calculateWorkerCount()

	session := &Session{
		reconnectConfig: ReconnectConfig{},
		reconnectChan:   make(chan struct{}, 1),
		ctx:             ctx,
		cancelFunc:      cancel,
		streamChan:      make(chan *smux.Stream, workerCount*10), // Buffer based on worker count
		workerCount:     workerCount,
		done:            make(chan struct{}),
	}
	session.muxSess.Store(s)
	session.state.Store(int32(StateConnected))

	// Start worker pool
	session.startWorkerPool()

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
	workerCount := calculateWorkerCount()

	session := &Session{
		reconnectConfig: ReconnectConfig{},
		reconnectChan:   make(chan struct{}, 1),
		ctx:             ctx,
		cancelFunc:      cancel,
		streamChan:      make(chan *smux.Stream, workerCount*10), // Buffer based on worker count
		workerCount:     workerCount,
		done:            make(chan struct{}),
	}
	session.muxSess.Store(s)
	session.state.Store(int32(StateConnected))

	// Start worker pool
	session.startWorkerPool()

	return session, nil
}

// startWorkerPool starts the configured number of worker goroutines
func (s *Session) startWorkerPool() {
	s.workersWg.Add(s.workerCount)

	for i := 0; i < s.workerCount; i++ {
		go s.streamWorker()
	}

	// Start a goroutine to wait for all workers to finish
	go func() {
		s.workersWg.Wait()
		close(s.done)
	}()
}

// streamWorker is a worker goroutine that processes streams
func (s *Session) streamWorker() {
	defer s.workersWg.Done()

	for {
		select {
		case <-s.ctx.Done():
			return
		case stream := <-s.streamChan:
			if stream == nil {
				return // Channel closed
			}
			s.handleStream(stream)
		}
	}
}

// handleStream processes a single stream
func (s *Session) handleStream(stream *smux.Stream) {
	router := s.GetRouter()
	if router == nil {
		stream.Close()
		return
	}

	// Use the existing router logic
	router.ServeStream(stream)
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

// Serve distributes streams across the worker pool
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

		// Distribute stream to worker pool
		select {
		case s.streamChan <- stream:
			// Stream queued successfully
		case <-s.ctx.Done():
			stream.Close()
			return s.ctx.Err()
		default:
			// Channel full - block until space is available
			select {
			case s.streamChan <- stream:
			case <-s.ctx.Done():
				stream.Close()
				return s.ctx.Err()
			}
		}
	}
}

func (s *Session) GetWorkerCount() int {
	return s.workerCount
}

func (s *Session) GetStreamQueueLength() int {
	return len(s.streamChan)
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
			_ = conn.Close()
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

// Close closes the session and stops all workers
func (s *Session) Close() error {
	s.cancelFunc() // Signal shutdown to all workers

	// Close the stream channel to stop workers
	close(s.streamChan)

	// Wait for all workers to finish
	<-s.done

	sess := s.muxSess.Load()
	if sess != nil {
		return sess.Close()
	}
	return nil
}
