package arpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/hashicorp/yamux"
)

// ConnectionState represents the current connection state.
type ConnectionState int32

const (
	StateConnected ConnectionState = iota
	StateDisconnected
	StateReconnecting
	StateFailed
)

// ReconnectConfig holds the parameters for automatic reconnection.
type ReconnectConfig struct {
	AutoReconnect    bool
	DialFunc         func() (net.Conn, error)
	UpgradeFunc      func(net.Conn) (*Session, error)
	InitialBackoff   time.Duration
	MaxBackoff       time.Duration
	ReconnectCtx     context.Context
	BackoffJitter    float64
	CircuitBreakTime time.Duration
}

// dialResult is used by dialWithProbe to deliver dialing results.
type dialResult struct {
	conn net.Conn
	err  error
}

// dialWithProbe wraps the DialFunc into a context-aware call.
func dialWithProbe(ctx context.Context, dialFunc func() (net.Conn, error)) (net.Conn, error) {
	resultCh := make(chan dialResult, 1)
	go func() {
		conn, err := dialFunc()
		select {
		case resultCh <- dialResult{conn, err}:
		default:
			if conn != nil {
				_ = conn.Close()
			}
		}
		close(resultCh)
	}()

	select {
	case res := <-resultCh:
		return res.conn, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// dialWithBackoff attempts to establish a connection with exponential backoff and jitter.
func dialWithBackoff(
	ctx context.Context,
	dialFunc func() (net.Conn, error),
	upgradeFunc func(net.Conn) (*Session, error),
	initialBackoff time.Duration,
	maxBackoff time.Duration,
) (*Session, error) {
	if initialBackoff <= 0 {
		initialBackoff = 100 * time.Millisecond
	}
	if maxBackoff <= 0 {
		maxBackoff = 30 * time.Second
	}

	jitterFactor := 0.2
	backoff := initialBackoff
	timer := time.NewTimer(0)
	defer timer.Stop()

	var attempt int
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if attempt > 0 {
			jitteredBackoff := getJitteredBackoff(backoff, jitterFactor)
			timer.Reset(jitteredBackoff)
			select {
			case <-timer.C:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		attempt++

		conn, err := dialFunc()
		if err != nil {
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		session, err := upgradeFunc(conn)
		if err != nil {
			_ = conn.Close()
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		return session, nil
	}
}

// openStreamWithReconnect opens a stream with reconnection support.
func openStreamWithReconnect(s *Session, curSession *yamux.Session) (*yamux.Stream, error) {
	stream, err := curSession.OpenStream()
	if err == nil {
		return stream, nil
	}

	if !s.reconnectConfig.AutoReconnect {
		s.Close()
		return nil, err
	}

	if ConnectionState(s.state.Load()) == StateConnected {
		s.state.Store(int32(StateDisconnected))
	}

	if s.circuitOpen.Load() {
		resetTime := s.circuitResetAt.Load()
		if resetTime > 0 && time.Now().Unix() < resetTime {
			s.Close()
			return nil, errors.New("connection failed and circuit breaker is open")
		}
		s.circuitOpen.Store(false)
	}

	probeCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	conn, probeErr := dialWithProbe(probeCtx, s.reconnectConfig.DialFunc)
	if probeErr != nil {
		s.circuitOpen.Store(true)
		s.circuitResetAt.Store(time.Now().Add(5 * time.Second).Unix())

		s.Close()
		return nil, errors.New("server not reachable")
	}
	_ = conn.Close()

	timeout := getJitteredBackoff(5*time.Second, 0.3)
	if ConnectionState(s.state.Load()) == StateReconnecting {
		select {
		case <-s.reconnectChan:
		case <-time.After(timeout):
			s.Close()
			return nil, errors.New("timeout waiting for reconnection")
		case <-s.ctx.Done():
			s.Close()
			return nil, s.ctx.Err()
		}
	} else {
		go s.attemptReconnect()
		select {
		case <-s.reconnectChan:
		case <-time.After(timeout):
			s.Close()
			return nil, errors.New("timeout waiting for reconnection")
		case <-s.ctx.Done():
			s.Close()
			return nil, s.ctx.Err()
		}
	}

	if ConnectionState(s.state.Load()) != StateConnected {
		s.Close()
		return nil, errors.New("failed to reconnect")
	}

	newSession := s.muxSess.Load()
	return newSession.OpenStream()
}

// EnableAutoReconnect configures auto-reconnection and starts the connection monitor.
func (s *Session) EnableAutoReconnect(rc ReconnectConfig) {
	if rc.InitialBackoff <= 0 {
		rc.InitialBackoff = 100 * time.Millisecond
	}
	if rc.MaxBackoff <= 0 {
		rc.MaxBackoff = 30 * time.Second
	}
	if rc.BackoffJitter <= 0 {
		rc.BackoffJitter = 0.2
	}
	if rc.CircuitBreakTime <= 0 {
		rc.CircuitBreakTime = 60 * time.Second
	}
	if rc.ReconnectCtx == nil {
		rc.ReconnectCtx = context.Background()
	}

	s.reconnectConfig = rc
	go s.connectionMonitor()
}

// connectionMonitor periodically checks if reconnection is needed.
func (s *Session) connectionMonitor() {
	if !s.reconnectConfig.AutoReconnect {
		return
	}

	timer := time.NewTimer(getJitteredBackoff(5*time.Second, 0.5))
	defer timer.Stop()
	const checkInterval = 5 * time.Second

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-timer.C:
			sess := s.muxSess.Load()
			if sess == nil || sess.IsClosed() {
				currentState := ConnectionState(s.state.Load())
				if currentState != StateReconnecting {
					if !s.circuitOpen.Load() {
						go s.attemptReconnect()
					} else {
						resetTime := s.circuitResetAt.Load()
						if resetTime > 0 && time.Now().Unix() > resetTime {
							s.circuitOpen.Store(false)
							go func() {
								probeCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
								defer cancel()
								conn, err := dialWithProbe(probeCtx, s.reconnectConfig.DialFunc)
								if err == nil && conn != nil {
									_ = conn.Close()
									s.attemptReconnect()
								}
							}()
						}
					}
				}
			}
			timer.Reset(getJitteredBackoff(checkInterval, 0.2))
		}
	}
}

// attemptReconnect tries to reconnect using exponential backoff with jitter.
func (s *Session) attemptReconnect() error {
	if !s.state.CompareAndSwap(int32(StateDisconnected), int32(StateReconnecting)) {
		currentState := ConnectionState(s.state.Load())
		if currentState == StateConnected {
			return nil
		}
		select {
		case <-s.reconnectChan:
			if ConnectionState(s.state.Load()) == StateConnected {
				return nil
			}
			return errors.New("reconnection failed")
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	}
	defer func() {
		select {
		case s.reconnectChan <- struct{}{}:
		default:
		}
	}()

	if !s.reconnectConfig.AutoReconnect {
		s.state.Store(int32(StateDisconnected))
		return fmt.Errorf("auto reconnect not configured")
	}

	probeCtx, cancel := context.WithTimeout(s.reconnectConfig.ReconnectCtx, 2*time.Second)
	defer cancel()
	conn, err := dialWithProbe(probeCtx, s.reconnectConfig.DialFunc)
	if err != nil {
		s.circuitOpen.Store(true)
		s.circuitResetAt.Store(time.Now().Add(5 * time.Second).Unix())
		s.state.Store(int32(StateDisconnected))
		return fmt.Errorf("server not reachable: %w", err)
	}
	_ = conn.Close()

	newSession, err := dialWithBackoff(
		s.reconnectConfig.ReconnectCtx,
		s.reconnectConfig.DialFunc,
		s.reconnectConfig.UpgradeFunc,
		s.reconnectConfig.InitialBackoff,
		s.reconnectConfig.MaxBackoff,
	)
	if err != nil {
		s.circuitOpen.Store(true)
		s.circuitResetAt.Store(time.Now().Add(s.reconnectConfig.CircuitBreakTime).Unix())
		s.state.Store(int32(StateFailed))
		return fmt.Errorf("reconnection failed: %w", err)
	}

	s.reconnectMu.Lock()
	s.muxSess.Store(newSession.muxSess.Load())
	s.reconnectMu.Unlock()
	s.state.Store(int32(StateConnected))
	return nil
}

// GetState returns the current connection state.
func (s *Session) GetState() ConnectionState {
	return ConnectionState(s.state.Load())
}
