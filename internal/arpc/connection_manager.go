package arpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/xtaci/smux"
)

type ConnectionState int32

const (
	StateConnected ConnectionState = iota
	StateDisconnected
	StateReconnecting
	StateFailed
)

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

type dialResult struct {
	conn net.Conn
	err  error
}

func dialWithProbe(ctx context.Context, dialFunc func() (net.Conn, error)) (net.Conn, error) {
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	if dialFunc == nil {
		return nil, errors.New("dialFunc is nil")
	}

	resultCh := make(chan dialResult, 1)
	go func() {
		defer close(resultCh)
		conn, err := dialFunc()
		select {
		case resultCh <- dialResult{conn, err}:
		case <-ctx.Done():
			if conn != nil {
				conn.Close()
			}
		}
	}()

	select {
	case res := <-resultCh:
		return res.conn, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func dialWithBackoff(
	ctx context.Context,
	dialFunc func() (net.Conn, error),
	upgradeFunc func(net.Conn) (*Session, error),
	initialBackoff time.Duration,
	maxBackoff time.Duration,
) (*Session, error) {
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	if dialFunc == nil {
		return nil, errors.New("dialFunc is nil")
	}
	if upgradeFunc == nil {
		return nil, errors.New("upgradeFunc is nil")
	}

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
			syslog.L.Error(err).WithField("attempt", attempt).WithField("stage", "dial").WithField("backoff", backoff).Write()
			continue
		}

		session, err := upgradeFunc(conn)
		if err != nil {
			conn.Close()
			backoff = min(backoff*2, maxBackoff)
			syslog.L.Error(err).WithField("attempt", attempt).WithField("stage", "upgrade").WithField("backoff", backoff).Write()
			continue
		}

		return session, nil
	}
}

func openStreamWithReconnect(s *Session, curSession *smux.Session) (*smux.Stream, error) {
	if s == nil {
		return nil, errors.New("session is nil")
	}
	if curSession == nil {
		return nil, errors.New("current session is nil")
	}

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

	if s.reconnectConfig.DialFunc == nil {
		s.Close()
		return nil, errors.New("dialFunc not configured")
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
	conn.Close()

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
		go func() {
			if err := s.attemptReconnect(); err != nil {
				s.state.Store(int32(StateFailed))
			}
		}()
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
	if newSession == nil {
		s.Close()
		return nil, errors.New("new session is nil after reconnection")
	}
	return newSession.OpenStream()
}

func (s *Session) EnableAutoReconnect(rc ReconnectConfig) {
	if s == nil {
		return
	}

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

func (s *Session) connectionMonitor() {
	if s == nil || !s.reconnectConfig.AutoReconnect {
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
						go func() {
							if err := s.attemptReconnect(); err != nil {
								s.state.Store(int32(StateFailed))
							}
						}()
					} else {
						resetTime := s.circuitResetAt.Load()
						if resetTime > 0 && time.Now().Unix() > resetTime {
							s.circuitOpen.Store(false)
							go func() {
								probeCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
								defer cancel()
								if s.reconnectConfig.DialFunc != nil {
									conn, err := dialWithProbe(probeCtx, s.reconnectConfig.DialFunc)
									if err == nil && conn != nil {
										conn.Close()
										if err := s.attemptReconnect(); err != nil {
											s.state.Store(int32(StateFailed))
										}
									}
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

func (s *Session) attemptReconnect() error {
	if s == nil {
		return errors.New("session is nil")
	}

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

	if s.reconnectConfig.DialFunc == nil {
		s.state.Store(int32(StateDisconnected))
		return fmt.Errorf("dialFunc not configured")
	}

	if s.reconnectConfig.UpgradeFunc == nil {
		s.state.Store(int32(StateDisconnected))
		return fmt.Errorf("upgradeFunc not configured")
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
	conn.Close()

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

	if newSession == nil {
		s.state.Store(int32(StateFailed))
		return fmt.Errorf("new session is nil")
	}

	newMuxSess := newSession.muxSess.Load()
	if newMuxSess == nil {
		s.state.Store(int32(StateFailed))
		return fmt.Errorf("new mux session is nil")
	}

	s.reconnectMu.Lock()
	s.muxSess.Store(newMuxSess)
	s.reconnectMu.Unlock()
	s.state.Store(int32(StateConnected))
	return nil
}

func (s *Session) GetState() ConnectionState {
	if s == nil {
		return StateDisconnected
	}
	return ConnectionState(s.state.Load())
}
