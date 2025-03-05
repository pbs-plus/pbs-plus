package arpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

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
	resultCh := make(chan dialResult, 1)
	go func() {
		conn, err := dialFunc()
		select {
		case resultCh <- dialResult{conn, err}:
		default:
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
	if initialBackoff <= 0 {
		initialBackoff = 100 * time.Millisecond
	}
	if maxBackoff <= 0 {
		maxBackoff = 30 * time.Second
	}

	backoff := initialBackoff
	timer := time.NewTimer(0)
	defer timer.Stop()

	for attempt := 0; ; attempt++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if attempt > 0 {
			jitteredBackoff := getJitteredBackoff(backoff, 0.2)
			timer.Reset(jitteredBackoff)
			select {
			case <-timer.C:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		conn, err := dialFunc()
		if err != nil {
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		session, err := upgradeFunc(conn)
		if err != nil {
			conn.Close()
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		return session, nil
	}
}

func openStreamWithReconnect(s *Session, curSession *smux.Session) (*smux.Stream, error) {
	stream, err := curSession.OpenStream()
	if err == nil {
		return stream, nil
	}

	if !s.reconnectConfig.AutoReconnect {
		return nil, err
	}

	if ConnectionState(s.state.Load()) == StateConnected {
		s.state.Store(int32(StateDisconnected))
	}

	if s.circuitOpen.Load() {
		if resetTime := s.circuitResetAt.Load(); time.Now().Unix() < resetTime {
			return nil, errors.New("circuit breaker open")
		}
		s.circuitOpen.Store(false)
	}

	probeCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if _, err := dialWithProbe(probeCtx, s.reconnectConfig.DialFunc); err != nil {
		s.circuitOpen.Store(true)
		s.circuitResetAt.Store(time.Now().Add(5 * time.Second).Unix())
		return nil, errors.New("server unreachable")
	}

	if ConnectionState(s.state.Load()) != StateReconnecting {
		go s.attemptReconnect()
	}

	timeout := getJitteredBackoff(5*time.Second, 0.3)
	select {
	case <-s.reconnectChan:
	case <-time.After(timeout):
		return nil, errors.New("reconnect timeout")
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}

	if ConnectionState(s.state.Load()) != StateConnected {
		return nil, errors.New("reconnect failed")
	}

	return s.muxSess.Load().OpenStream()
}

func (s *Session) EnableAutoReconnect(rc ReconnectConfig) {
	if rc.InitialBackoff <= 0 {
		rc.InitialBackoff = 100 * time.Millisecond
	}
	if rc.MaxBackoff <= 0 {
		rc.MaxBackoff = 30 * time.Second
	}
	rc.BackoffJitter = max(rc.BackoffJitter, 0.2)
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
	if !s.reconnectConfig.AutoReconnect {
		return
	}

	timer := time.NewTimer(getJitteredBackoff(5*time.Second, 0.5))
	defer timer.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-timer.C:
			sess := s.muxSess.Load()
			if sess == nil || sess.IsClosed() {
				if state := ConnectionState(s.state.Load()); state != StateReconnecting {
					if !s.circuitOpen.Load() {
						go s.attemptReconnect()
					} else if resetTime := s.circuitResetAt.Load(); time.Now().Unix() > resetTime {
						s.circuitOpen.Store(false)
						go s.probeAndReconnect()
					}
				}
			}
			timer.Reset(getJitteredBackoff(5*time.Second, 0.2))
		}
	}
}

func (s *Session) probeAndReconnect() {
	probeCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	if conn, err := dialWithProbe(probeCtx, s.reconnectConfig.DialFunc); err == nil {
		conn.Close()
		s.attemptReconnect()
	}
}

func (s *Session) attemptReconnect() error {
	if !s.state.CompareAndSwap(int32(StateDisconnected), int32(StateReconnecting)) {
		if ConnectionState(s.state.Load()) == StateConnected {
			return nil
		}
		select {
		case <-s.reconnectChan:
			return nil
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	}
	defer func() { s.reconnectChan <- struct{}{} }()

	if !s.reconnectConfig.AutoReconnect {
		s.state.Store(int32(StateDisconnected))
		return fmt.Errorf("auto reconnect disabled")
	}

	probeCtx, cancel := context.WithTimeout(s.reconnectConfig.ReconnectCtx, 2*time.Second)
	defer cancel()
	if _, err := dialWithProbe(probeCtx, s.reconnectConfig.DialFunc); err != nil {
		s.circuitOpen.Store(true)
		s.circuitResetAt.Store(time.Now().Add(5 * time.Second).Unix())
		s.state.Store(int32(StateDisconnected))
		return fmt.Errorf("probe failed: %w", err)
	}

	session, err := dialWithBackoff(
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
		return fmt.Errorf("reconnect failed: %w", err)
	}

	s.reconnectMu.Lock()
	s.muxSess.Store(session.muxSess.Load())
	s.reconnectMu.Unlock()
	s.state.Store(int32(StateConnected))
	return nil
}

func (s *Session) GetState() ConnectionState {
	return ConnectionState(s.state.Load())
}
