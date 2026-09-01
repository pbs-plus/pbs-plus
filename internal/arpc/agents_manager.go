package arpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/safemap"
	"github.com/quic-go/quic-go"
	"github.com/xtaci/smux"
	"golang.org/x/time/rate"
)

type AgentsManager struct {
	expectedList *safemap.Map[string, struct{}]
	sessions     *safemap.Map[string, *StreamPipe]
	quicSessions *safemap.Map[string, *QuicPipe]
	rateLimiters *safemap.Map[string, *rate.Limiter]

	mu                sync.Mutex
	customExpectCheck func(string, []*x509.Certificate) bool

	regMu sync.Mutex
}

func NewAgentsManager() *AgentsManager {
	return &AgentsManager{
		expectedList: safemap.New[string, struct{}](),
		sessions:     safemap.New[string, *StreamPipe](),
		quicSessions: safemap.New[string, *QuicPipe](),
		rateLimiters: safemap.New[string, *rate.Limiter](),
	}
}

func (sm *AgentsManager) Expect(id string) {
	sm.expectedList.Set(id, struct{}{})
}

func (sm *AgentsManager) SetExtraExpectFunc(custom func(string, []*x509.Certificate) bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.customExpectCheck = custom
}

func (sm *AgentsManager) NotExpect(id string) {
	sm.expectedList.Del(id)
}

func (sm *AgentsManager) isExpected(id string, cert []*x509.Certificate) bool {
	_, expected := sm.expectedList.Get(id)

	if expected {
		return true
	}

	customExpected := false

	sm.mu.Lock()
	custom := sm.customExpectCheck
	sm.mu.Unlock()

	if custom != nil {
		customExpected = custom(id, cert)
	}

	return customExpected
}

func (sm *AgentsManager) getClientId(state tls.ConnectionState, headers http.Header) string {
	clientID := state.ServerName

	if len(state.PeerCertificates) > 0 {
		clientCertificate := state.PeerCertificates[0]
		clientID = clientCertificate.Subject.CommonName
	}

	jobIdHeader := headers.Get("X-PBS-Plus-BackupID")
	if jobIdHeader != "" {
		clientID = clientID + "|" + jobIdHeader
	}

	restoreIdHeader := headers.Get("X-PBS-Plus-RestoreID")
	if restoreIdHeader != "" {
		clientID = clientID + "|" + restoreIdHeader + "|restore"
	}

	verifyIdHeader := headers.Get("X-PBS-Plus-VerifyID")
	if verifyIdHeader != "" {
		clientID = clientID + "|" + verifyIdHeader + "|verify"
	}

	return clientID
}

func (sm *AgentsManager) validateClientCert(state tls.ConnectionState) error {
	if len(state.PeerCertificates) == 0 {
		return errors.New("no client certificate provided")
	}

	cert := state.PeerCertificates[0]

	now := time.Now()
	if now.Before(cert.NotBefore) || now.After(cert.NotAfter) {
		return fmt.Errorf("certificate expired or not yet valid")
	}

	if len(state.VerifiedChains) == 0 {
		return errors.New("certificate chain verification failed")
	}

	return nil
}

func (sm *AgentsManager) checkRateLimit(clientID string) error {
	limiter, _ := sm.rateLimiters.GetOrSet(clientID, rate.NewLimiter(rate.Limit(10), 20))

	if !limiter.Allow() {
		return errors.New("rate limit exceeded")
	}
	return nil
}

func (sm *AgentsManager) registerStreamPipe(ctx context.Context, smuxTun *smux.Session, conn net.Conn, headers http.Header) (*StreamPipe, string, error) {
	if err := conn.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return nil, "", err
	}
	defer func() { _ = conn.SetReadDeadline(time.Time{}) }()

	tlsConn, ok := conn.(*tls.Conn)
	if !ok {
		return nil, "", errors.New("connection is not a TLS connection")
	}

	state := tlsConn.ConnectionState()

	if err := sm.validateClientCert(state); err != nil {
		return nil, "", err
	}

	clientID := sm.getClientId(state, headers)

	if err := sm.checkRateLimit(clientID); err != nil {
		return nil, "", err
	}

	if !sm.isExpected(clientID, state.PeerCertificates) {
		return nil, "", errors.New("connection is not expected by server")
	}

	pipe, err := AcceptConnection(ctx, smuxTun, conn)
	if err != nil {
		return nil, "", err
	}

	sm.regMu.Lock()
	if existingSession, exists := sm.sessions.Get(clientID); exists {
		existingSession.Close()
	}
	if existingQuic, exists := sm.quicSessions.Get(clientID); exists {
		existingQuic.Close()
	}
	sm.sessions.Set(clientID, pipe)
	sm.regMu.Unlock()
	log.Info("agent successfully connected", "hostname", clientID)

	return pipe, clientID, nil
}

func (sm *AgentsManager) GetStreamPipe(clientID string) (*StreamPipe, bool) {
	pipe, ok := sm.sessions.Get(clientID)
	if !ok {
		return nil, false
	}
	if pipe.GetState() != StateConnected {
		return nil, false
	}
	return pipe, true
}

func (sm *AgentsManager) WaitStreamPipe(ctx context.Context, clientID string) (*StreamPipe, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			if pipe, ok := sm.GetStreamPipe(clientID); ok {
				return pipe, nil
			}
		}
	}
}

// unregisterStreamIfCurrent drops the registration only when the map still holds this exact pipe (reconnect-safe).
func (sm *AgentsManager) unregisterStreamIfCurrent(clientID string, pipe *StreamPipe) {
	sm.regMu.Lock()
	defer sm.regMu.Unlock()
	if cur, ok := sm.sessions.Get(clientID); ok && cur == pipe {
		sm.sessions.Del(clientID)
		log.Info("agent disconnected", "hostname", clientID)
		sm.rateLimiters.Del(clientID)
	}
}

func (sm *AgentsManager) registerQuicPipe(ctx context.Context, conn *quic.Conn, tlsState *tls.ConnectionState, headers http.Header) (*QuicPipe, string, error) {
	if err := sm.validateTLSState(tlsState); err != nil {
		return nil, "", err
	}

	state := *tlsState
	clientID := sm.getClientId(state, headers)

	if err := sm.checkRateLimit(clientID); err != nil {
		return nil, "", err
	}

	if !sm.isExpected(clientID, state.PeerCertificates) {
		return nil, "", errors.New("connection is not expected by server")
	}

	qPipe := NewQuicServerPipe(ctx, conn)

	sm.regMu.Lock()
	if existingSession, exists := sm.sessions.Get(clientID); exists {
		existingSession.Close()
	}
	if existingQuic, exists := sm.quicSessions.Get(clientID); exists {
		existingQuic.Close()
	}
	sm.quicSessions.Set(clientID, qPipe)
	sm.regMu.Unlock()
	log.Info("agent connected via QUIC", "hostname", clientID)

	return qPipe, clientID, nil
}

func (sm *AgentsManager) GetQuicPipe(clientID string) (*QuicPipe, bool) {
	pipe, ok := sm.quicSessions.Get(clientID)
	if !ok {
		return nil, false
	}
	if pipe.GetState() != StateConnected {
		return nil, false
	}
	return pipe, true
}

// IsOnline reports whether the agent currently holds a live session (QUIC or stream).
func (sm *AgentsManager) IsOnline(clientID string) bool {
	if _, ok := sm.GetQuicPipe(clientID); ok {
		return true
	}
	_, ok := sm.GetStreamPipe(clientID)
	return ok
}

// unregisterQuicIfCurrent drops the registration only when the map still holds this exact pipe (reconnect-safe).
func (sm *AgentsManager) unregisterQuicIfCurrent(clientID string, pipe *QuicPipe) {
	sm.regMu.Lock()
	defer sm.regMu.Unlock()
	if cur, ok := sm.quicSessions.Get(clientID); ok && cur == pipe {
		sm.quicSessions.Del(clientID)
		log.Info("agent QUIC disconnected", "hostname", clientID)
		sm.rateLimiters.Del(clientID)
	}
}

func (sm *AgentsManager) validateTLSState(state *tls.ConnectionState) error {
	if state == nil {
		return errors.New("nil TLS state")
	}
	if len(state.PeerCertificates) == 0 {
		return errors.New("no client certificate provided")
	}

	cert := state.PeerCertificates[0]
	now := time.Now()
	if now.Before(cert.NotBefore) || now.After(cert.NotAfter) {
		return fmt.Errorf("certificate expired or not yet valid")
	}
	if len(state.VerifiedChains) == 0 {
		return errors.New("certificate chain verification failed")
	}
	return nil
}
