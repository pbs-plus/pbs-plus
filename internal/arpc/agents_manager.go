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

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/safemap"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
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
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	defer conn.SetReadDeadline(time.Time{})

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

	if existingSession, exists := sm.sessions.Get(clientID); exists {
		existingSession.Close()
	}
	if existingQuic, exists := sm.quicSessions.Get(clientID); exists {
		existingQuic.Close()
	}

	if !sm.isExpected(clientID, state.PeerCertificates) {
		return nil, "", errors.New("connection is not expected by server")
	}

	pipe, err := AcceptConnection(ctx, smuxTun, conn)
	if err != nil {
		return nil, "", err
	}

	if existingSession, exists := sm.sessions.Get(clientID); exists {
		existingSession.Close()
		syslog.L.Error(err).WithMessage("agent reconnecting, creating a new pipe").WithField("hostname", clientID).Write()
	}

	router := NewRouter()
	router.Handle("echo", func(req *Request) (Response, error) {
		var msg string
		if err := cbor.Unmarshal(req.Payload, &msg); err != nil {
			return Response{}, WrapError(err)
		}
		data, err := cbor.Marshal(msg)
		if err != nil {
			return Response{}, WrapError(err)
		}
		return Response{Status: 200, Data: data}, nil
	})
	pipe.SetRouter(router)

	sm.sessions.Set(clientID, pipe)

	syslog.L.Info().WithMessage("agent successfully connected").WithField("hostname", clientID).Write()

	return pipe, clientID, nil
}

func (sm *AgentsManager) GetStreamPipe(clientID string) (*StreamPipe, bool) {
	return sm.sessions.Get(clientID)
}

func (sm *AgentsManager) WaitStreamPipe(ctx context.Context, clientID string) (*StreamPipe, error) {
	if pipe, ok := sm.sessions.Get(clientID); ok {
		return pipe, nil
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			if pipe, ok := sm.sessions.Get(clientID); ok {
				return pipe, nil
			}
		}
	}
}

func (sm *AgentsManager) unregisterStreamPipe(clientID string) {
	_, exists := sm.sessions.GetAndDel(clientID)
	if exists {
		syslog.L.Info().WithMessage("agent disconnected").WithField("hostname", clientID).Write()
	}
	sm.rateLimiters.Del(clientID)
}

func (sm *AgentsManager) registerQuicPipe(ctx context.Context, conn *quic.Conn, tlsState *tls.ConnectionState, headers http.Header) (string, error) {
	if err := sm.validateTLSState(tlsState); err != nil {
		return "", err
	}

	state := *tlsState
	clientID := sm.getClientId(state, headers)

	if err := sm.checkRateLimit(clientID); err != nil {
		return "", err
	}

	// Evict any existing session (TCP or QUIC) with the same client ID.
	if existingSession, exists := sm.sessions.Get(clientID); exists {
		existingSession.Close()
	}
	if existingQuic, exists := sm.quicSessions.Get(clientID); exists {
		existingQuic.Close()
	}

	if !sm.isExpected(clientID, state.PeerCertificates) {
		return "", errors.New("connection is not expected by server")
	}

	qPipe := NewQuicServerPipe(ctx, conn)

	router := NewRouter()
	router.Handle("echo", func(req *Request) (Response, error) {
		var msg string
		if err := cbor.Unmarshal(req.Payload, &msg); err != nil {
			return Response{}, WrapError(err)
		}
		data, err := cbor.Marshal(msg)
		if err != nil {
			return Response{}, WrapError(err)
		}
		return Response{Status: 200, Data: data}, nil
	})
	qPipe.SetRouter(router)

	sm.quicSessions.Set(clientID, qPipe)

	syslog.L.Info().WithMessage("agent connected via QUIC").WithField("hostname", clientID).Write()

	return clientID, nil
}

func (sm *AgentsManager) GetQuicPipe(clientID string) (*QuicPipe, bool) {
	return sm.quicSessions.Get(clientID)
}

func (sm *AgentsManager) WaitQuicPipe(ctx context.Context, clientID string) (*QuicPipe, error) {
	if pipe, ok := sm.quicSessions.Get(clientID); ok {
		return pipe, nil
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			if pipe, ok := sm.quicSessions.Get(clientID); ok {
				return pipe, nil
			}
		}
	}
}

func (sm *AgentsManager) unregisterQuicPipe(clientID string) {
	_, exists := sm.quicSessions.GetAndDel(clientID)
	if exists {
		syslog.L.Info().WithMessage("agent QUIC disconnected").WithField("hostname", clientID).Write()
	}
	sm.rateLimiters.Del(clientID)
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
