package arpc

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils/safemap"
	"github.com/xtaci/smux"
)

type AgentsManager struct {
	sessions *safemap.Map[string, *StreamPipe]
}

func NewAgentsManager() *AgentsManager {
	return &AgentsManager{
		sessions: safemap.New[string, *StreamPipe](),
	}
}

func (sm *AgentsManager) registerStreamPipe(ctx context.Context, smuxTun *smux.Session, conn net.Conn, headers http.Header) (*StreamPipe, string, error) {
	tlsConn, ok := conn.(*tls.Conn)
	if !ok {
		return nil, "", errors.New("connection is not a TLS connection")
	}

	state := tlsConn.ConnectionState()
	clientID := state.ServerName

	if len(state.PeerCertificates) > 0 {
		clientCertificate := state.PeerCertificates[0]
		clientID = clientCertificate.Subject.CommonName
	}

	jobIdHeader := headers.Get("X-PBS-Plus-JobId")
	if jobIdHeader != "" {
		clientID = clientID + "|" + jobIdHeader
	}

	restoreIdHeader := headers.Get("X-PBS-Plus-RestoreId")
	if restoreIdHeader != "" {
		clientID = clientID + "|" + restoreIdHeader + "|restore"
	}

	if existingSession, exists := sm.sessions.Get(clientID); exists {
		existingSession.Close()
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

func (sm *AgentsManager) unregisterStreamPipe(clientID string) {
	_, exists := sm.sessions.GetAndDel(clientID)
	if exists {
		syslog.L.Info().WithMessage("agent disconnected").WithField("hostname", clientID).Write()
	}
}
