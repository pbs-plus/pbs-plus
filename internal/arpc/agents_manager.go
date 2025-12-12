package arpc

import (
	"errors"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils/safemap"
	"github.com/quic-go/quic-go"
)

type AgentsManager struct {
	sessions *safemap.Map[string, *StreamPipe]
}

func NewAgentsManager() *AgentsManager {
	return &AgentsManager{
		sessions: safemap.New[string, *StreamPipe](),
	}
}

func (sm *AgentsManager) GetOrCreateStreamPipe(conn *quic.Conn) (*StreamPipe, string, error) {
	clientID := conn.ConnectionState().TLS.ServerName

	if len(conn.ConnectionState().TLS.PeerCertificates) > 0 {
		clientCertificate := conn.ConnectionState().TLS.PeerCertificates[0]
		clientID = clientCertificate.Subject.CommonName
	}

	if session, exists := sm.sessions.Get(clientID); exists {
		return session, "", nil
	}

	session, err := NewStreamPipe(conn)
	if err != nil {
		return nil, "", err
	}

	router := NewRouter()
	router.Handle("echo", func(req Request) (Response, error) {
		var msg StringMsg
		if err := msg.Decode(req.Payload); err != nil {
			return Response{}, WrapError(err)
		}
		data, err := msg.Encode()
		if err != nil {
			return Response{}, WrapError(err)
		}
		return Response{Status: 200, Data: data}, nil
	})
	session.SetRouter(router)

	sm.sessions.Set(clientID, session)

	syslog.L.Info().WithMessage("agent successfully connected").WithField("hostname", clientID).Write()

	return session, clientID, nil
}

func (sm *AgentsManager) GetStreamPipe(clientID string) (*StreamPipe, bool) {
	return sm.sessions.Get(clientID)
}

func (sm *AgentsManager) CloseStreamPipe(clientID string) error {
	session, exists := sm.sessions.Get(clientID)
	if !exists {
		return errors.New("session not found")
	}

	sm.sessions.Del(clientID)
	syslog.L.Info().WithMessage("agent disconnected").WithField("hostname", clientID).Write()

	return session.Close()
}
