package arpc

import (
	"errors"
	"net"

	"github.com/pbs-plus/pbs-plus/internal/utils/safemap"
)

type AgentsManager struct {
	sessions *safemap.Map[string, *Session]
}

func NewAgentsManager() *AgentsManager {
	return &AgentsManager{
		sessions: safemap.New[string, *Session](),
	}
}

func (sm *AgentsManager) GetOrCreateSession(clientID string, version string, conn net.Conn) (*Session, error) {
	if session, exists := sm.sessions.Get(clientID); exists {
		return session, nil
	}

	session, err := NewServerSession(conn, nil)
	if err != nil {
		return nil, err
	}
	session.version = version

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
	return session, nil
}

func (sm *AgentsManager) GetSession(clientID string) (*Session, bool) {
	return sm.sessions.Get(clientID)
}

func (sm *AgentsManager) CloseSession(clientID string) error {
	session, exists := sm.sessions.Get(clientID)
	if !exists {
		return errors.New("session not found")
	}

	sm.sessions.Del(clientID)

	return session.Close()
}
