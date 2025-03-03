package arpc

import (
	"net"

	"github.com/alphadose/haxmap"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils/hashmap"
)

type SessionManager struct {
	sessions *haxmap.Map[string, *Session] // Map of client ID to Session
}

func NewSessionManager() *SessionManager {
	return &SessionManager{
		sessions: hashmap.New[*Session](),
	}
}

// GetOrCreateSession ensures a single Session per client.
func (sm *SessionManager) GetOrCreateSession(clientID string, version string, conn net.Conn) (*Session, error) {
	// Check if a session already exists for the client
	if session, exists := sm.sessions.Get(clientID); exists {
		return session, nil
	}

	// Create a new session
	session, err := NewServerSession(conn, nil)
	if err != nil {
		return nil, err
	}
	session.version = version

	router := NewRouter()
	router.Handle("echo", func(req Request) (*Response, error) {
		var msg StringMsg
		if _, err := msg.UnmarshalMsg(req.Payload); err != nil {
			return nil, err
		}
		data, err := msg.MarshalMsg(nil)
		if err != nil {
			return nil, err
		}
		return &Response{Status: 200, Data: data}, nil
	})
	session.SetRouter(router)

	sm.sessions.Set(clientID, session)
	return session, nil
}

func (sm *SessionManager) GetSession(clientID string) (*Session, bool) {
	return sm.sessions.Get(clientID)
}

// CloseSession closes and removes a Session for a client.
func (sm *SessionManager) CloseSession(clientID string) error {
	if session, exists := sm.sessions.Get(clientID); exists {
		sm.sessions.Del(clientID)
		return session.Close()
	}
	return nil
}
