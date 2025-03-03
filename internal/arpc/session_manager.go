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
func (sm *SessionManager) GetOrCreateSession(clientID string, conn net.Conn) (*Session, error) {
	// Check if a session already exists for the client
	if session, exists := sm.sessions.Get(clientID); exists {
		return session, nil
	}

	// Create a new session
	session, err := NewServerSession(conn, nil)
	if err != nil {
		return nil, err
	}

	sm.sessions.Set(clientID, session)
	return session, nil
}

// CloseSession closes and removes a Session for a client.
func (sm *SessionManager) CloseSession(clientID string) error {
	if session, exists := sm.sessions.Get(clientID); exists {
		sm.sessions.Del(clientID)
		return session.Close()
	}
	return nil
}
