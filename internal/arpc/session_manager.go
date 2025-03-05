package arpc

import (
	"errors"
	"net"

	"github.com/sonroyaalmerol/pbs-plus/internal/utils/safemap"
)

// SessionManager manages client sessions.
type SessionManager struct {
	sessions *safemap.Map[string, *Session] // Map of client ID to Session
}

// NewSessionManager creates a new SessionManager instance.
func NewSessionManager() *SessionManager {
	return &SessionManager{
		sessions: safemap.New[string, *Session](),
	}
}

// GetOrCreateSession ensures a single Session per client.
// If a session already exists for the given client ID, it is returned.
// Otherwise, a new session is created, initialized, and stored.
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

	// Initialize the router for the session
	router := NewRouter()
	router.Handle("echo", func(req Request) (Response, error) {
		// Echo handler: Unmarshal the payload and return it as the response
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

	// Store the session in the map
	sm.sessions.Set(clientID, session)
	return session, nil
}

// GetSession retrieves an existing session for a client by client ID.
// Returns the session and a boolean indicating whether it exists.
func (sm *SessionManager) GetSession(clientID string) (*Session, bool) {
	return sm.sessions.Get(clientID)
}

// CloseSession closes and removes a Session for a client.
// If the session does not exist, it returns an error.
func (sm *SessionManager) CloseSession(clientID string) error {
	session, exists := sm.sessions.Get(clientID)
	if !exists {
		return errors.New("session not found")
	}

	// Remove the session from the map
	sm.sessions.Del(clientID)

	// Close the session
	return session.Close()
}
