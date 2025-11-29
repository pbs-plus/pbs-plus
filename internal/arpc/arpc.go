package arpc

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
)

// Session represents a connection over the transport
type Session struct {
	conn   net.Conn
	router atomic.Pointer[Router]

	ctx        context.Context
	cancelFunc context.CancelFunc

	version string
}

func (s *Session) SetRouter(router Router) {
	s.router.Store(&router)
}

func (s *Session) GetRouter() *Router {
	return s.router.Load()
}

func (s *Session) GetVersion() string {
	return s.version
}

// NewSession creates a session from an accepted connection
func NewSession(conn net.Conn) (*Session, error) {
	if conn == nil {
		return nil, errors.New("connection cannot be nil")
	}

	ctx, cancel := context.WithCancel(context.Background())
	session := &Session{
		conn:       conn,
		ctx:        ctx,
		cancelFunc: cancel,
	}

	return session, nil
}

// Serve handles incoming requests on this session in a loop
func (s *Session) Serve() error {
	defer s.conn.Close()

	router := s.GetRouter()
	if router == nil {
		return errors.New("router is nil")
	}

	for {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		default:
		}

		// ServeConn handles one request/response cycle
		if err := router.ServeConn(s.conn); err != nil {
			// Clean EOF means client closed connection
			if err == io.EOF {
				return nil
			}
			// Any other error, return it
			return err
		}
	}
}

func (s *Session) Close() error {
	s.cancelFunc()
	if s.conn != nil {
		return s.conn.Close()
	}
	return nil
}

func (s *Session) GetConn() net.Conn {
	return s.conn
}
