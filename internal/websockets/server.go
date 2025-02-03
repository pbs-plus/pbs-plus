package websockets

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
)

const (
	workerPoolSize = 100
)

type (
	Message struct {
		ClientID string    `json:"client_id"`
		Type     string    `json:"type"`
		Content  string    `json:"content"`
		Time     time.Time `json:"time"`
	}

	Client struct {
		ID           string
		AgentVersion string
		conn         *websocket.Conn
		server       *Server
		ctx          context.Context
		cancel       context.CancelFunc
		closeOnce    sync.Once
	}

	Server struct {
		// Client management
		clients   map[string]*Client
		clientsMu sync.RWMutex

		// Message handling
		handlers  map[string][]MessageHandler
		handlerMu sync.RWMutex
		workers   *WorkerPool

		// Context management
		ctx    context.Context
		cancel context.CancelFunc
	}

	ServerOption func(*Server)
)

func WithWorkerPoolSize(size int) ServerOption {
	return func(s *Server) {
		s.workers = NewWorkerPool(size)
		syslog.L.Infof("Worker pool size configured | size=%d", size)
	}
}

func NewServer(ctx context.Context, opts ...ServerOption) *Server {
	ctx, cancel := context.WithCancel(ctx)

	s := &Server{
		clients:  make(map[string]*Client),
		handlers: make(map[string][]MessageHandler),
		workers:  NewWorkerPool(workerPoolSize),
		ctx:      ctx,
		cancel:   cancel,
	}

	syslog.L.Info("Initializing WebSocket server")

	for _, opt := range opts {
		opt(s)
	}

	syslog.L.Info("WebSocket server initialized successfully")
	return s
}

type UnregisterFunc func()

func (s *Server) RegisterHandler(msgType string, handler MessageHandler) UnregisterFunc {
	s.handlerMu.Lock()
	currentHandlers := s.handlers[msgType]
	handlerIndex := len(currentHandlers)
	s.handlers[msgType] = append(currentHandlers, handler)
	s.handlerMu.Unlock()

	syslog.L.Infof("Registered message handler | type=%s handler_count=%d",
		msgType, handlerIndex+1)

	return func() {
		s.handlerMu.Lock()
		defer s.handlerMu.Unlock()

		handlers := s.handlers[msgType]
		if handlerIndex >= len(handlers) {
			syslog.L.Warnf("Handler already unregistered | type=%s handler_index=%d",
				msgType, handlerIndex)
			return
		}

		newHandlers := make([]MessageHandler, 0, len(handlers)-1)
		newHandlers = append(newHandlers, handlers[:handlerIndex]...)
		if handlerIndex+1 < len(handlers) {
			newHandlers = append(newHandlers, handlers[handlerIndex+1:]...)
		}

		if len(newHandlers) == 0 {
			delete(s.handlers, msgType)
		} else {
			s.handlers[msgType] = newHandlers
		}

		syslog.L.Infof("Unregistered message handler | type=%s remaining_handlers=%d",
			msgType, len(newHandlers))
	}
}

func (s *Server) handleMessage(msg *Message) {
	s.handlerMu.RLock()
	handlers, exists := s.handlers[msg.Type]
	handlerCount := len(handlers)
	s.handlerMu.RUnlock()

	if !exists {
		syslog.L.Warnf("No handlers registered | message_type=%s client_id=%s",
			msg.Type, msg.ClientID)
		return
	}

	syslog.L.Infof("Processing message | type=%s client_id=%s handler_count=%d",
		msg.Type, msg.ClientID, handlerCount)

	for i, handler := range handlers {
		handler := handler // Create new variable for closure
		ctx, cancel := context.WithTimeout(s.ctx, messageTimeout)

		start := time.Now()
		err := s.workers.Submit(ctx, func() {
			if err := handler(ctx, msg); err != nil {
				syslog.L.Errorf("Handler error | type=%s client_id=%s handler_index=%d error=%v duration=%v",
					msg.Type, msg.ClientID, i, err, time.Since(start))
			} else {
				syslog.L.Infof("Handler completed | type=%s client_id=%s handler_index=%d duration=%v",
					msg.Type, msg.ClientID, i, time.Since(start))
			}
		})

		if err != nil {
			syslog.L.Errorf("Worker pool submission failed | type=%s client_id=%s handler_index=%d error=%v",
				msg.Type, msg.ClientID, i, err)
		}

		cancel()
	}
}

func (s *Server) ServeWS(w http.ResponseWriter, r *http.Request) {
	clientID := r.Header.Get("X-PBS-Agent")
	if clientID == "" {
		syslog.L.Warnf("Rejected WebSocket connection | reason=missing_client_id remote_addr=%s",
			r.RemoteAddr)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	clientVersion := r.Header.Get("X-PBS-Plus-Version")
	syslog.L.Infof("WebSocket connection request | client_id=%s version=%s remote_addr=%s",
		clientID, clientVersion, r.RemoteAddr)

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols: []string{"pbs"},
	})
	if err != nil {
		syslog.L.Errorf("WebSocket acceptance failed | client_id=%s error=%v", clientID, err)
		return
	}

	ctx, cancel := context.WithCancel(s.ctx)
	client := &Client{
		ID:           clientID,
		AgentVersion: clientVersion,
		conn:         conn,
		server:       s,
		ctx:          ctx,
		cancel:       cancel,
	}

	s.registerClient(client)
	go s.handleClientConnection(client)

	<-client.ctx.Done()
	s.unregisterClient(client)
}

func (s *Server) handleClientConnection(client *Client) {
	syslog.L.Infof("Starting client connection handler | client_id=%s version=%s",
		client.ID, client.AgentVersion)

	defer syslog.L.Infof("Client connection handler stopped | client_id=%s", client.ID)

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-client.ctx.Done():
			return
		default:
			var msg Message
			if err := wsjson.Read(client.ctx, client.conn, &msg); err != nil {
				if !isNormalClosureError(err) {
					syslog.L.Errorf("Message read error | client_id=%s error=%v",
						client.ID, err)
				} else {
					syslog.L.Infof("Client connection closed normally | client_id=%s",
						client.ID)
				}
				return
			}

			msg.ClientID = client.ID
			msg.Time = time.Now()

			syslog.L.Infof("Received message | type=%s client_id=%s", msg.Type, msg.ClientID)
			s.handleMessage(&msg)
		}
	}
}

func (s *Server) registerClient(client *Client) {
	s.clientsMu.Lock()
	s.clients[client.ID] = client
	clientCount := len(s.clients)
	s.clientsMu.Unlock()

	syslog.L.Infof("Client registered | id=%s version=%s total_clients=%d",
		client.ID, client.AgentVersion, clientCount)
}

func (s *Server) unregisterClient(client *Client) {
	if client == nil {
		return
	}

	s.clientsMu.Lock()
	if _, exists := s.clients[client.ID]; exists {
		client.close()
		delete(s.clients, client.ID)
		clientCount := len(s.clients)
		s.clientsMu.Unlock()

		syslog.L.Infof("Client unregistered | id=%s total_clients=%d",
			client.ID, clientCount)
	} else {
		s.clientsMu.Unlock()
		syslog.L.Warnf("Attempted to unregister non-existent client | id=%s",
			client.ID)
	}
}

func (c *Client) close() {
	c.closeOnce.Do(func() {
		syslog.L.Infof("Closing client connection | id=%s", c.ID)
		c.cancel()
		if err := c.conn.Close(websocket.StatusNormalClosure, "client disconnecting"); err != nil {
			syslog.L.Errorf("Error closing client connection | id=%s error=%v", c.ID, err)
		}
	})
}

func (s *Server) SendToClient(clientID string, msg Message) error {
	s.clientsMu.RLock()
	client, exists := s.clients[clientID]
	s.clientsMu.RUnlock()

	if !exists {
		return fmt.Errorf("client %s not connected", clientID)
	}

	ctx, cancel := context.WithTimeout(client.ctx, messageTimeout)
	defer cancel()

	start := time.Now()
	err := wsjson.Write(ctx, client.conn, &msg)
	if err != nil {
		syslog.L.Errorf("Failed to send message | client_id=%s type=%s error=%v duration=%v",
			clientID, msg.Type, err, time.Since(start))
		return err
	}

	syslog.L.Infof("Message sent successfully | client_id=%s type=%s duration=%v",
		clientID, msg.Type, time.Since(start))
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	syslog.L.Info("Starting server shutdown")
	s.cancel()

	done := make(chan struct{})
	go func() {
		s.cleanup()
		close(done)
	}()

	select {
	case <-done:
		syslog.L.Info("Server shutdown completed successfully")
		return nil
	case <-ctx.Done():
		syslog.L.Errorf("Server shutdown timed out | error=%v", ctx.Err())
		return ctx.Err()
	}
}

func (s *Server) cleanup() {
	start := time.Now()

	s.clientsMu.Lock()
	clientCount := len(s.clients)
	for _, client := range s.clients {
		client.close()
	}
	clear(s.clients)
	s.clientsMu.Unlock()

	s.workers.Wait()

	syslog.L.Infof("Cleanup completed | clients_closed=%d duration=%v",
		clientCount, time.Since(start))
}

func (s *Server) IsClientConnected(clientID string) bool {
	s.clientsMu.RLock()
	_, exists := s.clients[clientID]
	s.clientsMu.RUnlock()
	return exists
}

func (s *Server) GetClientVersion(clientID string) string {
	s.clientsMu.RLock()
	client, exists := s.clients[clientID]
	s.clientsMu.RUnlock()

	if exists {
		return client.AgentVersion
	}
	return ""
}
