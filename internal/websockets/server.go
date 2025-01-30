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

	for _, opt := range opts {
		opt(s)
	}

	return s
}

func (s *Server) RegisterHandler(msgType string, handler MessageHandler) {
	s.handlerMu.Lock()
	s.handlers[msgType] = append(s.handlers[msgType], handler)
	s.handlerMu.Unlock()

	if syslog.L != nil {
		syslog.L.Infof("Registered new handler for message type: %s", msgType)
	}
}

func (s *Server) handleMessage(msg *Message) {
	s.handlerMu.RLock()
	handlers, exists := s.handlers[msg.Type]
	s.handlerMu.RUnlock()

	if !exists {
		if syslog.L != nil {
			syslog.L.Infof("No handlers registered for message type: %s", msg.Type)
		}
		return
	}

	for _, handler := range handlers {
		handler := handler // Create new variable for closure
		ctx, cancel := context.WithTimeout(s.ctx, messageTimeout)

		err := s.workers.Submit(ctx, func() {
			if err := handler(ctx, msg); err != nil && syslog.L != nil {
				syslog.L.Errorf("Handler error for message type %s: %v", msg.Type, err)
			}
		})

		if err != nil && syslog.L != nil {
			syslog.L.Warnf("Failed to submit message to worker pool: %v", err)
		}

		cancel()
	}
}

func (s *Server) ServeWS(w http.ResponseWriter, r *http.Request) {
	clientID := r.Header.Get("X-PBS-Agent")
	if clientID == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	clientVersion := r.Header.Get("X-PBS-Plus-Version")

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols: []string{"pbs"},
	})
	if err != nil {
		if syslog.L != nil {
			syslog.L.Errorf("Failed to accept websocket connection: %v", err)
		}
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
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-client.ctx.Done():
			return
		default:
			var msg Message
			if err := wsjson.Read(client.ctx, client.conn, &msg); err != nil {
				if websocket.CloseStatus(err) != websocket.StatusNormalClosure && syslog.L != nil {
					syslog.L.Errorf("Read error for client %s: %v", client.ID, err)
				}
				return
			}

			msg.ClientID = client.ID
			msg.Time = time.Now()

			s.handleMessage(&msg)
		}
	}
}

func (s *Server) registerClient(client *Client) {
	s.clientsMu.Lock()
	s.clients[client.ID] = client
	s.clientsMu.Unlock()

	if syslog.L != nil {
		syslog.L.Infof("Client registered: %s", client.ID)
	}
}

func (s *Server) unregisterClient(client *Client) {
	if client == nil {
		return
	}

	s.clientsMu.Lock()
	if _, exists := s.clients[client.ID]; exists {
		client.close()
		delete(s.clients, client.ID)
	}
	s.clientsMu.Unlock()

	if syslog.L != nil {
		syslog.L.Infof("Client unregistered: %s", client.ID)
	}
}

func (c *Client) close() {
	c.closeOnce.Do(func() {
		c.cancel()
		c.conn.Close(websocket.StatusNormalClosure, "client disconnecting")
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

	return wsjson.Write(ctx, client.conn, &msg)
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.cancel()

	done := make(chan struct{})
	go func() {
		s.cleanup()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) cleanup() {
	s.clientsMu.Lock()
	for _, client := range s.clients {
		client.close()
	}
	clear(s.clients)
	s.clientsMu.Unlock()

	s.workers.Wait()
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
