package websockets

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
)

const (
	defaultHandlerBuffer   = 256
	defaultMessageTimeout  = 5 * time.Second
	defaultShutdownTimeout = 10 * time.Second
	maxClientMessageSize   = 1024 * 1024 // 1MB
	rateLimitPeriod        = 1 * time.Second
	rateLimitMessages      = 100
	heartbeatInterval      = 30 * time.Second
)

var (
	ErrClientNotFound      = errors.New("client not found")
	ErrInvalidClientID     = errors.New("invalid client ID")
	ErrUnauthorized        = errors.New("unauthorized connection attempt")
	ErrMessageSizeExceeded = errors.New("message size exceeded")
)

type ServerConfig struct {
	HandlerBufferSize  int
	MessageTimeout     time.Duration
	EnableRateLimiting bool
}

type Message struct {
	ClientID string    `json:"client_id"`
	Type     string    `json:"type"`
	Content  string    `json:"content"`
	Time     time.Time `json:"time"`
}

type Client struct {
	ID          string
	conn        *websocket.Conn
	server      *Server
	ctx         context.Context
	cancel      context.CancelFunc
	once        sync.Once
	lastMessage time.Time
	rateLimiter *rateLimiter
	version     string
}

type Server struct {
	clients     map[string]*Client
	clientsMux  sync.RWMutex
	handlers    map[chan Message]struct{}
	handlersMux sync.RWMutex
	ctx         context.Context
	cancel      context.CancelFunc
	config      ServerConfig
}

type rateLimiter struct {
	count     int
	windowEnd time.Time
	mu        sync.Mutex
}

func NewServer(ctx context.Context) *Server {
	ctx, cancel := context.WithCancel(ctx)

	return &Server{
		clients:  make(map[string]*Client),
		handlers: make(map[chan Message]struct{}),
		ctx:      ctx,
		cancel:   cancel,
		config: ServerConfig{
			HandlerBufferSize: defaultHandlerBuffer,
			MessageTimeout:    defaultMessageTimeout,
		},
	}
}

func (s *Server) RegisterHandler() (<-chan Message, func()) {
	ch := make(chan Message, s.config.HandlerBufferSize)

	s.handlersMux.Lock()
	s.handlers[ch] = struct{}{}
	s.handlersMux.Unlock()

	cleanup := func() {
		s.handlersMux.Lock()
		defer s.handlersMux.Unlock()

		if _, exists := s.handlers[ch]; exists {
			delete(s.handlers, ch)
			close(ch)
		}
	}

	return ch, cleanup
}

func (s *Server) ServeWS(w http.ResponseWriter, r *http.Request) {
	clientID := r.Header.Get("X-PBS-Agent")
	if clientID == "" || !isValidClientID(clientID) {
		s.handleError(w, ErrInvalidClientID, http.StatusBadRequest)
		return
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols:    []string{"pbs"},
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		syslog.L.Errorf("[WSServer.ServeWS] WebSocket upgrade failed: %v", err)
		return
	}

	conn.SetReadLimit(maxClientMessageSize)

	version := r.Header.Get("X-PBS-Plus-Version")

	ctx, cancel := context.WithCancel(s.ctx)
	client := &Client{
		ID:      clientID,
		conn:    conn,
		server:  s,
		ctx:     ctx,
		cancel:  cancel,
		version: version,
	}

	if s.config.EnableRateLimiting {
		client.rateLimiter = &rateLimiter{}
	}

	s.registerClient(client)
	go s.handleClientMessages(client)
	go s.manageHeartbeats(client)

	syslog.L.Infof("[WSServer.ServeWS] Client connected: %s", clientID)

	<-client.ctx.Done()
	s.unregisterClient(client)
}

func (s *Server) SendToClient(clientID string, msg Message) error {
	client, err := s.getClient(clientID)
	if err != nil {
		return err
	}

	if client.ctx.Err() != nil {
		return fmt.Errorf("client context canceled")
	}

	ctx, cancel := context.WithTimeout(client.ctx, s.config.MessageTimeout)
	defer cancel()

	if err := wsjson.Write(ctx, client.conn, &msg); err != nil {
		return fmt.Errorf("message send failed: %w", err)
	}

	return nil
}

func (s *Server) Run() error {
	defer s.cleanup()
	<-s.ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
	defer cancel()

	select {
	case <-shutdownCtx.Done():
		return errors.New("graceful shutdown timed out")
	default:
		return nil
	}
}

func (s *Server) IsClientConnected(clientID string) bool {
	s.clientsMux.RLock()
	defer s.clientsMux.RUnlock()

	_, exists := s.clients[clientID]
	return exists
}

func (s *Server) GetClientVersion(clientID string) string {
	s.clientsMux.RLock()
	defer s.clientsMux.RUnlock()

	c, exists := s.clients[clientID]
	if exists {
		return c.version
	}
	return ""
}

func (s *Server) handleClientMessages(client *Client) {
	defer func() {
		if r := recover(); r != nil {
			syslog.L.Errorf("[WSServer.handleClientMessages] recovered panic: %v", r)
		}
	}()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-client.ctx.Done():
			return
		default:
			message, err := s.readClientMessage(client)
			if err != nil {
				s.handleReadError(client, err)
				return
			}

			if s.config.EnableRateLimiting && !client.checkRateLimit() {
				syslog.L.Warnf("[WSServer.handleClientMessages] rate limit exceeded for client %s", client.ID)
				continue
			}

			s.distributeMessage(client, message)
		}
	}
}

func (s *Server) readClientMessage(client *Client) (Message, error) {
	var message Message
	err := wsjson.Read(client.ctx, client.conn, &message)
	if err != nil {
		return Message{}, err
	}

	message.ClientID = client.ID
	message.Time = time.Now().UTC()

	return message, nil
}

func (s *Server) distributeMessage(client *Client, msg Message) {
	s.handlersMux.RLock()
	defer s.handlersMux.RUnlock()

	for ch := range s.handlers {
		select {
		case ch <- msg:
		default:
			syslog.L.Warnf("[WSServer.distributeMessage] handler channel full, dropping message from %s", client.ID)
		}
	}
}

func (s *Server) registerClient(client *Client) {
	s.clientsMux.Lock()
	defer s.clientsMux.Unlock()
	s.clients[client.ID] = client
}

func (s *Server) unregisterClient(client *Client) {
	s.clientsMux.Lock()
	defer s.clientsMux.Unlock()

	if _, ok := s.clients[client.ID]; ok {
		client.close()
		delete(s.clients, client.ID)
		syslog.L.Infof("[WSServer.unregisterClient] Client disconnected: %s", client.ID)
	}
}

func (s *Server) getClient(clientID string) (*Client, error) {
	s.clientsMux.RLock()
	defer s.clientsMux.RUnlock()

	client, exists := s.clients[clientID]
	if !exists {
		return nil, ErrClientNotFound
	}
	return client, nil
}

func (s *Server) cleanup() {
	s.clientsMux.Lock()
	defer s.clientsMux.Unlock()
	s.handlersMux.Lock()
	defer s.handlersMux.Unlock()

	for _, client := range s.clients {
		client.close()
	}
	clear(s.clients)

	for ch := range s.handlers {
		close(ch)
	}
	clear(s.handlers)
}

func (c *Client) close() {
	c.once.Do(func() {
		c.cancel()
		err := c.conn.Close(websocket.StatusNormalClosure, "normal shutdown")
		if err != nil && !isExpectedCloseError(err) {
			syslog.L.Warnf("[Client.close] error closing connection: %v", err)
		}
	})
}

func (c *Client) checkRateLimit() bool {
	if c.rateLimiter == nil {
		return true
	}

	c.rateLimiter.mu.Lock()
	defer c.rateLimiter.mu.Unlock()

	now := time.Now()
	if now.After(c.rateLimiter.windowEnd) {
		c.rateLimiter.count = 0
		c.rateLimiter.windowEnd = now.Add(rateLimitPeriod)
	}

	if c.rateLimiter.count >= rateLimitMessages {
		return false
	}

	c.rateLimiter.count++
	return true
}

func (s *Server) manageHeartbeats(client *Client) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(client.ctx, s.config.MessageTimeout)
			defer cancel()

			if err := client.conn.Ping(ctx); err != nil {
				syslog.L.Errorf("[manageHeartbeats] ping failed for %s: %v", client.ID, err)
				return
			}
		case <-client.ctx.Done():
			return
		}
	}
}

func isValidClientID(id string) bool {
	return len(id) > 0 && len(id) <= 256 && strings.TrimSpace(id) == id
}

func isExpectedCloseError(err error) bool {
	return websocket.CloseStatus(err) != -1 ||
		errors.Is(err, net.ErrClosed) ||
		strings.Contains(err.Error(), "use of closed network connection")
}

func (s *Server) handleError(w http.ResponseWriter, err error, status int) {
	syslog.L.Warnf("[WSServer] request error: %v", err)
	w.WriteHeader(status)
}

func (s *Server) handleReadError(client *Client, err error) {
	switch {
	case websocket.CloseStatus(err) == websocket.StatusNormalClosure:
		syslog.L.Errorf("[WSServer] Client %s disconnected normally", client.ID)
		return

	case strings.Contains(err.Error(), "failed to read frame header: EOF"):
		syslog.L.Errorf("[WSServer] Client %s connection closed", client.ID)
		return

	case errors.Is(err, context.Canceled):
		syslog.L.Errorf("[WSServer] Client %s connection canceled", client.ID)
		return

	case errors.Is(err, context.DeadlineExceeded):
		syslog.L.Warnf("[WSServer] Client %s read timeout", client.ID)

	case isExpectedCloseError(err):
		syslog.L.Errorf("[WSServer] Client %s closed connection: %v", client.ID, err)
		return

	default:
		// Log unexpected errors
		syslog.L.Errorf("[WSServer] Read error for client %s: %v", client.ID, err)
	}

	// Force close the connection for abnormal errors
	client.close()

	// Debug logging for connection state
	if s, ok := err.(interface{ Timeout() bool }); ok && s.Timeout() {
		syslog.L.Errorf("[WSServer] Timeout closing connection for client %s", client.ID)
	}
}
