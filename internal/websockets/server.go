package websockets

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
)

const (
	handlerBuffer = 256
)

type Message struct {
	ClientID string    `json:"client_id"`
	Type     string    `json:"type"`
	Content  string    `json:"content"`
	Time     time.Time `json:"time"`
}

type Client struct {
	ID           string
	agentVersion string
	conn         *websocket.Conn
	server       *Server
	ctx          context.Context
	cancel       context.CancelFunc
	once         sync.Once
}

type Server struct {
	clients     map[string]*Client
	clientsMux  sync.RWMutex
	handlers    map[chan Message]struct{}
	handlersMux sync.RWMutex
	ctx         context.Context
	cancel      context.CancelFunc
}

func NewServer(ctx context.Context) *Server {
	ctx, cancel := context.WithCancel(ctx)
	return &Server{
		clients:  make(map[string]*Client),
		handlers: make(map[chan Message]struct{}),
		ctx:      ctx,
		cancel:   cancel,
	}
}

func (s *Server) RegisterHandler() (<-chan Message, func()) {
	ch := make(chan Message, handlerBuffer)

	s.handlersMux.Lock()
	s.handlers[ch] = struct{}{}
	s.handlersMux.Unlock()

	cleanup := func() {
		s.handlersMux.Lock()
		if _, exists := s.handlers[ch]; exists {
			delete(s.handlers, ch)
			close(ch)
		}
		s.handlersMux.Unlock()
	}

	return ch, cleanup
}

func (s *Server) handleClientMessages(client *Client) {
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-client.ctx.Done():
			return
		default:
			message := Message{}
			err := wsjson.Read(client.ctx, client.conn, &message)
			if err != nil {
				if !strings.Contains(err.Error(), "failed to read frame header: EOF") {
					if websocket.CloseStatus(err) != websocket.StatusNormalClosure {
						syslog.L.Errorf("[WSServer.MessageHandler] Read error for client %s: %v", client.ID, err)
					}
				}
				return
			}

			message.ClientID = client.ID
			message.Time = time.Now()

			s.handlersMux.RLock()
			for ch := range s.handlers {
				select {
				case ch <- message:
				default:
					syslog.L.Warnf("[WSServer.MessageHandler] Handler channel full, dropping message from client %s", client.ID)
				}
			}
			s.handlersMux.RUnlock()
		}
	}
}

func (s *Server) Register(client *Client) {
	s.clientsMux.Lock()
	s.clients[client.ID] = client
	s.clientsMux.Unlock()
}

func (s *Server) Unregister(client *Client) {
	if client == nil {
		return
	}
	s.clientsMux.Lock()
	if _, ok := s.clients[client.ID]; ok {
		client.close()
		delete(s.clients, client.ID)
	}
	s.clientsMux.Unlock()
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
		return
	}

	ctx, cancel := context.WithCancel(s.ctx)
	client := &Client{
		ID:           clientID,
		conn:         conn,
		server:       s,
		ctx:          ctx,
		cancel:       cancel,
		agentVersion: clientVersion,
	}

	s.Register(client)
	go s.handleClientMessages(client)

	<-client.ctx.Done()
	s.Unregister(client)
}

func (c *Client) close() {
	c.once.Do(func() {
		c.cancel()
		c.conn.Close(websocket.StatusNormalClosure, "client disconnecting")
	})
}

func (s *Server) SendToClient(clientID string, msg Message) error {
	s.clientsMux.RLock()
	client, exists := s.clients[clientID]
	s.clientsMux.RUnlock()

	if !exists {
		return fmt.Errorf("client %s not connected", clientID)
	}

	ctx, cancel := context.WithTimeout(client.ctx, messageTimeout)
	defer cancel()

	if err := wsjson.Write(ctx, client.conn, &msg); err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	return nil
}

func (s *Server) IsClientConnected(clientID string) bool {
	s.clientsMux.RLock()
	_, exists := s.clients[clientID]
	s.clientsMux.RUnlock()
	return exists
}

func (s *Server) GetClientVersion(clientID string) string {
	s.clientsMux.RLock()
	client, exists := s.clients[clientID]
	s.clientsMux.RUnlock()

	if exists {
		return client.agentVersion
	}

	return ""
}

func (s *Server) Run() {
	defer func() {
		s.clientsMux.Lock()
		for _, client := range s.clients {
			client.close()
		}
		clear(s.clients)
		s.clientsMux.Unlock()

		s.handlersMux.Lock()
		for ch := range s.handlers {
			close(ch)
		}
		clear(s.handlers)
		s.handlersMux.Unlock()

		s.cancel()
	}()

	<-s.ctx.Done()
}

func (s *Server) cleanup() {
	s.clientsMux.Lock()
	for _, client := range s.clients {
		client.close()
	}
	clear(s.clients)
	s.clientsMux.Unlock()

	s.handlersMux.Lock()
	for ch := range s.handlers {
		close(ch)
	}
	clear(s.handlers)
	s.handlersMux.Unlock()
}
