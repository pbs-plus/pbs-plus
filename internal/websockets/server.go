package websockets

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait       = 10 * time.Second
	pongWait        = 60 * time.Second
	pingPeriod      = (pongWait * 9) / 10
	maxMessageSize  = 512 * 1024
	broadcastBuffer = 256
)

type Message struct {
	ClientID string    `json:"client_id"`
	Type     string    `json:"type"`
	Content  string    `json:"content"`
	Time     time.Time `json:"time"`
}

// Client represents a WebSocket client
type Client struct {
	ID     string
	conn   *websocket.Conn
	server *Server
	send   chan Message
	done   chan struct{}
	once   sync.Once
}

// Server manages both WebSocket connections and message broadcasting
type Server struct {
	// WebSocket connections
	clients    map[string]*Client
	clientsMux sync.RWMutex

	// Broadcast functionality
	subscribers    map[chan Message]struct{}
	subscribersMux sync.RWMutex

	// Control channels
	register   chan *Client
	unregister chan *Client
	broadcast  chan Message
	done       chan struct{}
	ctx        context.Context
	cancel     context.CancelFunc
}

var upgrader = websocket.Upgrader{
	CheckOrigin:     func(r *http.Request) bool { return true },
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

// NewServer creates a new server instance
func NewServer(ctx context.Context) *Server {
	ctx, cancel := context.WithCancel(ctx)
	return &Server{
		clients:     make(map[string]*Client),
		subscribers: make(map[chan Message]struct{}),
		register:    make(chan *Client),
		unregister:  make(chan *Client),
		broadcast:   make(chan Message, broadcastBuffer),
		done:        make(chan struct{}),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Subscribe allows any part of the codebase to listen for WebSocket messages
func (s *Server) Subscribe() (<-chan Message, func()) {
	ch := make(chan Message, broadcastBuffer)

	s.subscribersMux.Lock()
	s.subscribers[ch] = struct{}{}
	s.subscribersMux.Unlock()

	cleanup := func() {
		s.subscribersMux.Lock()
		if _, exists := s.subscribers[ch]; exists {
			delete(s.subscribers, ch)
			close(ch)
		}
		s.subscribersMux.Unlock()
	}

	return ch, cleanup
}

// Run starts the server
func (s *Server) Run() {
	defer func() {
		s.clientsMux.Lock()
		for _, client := range s.clients {
			client.close()
		}
		s.clientsMux.Unlock()

		s.subscribersMux.Lock()
		for ch := range s.subscribers {
			close(ch)
		}
		clear(s.subscribers)
		s.subscribersMux.Unlock()

		close(s.done)
	}()

	for {
		select {
		case <-s.ctx.Done():
			return

		case client := <-s.register:
			s.clientsMux.Lock()
			s.clients[client.ID] = client
			s.clientsMux.Unlock()

		case client := <-s.unregister:
			s.clientsMux.Lock()
			if _, ok := s.clients[client.ID]; ok {
				delete(s.clients, client.ID)
				client.close()
			}
			s.clientsMux.Unlock()

		case msg := <-s.broadcast:
			// Broadcast to all subscribers
			s.subscribersMux.RLock()
			deadChannels := make([]chan Message, 0)
			for ch := range s.subscribers {
				select {
				case ch <- msg:
					// Message sent successfully
				default:
					// Channel is full or blocked
					deadChannels = append(deadChannels, ch)
				}
			}
			s.subscribersMux.RUnlock()

			// Clean up dead channels
			if len(deadChannels) > 0 {
				s.subscribersMux.Lock()
				for _, ch := range deadChannels {
					if _, exists := s.subscribers[ch]; exists {
						delete(s.subscribers, ch)
						close(ch)
					}
				}
				s.subscribersMux.Unlock()
			}

			// Also send to all WebSocket clients
			s.clientsMux.RLock()
			for _, client := range s.clients {
				select {
				case client.send <- msg:
					// Message sent successfully
				default:
					go func(c *Client) {
						s.unregister <- c
					}(client)
				}
			}
			s.clientsMux.RUnlock()
		}
	}
}

// ServeWS handles WebSocket connections
func (s *Server) ServeWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Error upgrading connection: %v", err)
		return
	}

	// Read initialization message
	var initMsg Message
	if err := conn.ReadJSON(&initMsg); err != nil || initMsg.Type != "init" {
		log.Printf("Invalid initialization message: %v", err)
		conn.Close()
		return
	}

	client := &Client{
		ID:     initMsg.Content,
		conn:   conn,
		server: s,
		send:   make(chan Message, broadcastBuffer),
		done:   make(chan struct{}),
	}

	s.register <- client

	// Start client routines
	go client.readPump()
	go client.writePump()
}

func (c *Client) readPump() {
	defer func() {
		c.server.unregister <- c
	}()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		var message Message
		err := c.conn.ReadJSON(&message)
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("Error reading message from client %s: %v", c.ID, err)
			}
			break
		}

		message.ClientID = c.ID
		message.Time = time.Now()

		// Send to broadcast channel
		select {
		case c.server.broadcast <- message:
		default:
			log.Printf("Broadcast channel full, dropping message from client %s", c.ID)
		}
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			if err := c.conn.WriteJSON(message); err != nil {
				return
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}

		case <-c.done:
			return
		}
	}
}

func (c *Client) close() {
	c.once.Do(func() {
		close(c.done)
		close(c.send)
		c.conn.Close()
	})
}

// SendToClient sends a message to a specific client
func (s *Server) SendToClient(clientID string, msg Message) error {
	s.clientsMux.RLock()
	client, exists := s.clients[clientID]
	s.clientsMux.RUnlock()

	if !exists {
		return fmt.Errorf("client %s not connected", clientID)
	}

	select {
	case client.send <- msg:
		return nil
	case <-time.After(writeWait):
		return fmt.Errorf("send timeout for client %s", clientID)
	}
}

func (s *Server) IsClientConnected(clientID string) bool {
	s.clientsMux.RLock()
	_, exists := s.clients[clientID]
	s.clientsMux.RUnlock()
	return exists
}
