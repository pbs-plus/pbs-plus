package websockets

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"golang.org/x/time/rate"
)

const (
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
	ID          string
	conn        *websocket.Conn
	server      *Server
	rateLimiter *rate.Limiter
	send        chan Message
	done        chan struct{}
	once        sync.Once
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

func (s *Server) handleClientMessages(client *Client) {
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
			message := Message{}
			err := wsjson.Read(context.Background(), client.conn, &message)
			if err != nil {
				if websocket.CloseStatus(err) != websocket.StatusNormalClosure {
					log.Printf("read error: %v", err)
				}
				return
			}

			message.ClientID = client.ID
			message.Time = time.Now()

			// Send to broadcast channel with timeout
			select {
			case s.broadcast <- message:
				// Message sent successfully
			case <-time.After(time.Second):
				log.Printf("Broadcast channel full or blocked")
			}
		}
	}
}

func (s *Server) ServeWS(w http.ResponseWriter, r *http.Request) {
	clientID := r.Header.Get("X-Client-ID")
	if clientID == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols: []string{"pbs"},
	})
	if err != nil {
		return
	}

	client := &Client{
		ID:          clientID,
		conn:        conn,
		server:      s,
		rateLimiter: rate.NewLimiter(rate.Every(time.Millisecond*100), 10),
		send:        make(chan Message, broadcastBuffer),
		done:        make(chan struct{}),
	}

	s.register <- client

	// Start message handling in a separate goroutine
	go s.handleClientMessages(client)

	// Wait for client to be done
	<-client.done
	s.unregister <- client
}

func (c *Client) handleMessage() error {
	ctx, cancel := context.WithTimeout(c.server.ctx, time.Second*10)
	defer cancel()

	err := c.rateLimiter.Wait(ctx)
	if err != nil {
		return err
	}

	var message Message
	err = wsjson.Read(ctx, c.conn, &message)
	if err != nil {
		return err
	}

	message.ClientID = c.ID
	message.Time = time.Now()

	// Send to broadcast channel
	select {
	case c.server.broadcast <- message:
	default:
		log.Printf("Broadcast channel full, dropping message from client %s", c.ID)
	}

	return nil
}

func (c *Client) close() {
	c.once.Do(func() {
		close(c.done)
		close(c.send)
		c.conn.CloseNow()
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

	err := wsjson.Write(s.ctx, client.conn, &msg)
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) IsClientConnected(clientID string) bool {
	s.clientsMux.RLock()
	_, exists := s.clients[clientID]
	s.clientsMux.RUnlock()
	return exists
}
