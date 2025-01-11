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

type Message struct {
	ClientID string
	Type     string `json:"type"`
	Content  string `json:"content"`
}

type Client struct {
	ID        string
	Conn      *websocket.Conn
	send      chan Message
	broadcast BroadcastServer
	done      chan struct{}
	mutex     sync.Mutex
}

type Server struct {
	Clients    map[string]*Client
	ClientsMux sync.RWMutex
	// Add broadcast related fields
	broadcastChan chan Message
	Broadcast     BroadcastServer
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

func NewServer() *Server {
	s := &Server{
		Clients:       make(map[string]*Client),
		broadcastChan: make(chan Message, 100),
	}
	// Initialize the broadcast server
	s.Broadcast = NewBroadcastServer(context.Background())
	return s
}

func NewClient(id string, conn *websocket.Conn, broadcast BroadcastServer) *Client {
	return &Client{
		ID:        id,
		Conn:      conn,
		send:      make(chan Message, 256),
		broadcast: broadcast,
		done:      make(chan struct{}),
	}
}

func (s *Server) HandleClientConnection(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Error upgrading connection: %v", err)
		return
	}

	var initMessage Message
	err = conn.ReadJSON(&initMessage)
	if err != nil || initMessage.Type != "init" {
		log.Printf("Invalid initialization message: %v", err)
		conn.Close()
		return
	}

	clientID := initMessage.Content
	client := NewClient(clientID, conn, s.Broadcast)

	// Subscribe to broadcasts
	subscription := s.Broadcast.Subscribe()

	s.ClientsMux.Lock()
	s.Clients[clientID] = client
	s.ClientsMux.Unlock()

	// Start broadcast handler
	go func() {
		defer s.Broadcast.CancelSubscription(subscription)
		for {
			select {
			case msg := <-subscription:
				select {
				case client.send <- msg:
					// Message queued successfully
				default:
					// Client's buffer is full, consider disconnecting
					log.Printf("Client %s message buffer full, dropping message", clientID)
				}
			case <-client.done:
				return
			}
		}
	}()

	// Start the read/write pumps
	go client.readPump(s)
	go client.writePump()

	log.Printf("Client connected: %s", clientID)
}

func (c *Client) readPump(s *Server) {
	defer func() {
		close(c.done)
		s.RemoveClient(c.ID)
		c.Conn.Close()
	}()

	c.Conn.SetReadLimit(512 * 1024)
	c.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.Conn.SetPongHandler(func(string) error {
		c.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		var msg Message
		err := c.Conn.ReadJSON(&msg)
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("error reading message: %v", err)
			}
			return
		}

		msg.ClientID = c.ID

		// Send to broadcast server
		if err := c.broadcast.Broadcast(msg); err != nil {
			log.Printf("Error broadcasting message: %v", err)
		}

		// Handle direct messages or other types
		log.Printf("Received message from %s: %s", c.ID, msg.Content)
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(54 * time.Second)
	defer func() {
		ticker.Stop()
		c.Conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			c.mutex.Lock()
			err := c.Conn.WriteJSON(message)
			c.mutex.Unlock()

			if err != nil {
				return
			}

		case <-ticker.C:
			c.mutex.Lock()
			err := c.Conn.WriteMessage(websocket.PingMessage, nil)
			c.mutex.Unlock()

			if err != nil {
				return
			}

		case <-c.done:
			return
		}
	}
}

func (s *Server) RemoveClient(clientID string) {
	s.ClientsMux.Lock()
	if client, ok := s.Clients[clientID]; ok {
		delete(s.Clients, clientID)
		close(client.send)
	}
	s.ClientsMux.Unlock()
}

func (s *Server) SendCommand(clientID string, msg Message) error {
	s.ClientsMux.RLock()
	client, exists := s.Clients[clientID]
	s.ClientsMux.RUnlock()

	if !exists {
		return fmt.Errorf("client %s not connected", clientID)
	}

	select {
	case client.send <- msg:
		return nil
	case <-time.After(time.Second):
		return fmt.Errorf("send timeout for client %s", clientID)
	}
}
