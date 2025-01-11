package websockets

import (
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
	Broadcast BroadcastServer
	send      chan Message  // Buffered channel for outbound messages
	done      chan struct{} // Channel to signal connection closure
	mutex     sync.Mutex    // Mutex for conn operations
}

type Server struct {
	Clients    map[string]*Client
	ClientsMux sync.RWMutex // Using RWMutex for better concurrency
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

func NewServer() *Server {
	return &Server{
		Clients: make(map[string]*Client),
	}
}

func NewClient(id string, conn *websocket.Conn, broadcast BroadcastServer) *Client {
	return &Client{
		ID:        id,
		Conn:      conn,
		Broadcast: broadcast,
		send:      make(chan Message, 256), // Buffered channel to prevent blocking
		done:      make(chan struct{}),
	}
}

func (c *Client) readPump(msgs chan Message) {
	defer func() {
		close(c.done)
		c.Conn.Close()
	}()

	c.Conn.SetReadLimit(512 * 1024) // 512KB max message size
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
		msgs <- msg

		log.Printf("Received message from %s: Type=%s, Content=%s", msg.ClientID, msg.Type, msg.Content)
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(54 * time.Second) // Send pings every 54 seconds
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
				log.Printf("error writing message: %v", err)
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

	msgs := make(chan Message)
	broadcastServer := NewBroadcastServer(r.Context(), msgs)

	clientID := initMessage.Content
	client := NewClient(clientID, conn, broadcastServer)

	s.ClientsMux.Lock()
	s.Clients[clientID] = client
	s.ClientsMux.Unlock()

	go client.readPump(msgs)
	go client.writePump()

	log.Printf("Client connected: %s", clientID)
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

func (s *Server) SendCommandWithBroadcast(clientID string, msg Message) (BroadcastServer, error) {
	err := s.SendCommand(clientID, msg)
	if err != nil {
		return nil, err
	}

	s.ClientsMux.RLock()
	broadcastServer := s.Clients[clientID].Broadcast
	s.ClientsMux.RUnlock()

	return broadcastServer, nil
}
