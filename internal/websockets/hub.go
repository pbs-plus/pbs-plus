package websockets

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Error definitions
var (
	ErrClientNotFound    = errors.New("client not found")
	ErrInvalidMessage    = errors.New("invalid message")
	ErrConnectionClosed  = errors.New("connection closed")
	ErrBufferFull       = errors.New("message buffer full")
)

// Constants for connection management
const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 512 * 1024
)

type Message struct {
	ClientID string `json:"clientId"`
	Type     string `json:"type"` // e.g., "init", "command", "response"
	Content  string `json:"content"`
}

type Client struct {
	ID        string
	Conn      *websocket.Conn
	Broadcast BroadcastServer
	send      chan Message
	done      chan struct{}
}

type Server struct {
	clients  sync.Map // thread-safe map[string]*Client
	upgrader websocket.Upgrader
}

func NewServer() *Server {
	return &Server{
		upgrader: websocket.Upgrader{
			CheckOrigin:     func(r *http.Request) bool { return true },
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
		},
	}
}

func (s *Server) HandleClientConnection(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Error upgrading connection: %v", err)
		return
	}

	// Configure connection
	conn.SetReadLimit(maxMessageSize)
	
	var initMessage Message
	if err := conn.ReadJSON(&initMessage); err != nil || initMessage.Type != "init" {
		log.Printf("Invalid initialization message: %v", err)
		conn.Close()
		return
	}

	clientID := initMessage.Content
	msgs := make(chan Message, 256)
	
	client := &Client{
		ID:        clientID,
		Conn:      conn,
		Broadcast: NewBroadcastServer(r.Context(), msgs),
		send:      make(chan Message, 256),
		done:      make(chan struct{}),
	}

	s.clients.Store(clientID, client)
	log.Printf("Client connected: %s", clientID)

	// Start read and write pumps
	go s.writePump(client)
	go s.readPump(client, msgs)
}

func (s *Server) readPump(client *Client, msgs chan<- Message) {
	defer func() {
		s.RemoveClient(client.ID)
		close(client.done)
	}()

	client.Conn.SetReadDeadline(time.Now().Add(pongWait))
	client.Conn.SetPongHandler(func(string) error {
		client.Conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		var msg Message
		err := client.Conn.ReadJSON(&msg)
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("Error reading message from client %s: %v", client.ID, err)
			}
			return
		}

		msg.ClientID = client.ID
		msgs <- msg

		log.Printf("Received message from %s: Type=%s, Content=%s", client.ID, msg.Type, msg.Content)
	}
}

func (s *Server) writePump(client *Client) {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		client.Conn.Close()
	}()

	for {
		select {
		case message, ok := <-client.send:
			client.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				client.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			if err := client.Conn.WriteJSON(message); err != nil {
				log.Printf("Error writing message to client %s: %v", client.ID, err)
				return
			}

		case <-ticker.C:
			client.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := client.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}

		case <-client.done:
			return
		}
	}
}

func (s *Server) RemoveClient(clientID string) {
	if client, loaded := s.clients.LoadAndDelete(clientID); loaded {
		c := client.(*Client)
		log.Printf("Removing client: %s", clientID)
		close(c.send)
		c.Conn.Close()
	}
}

func (s *Server) SendCommand(clientID string, msg Message) error {
	clientVal, ok := s.clients.Load(clientID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrClientNotFound, clientID)
	}

	client := clientVal.(*Client)
	select {
	case client.send <- msg:
		return nil
	case <-client.done:
		return ErrConnectionClosed
	default:
		s.RemoveClient(clientID)
		return ErrBufferFull
	}
}

func (s *Server) SendCommandWithBroadcast(clientID string, msg Message) (BroadcastServer, error) {
	if err := s.SendCommand(clientID, msg); err != nil {
		return nil, err
	}

	clientVal, ok := s.clients.Load(clientID)
	if !ok {
		return nil, ErrClientNotFound
	}

	return clientVal.(*Client).Broadcast, nil
}

func (s *Server) BroadcastMessage(msg Message) {
	s.clients.Range(func(key, value interface{}) bool {
		client := value.(*Client)
		select {
		case client.send <- msg:
		default:
			s.RemoveClient(client.ID)
		}
		return true
	})
}