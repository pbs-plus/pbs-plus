package websockets

import (
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

type Message struct {
	ClientID string
	Type     string `json:"type"` // e.g., "command", "response"
	Content  string `json:"content"`
}

type Client struct {
	ID        string
	Conn      *websocket.Conn
	Broadcast BroadcastServer
}

type Server struct {
	Clients    map[string]*Client
	ClientsMux sync.Mutex
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins (adjust for security in production)
	},
}

func NewServer() *Server {
	return &Server{
		Clients: make(map[string]*Client),
	}
}

func (s *Server) HandleClientConnection(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Error upgrading connection: %v", err)
		return
	}
	defer conn.Close()

	var initMessage Message
	err = conn.ReadJSON(&initMessage)
	if err != nil || initMessage.Type != "init" {
		log.Printf("Invalid initialization message: %v", err)
		return
	}

	msgs := make(chan Message)

	broadcastServer := NewBroadcastServer(r.Context(), msgs)
	defer broadcastServer.Close()

	clientID := initMessage.Content
	s.ClientsMux.Lock()
	s.Clients[clientID] = &Client{ID: clientID, Conn: conn, Broadcast: broadcastServer}
	s.ClientsMux.Unlock()

	log.Printf("Client connected: %s", clientID)

	for {
		var msg Message
		err := conn.ReadJSON(&msg)
		if err != nil {
			log.Printf("Client %s disconnected: %v", clientID, err)
			s.ClientsMux.Lock()
			delete(s.Clients, clientID)
			s.ClientsMux.Unlock()
			return
		}

		msg.ClientID = clientID
		msgs <- msg

		log.Printf("Received message from %s: Type=%s, Content=%s", clientID, msg.Type, msg.Content)
	}
}

func (s *Server) RemoveClient(clientID string) {
	s.ClientsMux.Lock()
	defer s.ClientsMux.Unlock()

	if _, exists := s.Clients[clientID]; exists {
		log.Printf("Removing client: %s", clientID)

		delete(s.Clients, clientID)
	}
}

func (s *Server) SendCommand(clientID string, msg Message) error {
	s.ClientsMux.Lock()
	client, exists := s.Clients[clientID]
	s.ClientsMux.Unlock()

	if !exists {
		return fmt.Errorf("client %s not connected", clientID)
	}

	err := client.Conn.WriteJSON(msg)
	if err != nil {
		log.Printf("Failed to send command to client %s: %v", clientID, err)
		s.RemoveClient(clientID)
		return err
	}

	return nil
}

func (s *Server) SendCommandWithBroadcast(clientID string, msg Message) (BroadcastServer, error) {
	err := s.SendCommand(clientID, msg)
	if err != nil {
		return nil, err
	}

	return s.Clients[clientID].Broadcast, nil
}
