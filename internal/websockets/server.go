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
	handlerBuffer  = 256
	handlerTimeout = 1 * time.Second
	closeTimeout   = 5 * time.Second
)

type Message struct {
	ClientID string    `json:"client_id"`
	Type     string    `json:"type"`
	Content  string    `json:"content"`
	Time     time.Time `json:"time"`
}

type Client struct {
	ID     string
	conn   *websocket.Conn
	server *Server
	send   chan Message
	done   chan struct{}
	once   sync.Once
}

type Server struct {
	clients    map[string]*Client
	clientsMux sync.RWMutex

	handlers    map[chan Message]struct{}
	handlersMux sync.RWMutex

	register   chan *Client
	unregister chan *Client
	handler    chan Message
	done       chan struct{}
	ctx        context.Context
	cancel     context.CancelFunc
}

func NewServer(ctx context.Context) *Server {
	ctx, cancel := context.WithCancel(ctx)
	server := &Server{
		clients:    make(map[string]*Client),
		handlers:   make(map[chan Message]struct{}),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		handler:    make(chan Message, handlerBuffer),
		done:       make(chan struct{}),
		ctx:        ctx,
		cancel:     cancel,
	}

	syslog.L.Infof("[WSServer.New] WebSocket server initialized")
	return server
}

func (s *Server) RegisterHandler() (<-chan Message, func()) {
	ch := make(chan Message, handlerBuffer)

	s.handlersMux.Lock()
	s.handlers[ch] = struct{}{}
	s.handlersMux.Unlock()

	syslog.L.Infof("[WSServer.RegisterHandler] New handler registered")

	cleanup := func() {
		s.handlersMux.Lock()
		if _, exists := s.handlers[ch]; exists {
			delete(s.handlers, ch)
			close(ch)
			syslog.L.Infof("[WSServer.RegisterHandler] Handler unregistered and channel closed")
		}
		s.handlersMux.Unlock()
	}

	return ch, cleanup
}

func (s *Server) Run() {
	syslog.L.Infof("[WSServer.Run] Server starting")
	defer func() {
		syslog.L.Infof("[WSServer.Run] Server initiating shutdown sequence")

		s.clientsMux.Lock()
		clientCount := len(s.clients)
		clients := make([]*Client, 0, clientCount)
		for _, client := range s.clients {
			clients = append(clients, client)
		}
		clear(s.clients)
		s.clientsMux.Unlock()

		var wg sync.WaitGroup
		for _, client := range clients {
			wg.Add(1)
			go func(c *Client) {
				defer wg.Done()
				syslog.L.Infof("[WSServer.Run] Closing connection for client %s", c.ID)
				c.close()
			}(client)
		}
		wg.Wait()
		syslog.L.Infof("[WSServer.Run] Closed %d client connections", clientCount)

		s.handlersMux.Lock()
		subCount := len(s.handlers)
		for ch := range s.handlers {
			close(ch)
		}
		clear(s.handlers)
		s.handlersMux.Unlock()
		syslog.L.Infof("[WSServer.Run] Closed %d handler channels", subCount)

		close(s.done)
		syslog.L.Infof("[WSServer.Run] Server shutdown complete")
	}()

	for {
		select {
		case <-s.ctx.Done():
			return

		case client := <-s.register:
			s.clientsMux.Lock()
			s.clients[client.ID] = client
			clientCount := len(s.clients)
			s.clientsMux.Unlock()
			syslog.L.Infof("[WSServer.Run] Client %s registered (total clients: %d)",
				client.ID, clientCount)

		case client := <-s.unregister:
			s.clientsMux.Lock()
			if _, ok := s.clients[client.ID]; ok {
				delete(s.clients, client.ID)
				syslog.L.Infof("[WSServer.Run] Client %s unregistered (remaining clients: %d)",
					client.ID, len(s.clients))
			}
			s.clientsMux.Unlock()

			// Close client connection asynchronously
			go client.close()

		case msg := <-s.handler:
			s.handlersMux.RLock()
			handlers := make([]chan Message, 0, len(s.handlers))
			for ch := range s.handlers {
				handlers = append(handlers, ch)
			}
			s.handlersMux.RUnlock()

			var wg sync.WaitGroup
			for _, ch := range handlers {
				wg.Add(1)
				go func(c chan Message) {
					defer wg.Done()
					ctx, cancel := context.WithTimeout(s.ctx, handlerTimeout)
					defer cancel()

					select {
					case c <- msg:
					case <-ctx.Done():
						syslog.L.Warnf("[WSServer.Run] Receive timeout for message from client %s",
							msg.ClientID)
					default:
						syslog.L.Warnf("[WSServer.Run] Receive channel full, message from client %s dropped",
							msg.ClientID)
					}
				}(ch)
			}
			wg.Wait()
			syslog.L.Infof("[WSServer.Run] Message (%s) from %s successfully received", msg.Type, msg.ClientID)
		}
	}
}

func (s *Server) handleClientMessages(client *Client) {
	syslog.L.Infof("[WSServer.MessageHandler] Starting message handling for client %s", client.ID)

	for {
		select {
		case <-s.ctx.Done():
			syslog.L.Infof("[WSServer.MessageHandler] Stopping message handling for client %s (server shutdown)",
				client.ID)
			return
		case <-client.done:
			syslog.L.Infof("[WSServer.MessageHandler] Stopping message handling for client %s (client disconnected)",
				client.ID)
			return
		default:
			message := Message{}

			// Add timeout for message reading
			readCtx, cancel := context.WithTimeout(s.ctx, messageTimeout)
			err := wsjson.Read(readCtx, client.conn, &message)
			cancel()

			if err != nil {
				if strings.Contains(err.Error(), "failed to read frame header: EOF") {
					continue
				}

				if websocket.CloseStatus(err) != websocket.StatusNormalClosure {
					syslog.L.Errorf("[WSServer.MessageHandler] Read error for client %s: %v",
						client.ID, err)
				}
				return
			}

			message.ClientID = client.ID
			message.Time = time.Now()

			handlerCtx, cancel := context.WithTimeout(s.ctx, handlerTimeout)
			select {
			case s.handler <- message:
				syslog.L.Infof("[WSServer.MessageHandler] Message from client %s queued for receive handler",
					client.ID)
			case <-handlerCtx.Done():
				syslog.L.Errorf("[WSServer.MessageHandler] Receive handler timeout for message from client %s",
					client.ID)
			}
			cancel()
		}
	}
}

func (s *Server) ServeWS(w http.ResponseWriter, r *http.Request) {
	clientID := r.Header.Get("X-PBS-Agent")
	if clientID == "" {
		syslog.L.Errorf("[WSServer.ServeWS] Connection rejected: missing X-PBS-Agent header")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols: []string{"pbs"},
	})
	if err != nil {
		syslog.L.Errorf("[WSServer.ServeWS] Connection acceptance failed for client %s: %v",
			clientID, err)
		return
	}

	syslog.L.Infof("[WSServer.ServeWS] New connection accepted for client %s", clientID)

	client := &Client{
		ID:     clientID,
		conn:   conn,
		server: s,
		send:   make(chan Message, handlerBuffer),
		done:   make(chan struct{}),
	}

	registerCtx, cancel := context.WithTimeout(s.ctx, operationTimeout)
	select {
	case s.register <- client:
		syslog.L.Infof("[WSServer.ServeWS] Client %s registered successfully", clientID)
	case <-registerCtx.Done():
		syslog.L.Errorf("[WSServer.ServeWS] Registration timeout for client %s", clientID)
		conn.Close(websocket.StatusInternalError, "registration timeout")
		cancel()
		return
	}
	cancel()

	go s.handleClientMessages(client)

	<-client.done

	unregisterCtx, cancel := context.WithTimeout(s.ctx, operationTimeout)
	select {
	case s.unregister <- client:
		syslog.L.Infof("[WSServer.ServeWS] Client %s unregistered successfully", clientID)
	case <-unregisterCtx.Done():
		syslog.L.Errorf("[WSServer.ServeWS] Unregistration timeout for client %s", clientID)
	}
	cancel()
}

func (c *Client) close() {
	c.once.Do(func() {
		syslog.L.Infof("[WSServer.Client] Closing connection for client %s", c.ID)
		close(c.done)

		err := c.conn.Close(websocket.StatusNormalClosure, "client disconnecting")
		if err != nil {
			syslog.L.Errorf("[WSServer.Client] Error closing connection for client %s: %v",
				c.ID, err)
		}

		// Close send channel after connection is closed
		close(c.send)
	})
}

func (s *Server) SendToClient(clientID string, msg Message) error {
	s.clientsMux.RLock()
	client, exists := s.clients[clientID]
	s.clientsMux.RUnlock()

	if !exists {
		return fmt.Errorf("client %s not connected", clientID)
	}

	syslog.L.Infof("[WSServer.SendToClient] Sending message to client %s", clientID)

	ctx, cancel := context.WithTimeout(s.ctx, messageTimeout)
	defer cancel()

	if err := wsjson.Write(ctx, client.conn, &msg); err != nil {
		syslog.L.Errorf("[WSServer.SendToClient] Failed to send message to client %s: %v",
			clientID, err)
		return fmt.Errorf("failed to send message: %w", err)
	}

	syslog.L.Infof("[WSServer.SendToClient] Message sent successfully to client %s", clientID)
	return nil
}

func (s *Server) IsClientConnected(clientID string) bool {
	s.clientsMux.RLock()
	_, exists := s.clients[clientID]
	s.clientsMux.RUnlock()
	return exists
}
