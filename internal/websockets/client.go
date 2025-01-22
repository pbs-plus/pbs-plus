package websockets

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"golang.org/x/time/rate"
)

const (
	maxSendBuffer    = 256
	maxRetryAttempts = 10
	messageTimeout   = 5 * time.Second
	operationTimeout = 10 * time.Second
	rateLimit        = 100 * time.Millisecond
	rateBurst        = 10
	maxMessageSize   = 1024 * 1024 // 1MB
)

type (
	MessageHandler func(msg *Message)

	Config struct {
		ServerURL string
		ClientID  string
		Headers   http.Header
		rootCAs   *x509.CertPool
		cert      tls.Certificate
	}

	WSClient struct {
		ClientID  string
		serverURL string
		headers   http.Header

		cert    tls.Certificate
		rootCAs *x509.CertPool

		readLimiter  *rate.Limiter
		writeLimiter *rate.Limiter

		ctx    context.Context
		cancel context.CancelFunc
		wg     sync.WaitGroup

		conn        *websocket.Conn
		connMu      sync.RWMutex
		send        chan Message
		IsConnected bool

		handlers  map[string]MessageHandler
		handlerMu sync.RWMutex
	}
)

func NewWSClient(ctx context.Context, config Config) (*WSClient, error) {
	ctx, cancel := context.WithCancel(ctx)

	client := &WSClient{
		ClientID:     config.ClientID,
		serverURL:    config.ServerURL,
		headers:      config.Headers,
		ctx:          ctx,
		cancel:       cancel,
		readLimiter:  rate.NewLimiter(rate.Every(rateLimit), rateBurst),
		writeLimiter: rate.NewLimiter(rate.Every(rateLimit), rateBurst),
		send:         make(chan Message, maxSendBuffer),
		handlers:     make(map[string]MessageHandler),
		rootCAs:      config.rootCAs,
		cert:         config.cert,
	}

	syslog.L.Infof("[WSClient.New] Client %s: Initialized new WebSocket client for server %s",
		client.ClientID, client.serverURL)

	return client, nil
}

func (c *WSClient) Connect() error {
	timeoutCtx, cancel := context.WithTimeout(c.ctx, operationTimeout)
	defer cancel()

	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c.IsConnected {
		return nil
	}

	syslog.L.Infof("[WSClient.Connect] Client %s: Attempting connection to %s",
		c.ClientID, c.serverURL)

	conn, _, err := websocket.Dial(timeoutCtx, c.serverURL, &websocket.DialOptions{
		Subprotocols: []string{"pbs"},
		HTTPHeader:   c.headers,
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				IdleConnTimeout: 90 * time.Second,
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
			},
		},
	})
	if err != nil {
		syslog.L.Errorf("[WSClient.Connect] Client %s: Connection failed - %v",
			c.ClientID, err)
		return fmt.Errorf("dial failed: %w", err)
	}

	c.conn = conn
	c.IsConnected = true
	syslog.L.Infof("[WSClient.Connect] Client %s: Connection established successfully",
		c.ClientID)

	return nil
}

func (c *WSClient) Close() error {
	timeoutCtx, cancel := context.WithTimeout(c.ctx, operationTimeout)
	defer cancel()

	c.connMu.Lock()
	defer c.connMu.Unlock()

	if !c.IsConnected {
		return nil
	}

	syslog.L.Infof("[WSClient.Close] Client %s: Initiating client shutdown", c.ClientID)
	c.cancel()
	c.IsConnected = false

	if c.conn != nil {
		done := make(chan error, 1)
		go func() {
			done <- c.conn.Close(websocket.StatusNormalClosure, "client closing")
		}()

		select {
		case err := <-done:
			if err != nil {
				syslog.L.Errorf("[WSClient.Close] Client %s: Error closing connection - %v",
					c.ClientID, err)
				return err
			}
		case <-timeoutCtx.Done():
			return fmt.Errorf("connection close timed out")
		}

		syslog.L.Infof("[WSClient.Close] Client %s: Connection closed successfully",
			c.ClientID)
	}
	return nil
}

func (c *WSClient) handleConnectionLoss() {
	syslog.L.Infof("[WSClient.ConnectionHandler] Client %s: Connection lost, initiating reconnection",
		c.ClientID)

	c.connMu.Lock()
	c.IsConnected = false
	c.connMu.Unlock()

	for attempt := 0; attempt < maxRetryAttempts; attempt++ {
		select {
		case <-c.ctx.Done():
			syslog.L.Infof("[WSClient.ConnectionHandler] Client %s: Shutdown requested, stopping reconnection",
				c.ClientID)
			return
		case <-time.After(calculateBackoff(attempt)):
			syslog.L.Infof("[WSClient.ConnectionHandler] Client %s: Reconnection attempt %d of %d",
				c.ClientID, attempt+1, maxRetryAttempts)

			if err := c.Connect(); err == nil {
				syslog.L.Infof("[WSClient.ConnectionHandler] Client %s: Reconnection successful",
					c.ClientID)
				return
			} else {
				syslog.L.Errorf("[WSClient.ConnectionHandler] Client %s: Reconnection attempt %d failed - %v",
					c.ClientID, attempt+1, err)
			}
		}
	}

	syslog.L.Errorf("[WSClient.ConnectionHandler] Client %s: Max reconnection attempts reached, giving up",
		c.ClientID)
}

func (c *WSClient) Send(msg Message) error {
	timeoutCtx, cancel := context.WithTimeout(c.ctx, operationTimeout)
	defer cancel()

	syslog.L.Infof("[WSClient.Send] Client %s: Queueing message type '%s'",
		c.ClientID, msg.Type)

	select {
	case c.send <- msg:
		syslog.L.Infof("[WSClient.Send] Client %s: Message type '%s' queued successfully",
			c.ClientID, msg.Type)
		return nil
	case <-timeoutCtx.Done():
		err := fmt.Errorf("send operation timed out")
		syslog.L.Errorf("[WSClient.Send] Client %s: Failed to queue message type '%s' - %v",
			c.ClientID, msg.Type, err)
		return err
	}
}

func (c *WSClient) Start() {
	syslog.L.Infof("[WSClient.Start] Client %s: Starting client", c.ClientID)

	// Initial connection
	if err := c.Connect(); err != nil {
		syslog.L.Errorf("[WSClient.Start] Client %s: Initial connection failed - %v",
			c.ClientID, err)
		return
	}

	receiveCtx, receiveCancel := context.WithCancel(c.ctx)
	sendCtx, sendCancel := context.WithCancel(c.ctx)

	c.wg.Add(2)

	// Start receive loop
	go func() {
		defer c.wg.Done()
		defer receiveCancel()
		c.receiveLoop(receiveCtx)
	}()

	// Start send loop
	go func() {
		defer c.wg.Done()
		defer sendCancel()
		c.sendLoop(sendCtx)
	}()

	// Start supervisor
	go c.superviseLoops(receiveCtx, receiveCancel, sendCtx, sendCancel)

	syslog.L.Infof("[WSClient.Start] Client %s: Client started successfully", c.ClientID)
}

func (c *WSClient) superviseLoops(receiveCtx context.Context, receiveCancel context.CancelFunc,
	sendCtx context.Context, sendCancel context.CancelFunc) {

	for {
		select {
		case <-c.ctx.Done():
			syslog.L.Infof("[WSClient.Supervisor] Client %s: Main context cancelled, shutting down",
				c.ClientID)
			c.connMu.Lock()
			c.IsConnected = false
			c.connMu.Unlock()
			return

		case <-receiveCtx.Done():
			if c.ctx.Err() != nil {
				return // Don't restart if main context is cancelled
			}
			syslog.L.Infof("[WSClient.Supervisor] Client %s: Restarting receive loop", c.ClientID)
			receiveCtx, receiveCancel = context.WithCancel(c.ctx)
			c.wg.Add(1)
			go func() {
				defer c.wg.Done()
				defer receiveCancel()
				c.receiveLoop(receiveCtx)
			}()

		case <-sendCtx.Done():
			if c.ctx.Err() != nil {
				return // Don't restart if main context is cancelled
			}
			syslog.L.Infof("[WSClient.Supervisor] Client %s: Restarting send loop", c.ClientID)
			sendCtx, sendCancel = context.WithCancel(c.ctx)
			c.wg.Add(1)
			go func() {
				defer c.wg.Done()
				defer sendCancel()
				c.sendLoop(sendCtx)
			}()
		}
	}
}

func (c *WSClient) receiveLoop(ctx context.Context) {
	syslog.L.Infof("[WSClient.ReceiveLoop] Client %s: Starting receive loop", c.ClientID)

	for {
		select {
		case <-ctx.Done():
			syslog.L.Infof("[WSClient.ReceiveLoop] Client %s: Receive loop context cancelled",
				c.ClientID)
			return
		default:
			// Process message with rate limiting
			if err := c.readLimiter.Wait(ctx); err != nil {
				syslog.L.Warnf("[WSClient.ReceiveLoop] Client %s: Rate limit exceeded, skipping message",
					c.ClientID)
				continue
			}

			message := Message{}

			err := wsjson.Read(ctx, c.conn, &message)
			if err != nil {
				if websocket.CloseStatus(err) != websocket.StatusNormalClosure {
					syslog.L.Errorf("[WSClient.ReceiveLoop] Client %s: Message read error - %v",
						c.ClientID, err)
				}
				c.handleConnectionLoss()
				return
			}

			syslog.L.Infof("[WSClient.ReceiveLoop] Client %s: Received message type '%s'",
				c.ClientID, message.Type)

			c.handlerMu.RLock()
			handler, exists := c.handlers[message.Type]
			c.handlerMu.RUnlock()

			if exists {
				go func() {
					handler(&message)
				}()
			} else {
				syslog.L.Warnf("[WSClient.ReceiveLoop] Client %s: No handler registered for message type '%s'",
					c.ClientID, message.Type)
			}
		}
	}
}

func (c *WSClient) sendLoop(ctx context.Context) {
	syslog.L.Infof("[WSClient.SendLoop] Client %s: Starting send loop", c.ClientID)

	for {
		select {
		case <-ctx.Done():
			syslog.L.Infof("[WSClient.SendLoop] Client %s: Send loop context cancelled",
				c.ClientID)
			return

		case msg := <-c.send:
			syslog.L.Infof("[WSClient.SendLoop] Client %s: Processing message type '%s' for sending",
				c.ClientID, msg.Type)

			messageCtx, cancel := context.WithTimeout(ctx, messageTimeout)
			if err := c.writeMessage(messageCtx, msg); err != nil {
				cancel()
				syslog.L.Errorf("[WSClient.SendLoop] Client %s: Failed to send message type '%s' - %v",
					c.ClientID, msg.Type, err)
				c.handleConnectionLoss()
				return
			}
			cancel()

			syslog.L.Infof("[WSClient.SendLoop] Client %s: Message type '%s' sent successfully",
				c.ClientID, msg.Type)
		}
	}
}

func (c *WSClient) writeMessage(ctx context.Context, msg Message) error {
	if err := c.writeLimiter.Wait(ctx); err != nil {
		return fmt.Errorf("rate limit exceeded: %w", err)
	}

	return wsjson.Write(ctx, c.conn, &msg)
}

func (c *WSClient) RegisterHandler(msgType string, handler MessageHandler) {
	c.handlerMu.Lock()
	c.handlers[msgType] = handler
	c.handlerMu.Unlock()

	syslog.L.Infof("[WSClient.RegisterHandler] Client %s: Handler registered for message type '%s'",
		c.ClientID, msgType)
}

func (c *WSClient) GetConnectionStatus() bool {
	c.connMu.RLock()
	defer c.connMu.RUnlock()
	return c.IsConnected
}
