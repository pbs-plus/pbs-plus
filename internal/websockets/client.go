package websockets

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
)

const (
	maxSendBuffer       = 256
	messageTimeout      = 5 * time.Second
	operationTimeout    = 10 * time.Second
	rateLimit           = 100 * time.Millisecond
	maxMessageSize      = 1024 * 1024 // 1MB
	handlerPoolSize     = 100
	initialBackoff      = 1 * time.Second
	maxBackoff          = 30 * time.Second
	healthCheckInterval = 15 * time.Second
	writeDeadline       = 2 * time.Second
)

type (
	MessageHandler func(msg *Message)

	Config struct {
		ServerURL  string
		ClientID   string
		Headers    http.Header
		MaxRetries int
	}

	WSClient struct {
		clientID   string
		serverURL  string
		headers    http.Header
		tlsConfig  *tls.Config
		maxRetries int

		ctx        context.Context
		cancel     context.CancelFunc
		wg         sync.WaitGroup
		conn       *websocket.Conn
		connMu     sync.RWMutex
		sendChan   chan Message
		handlers   map[string]MessageHandler
		handlerMu  sync.RWMutex
		workerPool chan struct{}
		status     atomic.Uint32 // 0=disconnected, 1=connecting, 2=connected
		backoff    time.Duration
		statusMu   sync.RWMutex
		lastPong   atomic.Int64
	}
)

type ConnectionState int

const (
	StateDisconnected ConnectionState = iota
	StateConnecting
	StateConnected
)

func NewWSClient(ctx context.Context, config Config, tlsConfig *tls.Config) *WSClient {
	ctx, cancel := context.WithCancel(ctx)
	if config.MaxRetries <= 0 {
		config.MaxRetries = 10
	}

	return &WSClient{
		clientID:   config.ClientID,
		serverURL:  config.ServerURL,
		headers:    config.Headers,
		tlsConfig:  tlsConfig,
		maxRetries: config.MaxRetries,
		ctx:        ctx,
		cancel:     cancel,
		sendChan:   make(chan Message, maxSendBuffer),
		handlers:   make(map[string]MessageHandler),
		workerPool: make(chan struct{}, handlerPoolSize),
		backoff:    initialBackoff,
	}
}

func (c *WSClient) Start() {
	c.wg.Add(1)
	go c.connectionManager()
}

func (c *WSClient) connectionManager() {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			c.closeConnection()
			return
		default:
			if !c.tryConnect() {
				time.Sleep(c.nextBackoff())
				continue
			}

			c.wg.Add(2)
			connCtx, cancel := context.WithCancel(c.ctx)

			var once sync.Once
			closeHandler := func() {
				once.Do(func() {
					cancel()
					c.closeConnection()
					c.wg.Done()
					c.wg.Done()
				})
			}

			go func() {
				defer closeHandler()
				c.receiveLoop(connCtx)
			}()

			go func() {
				defer closeHandler()
				c.sendLoop(connCtx)
			}()

			go c.monitorConnection(connCtx)
			c.resetBackoff()
		}
	}
}

func (c *WSClient) tryConnect() bool {
	c.statusMu.Lock()
	c.status.Store(uint32(StateConnecting))
	c.statusMu.Unlock()

	defer func() {
		c.statusMu.Lock()
		c.status.Store(uint32(StateDisconnected))
		c.statusMu.Unlock()
	}()

	c.connMu.Lock()
	defer c.connMu.Unlock()

	ctx, cancel := context.WithTimeout(c.ctx, operationTimeout)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, c.serverURL, &websocket.DialOptions{
		Subprotocols: []string{"pbs"},
		HTTPHeader:   c.headers,
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: c.tlsConfig,
				IdleConnTimeout: 90 * time.Second,
			},
		},
	})

	if err != nil {
		syslog.L.Errorf("[WSClient.connect] Client %s: Connection failed - %v", c.clientID, err)
		return false
	}

	conn.SetReadLimit(maxMessageSize)
	c.conn = conn
	c.statusMu.Lock()
	c.status.Store(uint32(StateConnected))
	c.statusMu.Unlock()
	return true
}

func (c *WSClient) monitorConnection(ctx context.Context) {
	ticker := time.NewTicker(healthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if !c.ping() {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (c *WSClient) ping() bool {
	c.statusMu.Lock()
	defer c.statusMu.Unlock()

	if c.conn == nil {
		return false
	}

	ctx, cancel := context.WithTimeout(c.ctx, messageTimeout)
	defer cancel()

	// Send ping and wait for pong
	err := c.conn.Ping(ctx)
	if err != nil {
		syslog.L.Errorf("[WSClient.ping] Client %s: Ping failed - %v", c.clientID, err)
		c.status.Store(uint32(StateDisconnected))
		return false
	}

	// Update last pong time on successful ping
	c.lastPong.Store(time.Now().Unix())
	return true
}

func (c *WSClient) receiveLoop(ctx context.Context) {
	syslog.L.Infof("[WSClient.receiveLoop] Client %s: Starting receiver", c.clientID)

	defer func() {
		if r := recover(); r != nil {
			syslog.L.Errorf("[WSClient.receiveLoop] Client %s: Panic recovered - %v", c.clientID, r)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			var msg Message
			err := c.readWithTimeout(ctx, &msg)
			if err != nil {
				if websocket.CloseStatus(err) == websocket.StatusNormalClosure {
					return
				}
				syslog.L.Errorf("[WSClient.receiveLoop] Client %s: Read error - %v", c.clientID, err)
				return
			}

			c.handleMessage(msg)
		}
	}
}

func (c *WSClient) readWithTimeout(ctx context.Context, msg *Message) error {
	ctx, cancel := context.WithTimeout(ctx, healthCheckInterval)
	defer cancel()
	return wsjson.Read(ctx, c.conn, msg)
}

func (c *WSClient) handleMessage(msg Message) {
	c.handlerMu.RLock()
	handler, exists := c.handlers[msg.Type]
	c.handlerMu.RUnlock()

	if !exists {
		syslog.L.Warnf("[WSClient.handleMessage] Client %s: No handler for type %s",
			c.clientID, msg.Type)
		return
	}

	select {
	case c.workerPool <- struct{}{}:
		go func() {
			defer func() {
				<-c.workerPool
				if r := recover(); r != nil {
					syslog.L.Errorf("[WSClient.handleMessage] Client %s: Handler panic - %v",
						c.clientID, r)
				}
			}()
			handler(&msg)
		}()
	default:
		syslog.L.Warnf("[WSClient.handleMessage] Client %s: Worker pool full, message dropped",
			c.clientID)
	}
}

func (c *WSClient) sendLoop(ctx context.Context) {
	syslog.L.Infof("[WSClient.sendLoop] Client %s: Starting sender", c.clientID)
	defer func() {
		if r := recover(); r != nil {
			syslog.L.Errorf("[WSClient.sendLoop] Client %s: Panic recovered - %v", c.clientID, r)
		}
	}()

	var backoff time.Duration

	for {
		select {
		case <-ctx.Done():
			c.drainSendQueue()
			return
		case msg := <-c.sendChan:
			// Attempt to send the message once
			err := c.writeMessage(msg)
			if err != nil {
				syslog.L.Warnf("[WSClient.sendLoop] Client %s: Send failed - %v (backoff %v)", c.clientID, err, backoff)
				// Calculate next backoff with jitter
				backoff = min(maxBackoff, backoff*2+time.Duration(rand.Int63n(int64(initialBackoff))))
				if backoff == 0 {
					backoff = initialBackoff
				}
				// Requeue the message at the front to retry
				select {
				case c.sendChan <- msg: // Requeue the message
				default: // If the buffer is full, discard the oldest message
					<-c.sendChan
					c.sendChan <- msg
				}
				// Wait for backoff before next attempt
				select {
				case <-time.After(backoff):
				case <-ctx.Done():
					c.drainSendQueue()
					return
				}
			} else {
				// Reset backoff on success
				backoff = 0
			}
		}
	}
}

func (c *WSClient) writeMessage(msg Message) error {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c.conn == nil {
		return fmt.Errorf("no active connection")
	}

	ctx, cancel := context.WithTimeout(c.ctx, messageTimeout)
	defer cancel()

	return wsjson.Write(ctx, c.conn, msg)
}

func (c *WSClient) drainSendQueue() {
	for {
		select {
		case msg := <-c.sendChan:
			syslog.L.Warnf("[WSClient.drainSendQueue] Client %s: Discarding message %s", c.clientID, msg.Type)
		default:
			return
		}
	}
}

func (c *WSClient) nextBackoff() time.Duration {
	c.backoff = min(maxBackoff, c.backoff*2)
	jitter := time.Duration(rand.Int63n(int64(c.backoff / 2)))
	return c.backoff + jitter
}

func (c *WSClient) resetBackoff() {
	c.backoff = initialBackoff
}

func (c *WSClient) closeConnection() {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c.conn != nil {
		c.conn.Close(websocket.StatusNormalClosure, "normal closure")
		c.conn = nil
	}
}

func (c *WSClient) Send(msg Message) error {
	if c.status.Load() != uint32(StateConnected) {
		return fmt.Errorf("connection not ready")
	}

	select {
	case c.sendChan <- msg:
		return nil
	case <-time.After(rateLimit):
		return fmt.Errorf("send queue full")
	case <-c.ctx.Done():
		return fmt.Errorf("client shutdown")
	}
}

func (c *WSClient) RegisterHandler(msgType string, handler MessageHandler) {
	c.handlerMu.Lock()
	defer c.handlerMu.Unlock()
	c.handlers[msgType] = handler
}

func (c *WSClient) Close() error {
	c.cancel()
	c.wg.Wait()
	c.closeConnection()
	return nil
}

func (c *WSClient) GetConnectionStatus() ConnectionState {
	c.statusMu.RLock()
	defer c.statusMu.RUnlock()

	// Check if we've missed pong responses
	if c.status.Load() == uint32(StateConnected) {
		lastPongTime := time.Unix(c.lastPong.Load(), 0)
		if time.Since(lastPongTime) > 2*healthCheckInterval {
			return StateDisconnected
		}
	}

	return ConnectionState(c.status.Load())
}
