package websockets

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
)

const (
	maxRetryAttempts = 100
	messageTimeout   = 5 * time.Second
	operationTimeout = 10 * time.Second
	maxMessageSize   = 1024 * 1024 // 1MB
	handlerPoolSize  = 100         // Max concurrent message handlers
)

type (
	MessageHandler func(ctx context.Context, msg *Message) error

	Config struct {
		ServerURL string
		ClientID  string
		Headers   http.Header
		TLSConfig *tls.Config
	}

	WSClient struct {
		config Config

		// Connection management
		conn        *websocket.Conn
		connMu      sync.RWMutex
		isConnected atomic.Bool

		// Message handling
		handlers   map[string]MessageHandler
		handlerMu  sync.RWMutex
		workerPool *WorkerPool

		// State management
		ctx       context.Context
		cancel    context.CancelFunc
		closeOnce sync.Once
	}

	WorkerPool struct {
		workers chan struct{}
		wg      sync.WaitGroup
	}
)

func NewWorkerPool(size int) *WorkerPool {
	return &WorkerPool{
		workers: make(chan struct{}, size),
	}
}

func (p *WorkerPool) Submit(ctx context.Context, task func()) error {
	select {
	case p.workers <- struct{}{}:
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			defer func() { <-p.workers }()
			task()
		}()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return fmt.Errorf("worker pool full")
	}
}

func (p *WorkerPool) Wait() {
	p.wg.Wait()
}

func NewWSClient(ctx context.Context, config Config) (*WSClient, error) {
	ctx, cancel := context.WithCancel(ctx)

	client := &WSClient{
		config:     config,
		handlers:   make(map[string]MessageHandler),
		workerPool: NewWorkerPool(handlerPoolSize),
		ctx:        ctx,
		cancel:     cancel,
	}

	syslog.L.Infof("Initializing WebSocket client | server=%s client_id=%s",
		client.config.ServerURL, client.config.ClientID)
	return client, nil
}

func (c *WSClient) Connect(ctx context.Context) error {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c.isConnected.Load() {
		syslog.L.Infof("Connection attempt skipped - already connected | client_id=%s", c.config.ClientID)
		return nil
	}

	syslog.L.Infof("Attempting WebSocket connection | server=%s client_id=%s",
		c.config.ServerURL, c.config.ClientID)

	conn, _, err := websocket.Dial(ctx, c.config.ServerURL, &websocket.DialOptions{
		Subprotocols: []string{"pbs"},
		HTTPHeader:   c.config.Headers,
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: c.config.TLSConfig,
			},
		},
	})

	if err != nil {
		syslog.L.Errorf("WebSocket connection failed | server=%s client_id=%s error=%v",
			c.config.ServerURL, c.config.ClientID, err)
		return fmt.Errorf("dial failed: %w", err)
	}

	c.conn = conn
	c.isConnected.Store(true)
	syslog.L.Infof("WebSocket connection established | server=%s client_id=%s",
		c.config.ServerURL, c.config.ClientID)

	// Start message handler in background
	go c.handleMessages()

	return nil
}

func (c *WSClient) handleMessages() {
	for {
		select {
		case <-c.ctx.Done():
			syslog.L.Infof("Message handler stopping - context cancelled | client_id=%s",
				c.config.ClientID)
			return
		default:
			var message Message
			err := wsjson.Read(c.ctx, c.conn, &message)
			if err != nil {
				c.handleConnectionError(err)
				return
			}

			syslog.L.Infof("Received message | type=%s client_id=%s",
				message.Type, c.config.ClientID)
			c.handleMessage(&message)
		}
	}
}

func (c *WSClient) handleMessage(msg *Message) {
	c.handlerMu.RLock()
	handler, exists := c.handlers[msg.Type]
	c.handlerMu.RUnlock()

	if !exists {
		syslog.L.Warnf("No handler registered | message_type=%s client_id=%s",
			msg.Type, c.config.ClientID)
		return
	}

	ctx, cancel := context.WithTimeout(c.ctx, messageTimeout)
	defer cancel()

	err := c.workerPool.Submit(ctx, func() {
		start := time.Now()
		if err := handler(ctx, msg); err != nil {
			syslog.L.Errorf("Message handler failed | type=%s client_id=%s error=%v duration=%v",
				msg.Type, c.config.ClientID, err, time.Since(start))
		} else {
			syslog.L.Infof("Message handled successfully | type=%s client_id=%s duration=%v",
				msg.Type, c.config.ClientID, time.Since(start))
		}
	})

	if err != nil {
		syslog.L.Warnf("Worker pool submission failed | type=%s client_id=%s error=%v",
			msg.Type, c.config.ClientID, err)
	}
}

func (c *WSClient) handleConnectionError(err error) {
	if isNormalClosureError(err) {
		syslog.L.Infof("WebSocket connection closed normally | client_id=%s", c.config.ClientID)
	} else {
		syslog.L.Errorf("WebSocket connection error | client_id=%s error=%v",
			c.config.ClientID, err)
	}
	c.isConnected.Store(false)

	// Attempt reconnection with backoff
	for attempt := 0; attempt < maxRetryAttempts; attempt++ {
		select {
		case <-c.ctx.Done():
			return
		case <-time.After(backoff(attempt)):
			syslog.L.Infof("Attempting reconnection | attempt=%d/%d client_id=%s",
				attempt+1, maxRetryAttempts, c.config.ClientID)

			ctx, cancel := context.WithTimeout(c.ctx, operationTimeout)
			if err := c.Connect(ctx); err == nil {
				syslog.L.Infof("Reconnection successful | client_id=%s attempt=%d",
					c.config.ClientID, attempt+1)
				cancel()
				return
			}
			cancel()
		}
	}

	syslog.L.Errorf("Reconnection failed after %d attempts | client_id=%s",
		maxRetryAttempts, c.config.ClientID)
}

func (c *WSClient) Send(ctx context.Context, msg Message) error {
	if !c.isConnected.Load() {
		return fmt.Errorf("not connected")
	}

	c.connMu.RLock()
	defer c.connMu.RUnlock()

	start := time.Now()
	err := wsjson.Write(ctx, c.conn, &msg)
	if err != nil {
		syslog.L.Errorf("Failed to send message | type=%s client_id=%s error=%v duration=%v",
			msg.Type, c.config.ClientID, err, time.Since(start))
		return err
	}

	syslog.L.Infof("Message sent successfully | type=%s client_id=%s duration=%v",
		msg.Type, c.config.ClientID, time.Since(start))
	return nil
}

func (c *WSClient) RegisterHandler(msgType string, handler MessageHandler) UnregisterFunc {
	c.handlerMu.Lock()
	c.handlers[msgType] = handler
	c.handlerMu.Unlock()

	syslog.L.Infof("Registered message handler | type=%s client_id=%s",
		msgType, c.config.ClientID)

	return func() {
		c.handlerMu.Lock()
		defer c.handlerMu.Unlock()

		if _, exists := c.handlers[msgType]; exists {
			delete(c.handlers, msgType)
			syslog.L.Infof("Unregistered message handler | type=%s client_id=%s",
				msgType, c.config.ClientID)
		} else {
			syslog.L.Warnf("Attempted to unregister non-existent handler | type=%s client_id=%s",
				msgType, c.config.ClientID)
		}
	}
}

func (c *WSClient) Close() error {
	var closeErr error
	c.closeOnce.Do(func() {
		syslog.L.Infof("Closing WebSocket client | client_id=%s", c.config.ClientID)
		c.cancel()

		c.connMu.Lock()
		defer c.connMu.Unlock()

		if c.conn != nil {
			closeErr = c.conn.Close(websocket.StatusNormalClosure, "client closing")
			if closeErr != nil {
				syslog.L.Errorf("Error closing connection | client_id=%s error=%v",
					c.config.ClientID, closeErr)
			}
		}

		c.isConnected.Store(false)
		c.workerPool.Wait()
		syslog.L.Infof("WebSocket client closed | client_id=%s", c.config.ClientID)
	})

	return closeErr
}

// Helper function to identify normal closure errors
func isNormalClosureError(err error) bool {
	return websocket.CloseStatus(err) == websocket.StatusNormalClosure ||
		strings.Contains(err.Error(), "context canceled") ||
		strings.Contains(err.Error(), "EOF")
}

func backoff(attempt int) time.Duration {
	base := time.Second
	max := 30 * time.Second
	duration := time.Duration(1<<uint(attempt)) * base
	if duration > max {
		duration = max
	}
	return duration
}

func (c *WSClient) GetConnectionStatus() bool {
	return c.isConnected.Load()
}
