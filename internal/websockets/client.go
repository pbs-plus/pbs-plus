package websockets

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
)

const (
	maxRetryAttempts = 10
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

	syslog.L.Infof("Initialized new WebSocket client for server %s", client.config.ServerURL)
	return client, nil
}

func (c *WSClient) Connect(ctx context.Context) error {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c.isConnected.Load() {
		return nil
	}

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
		return fmt.Errorf("dial failed: %w", err)
	}

	c.conn = conn
	c.isConnected.Store(true)

	// Start message handler in background
	go c.handleMessages()

	return nil
}

func (c *WSClient) handleMessages() {
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			var message Message
			err := wsjson.Read(c.ctx, c.conn, &message)
			if err != nil {
				c.handleConnectionError(err)
				return
			}

			c.handleMessage(&message)
		}
	}
}

func (c *WSClient) handleMessage(msg *Message) {
	c.handlerMu.RLock()
	handler, exists := c.handlers[msg.Type]
	c.handlerMu.RUnlock()

	if !exists {
		syslog.L.Infof("No handler registered for message type: %s", msg.Type)
		return
	}

	// Submit message handling to worker pool with timeout
	ctx, cancel := context.WithTimeout(c.ctx, messageTimeout)
	defer cancel()

	err := c.workerPool.Submit(ctx, func() {
		if err := handler(ctx, msg); err != nil {
			syslog.L.Errorf("Handler error for message type %s: %v", msg.Type, err)
		}
	})

	if err != nil {
		syslog.L.Warnf("Failed to submit message to worker pool: %v", err)
	}
}

func (c *WSClient) handleConnectionError(err error) {
	if websocket.CloseStatus(err) == websocket.StatusNormalClosure {
		return
	}

	syslog.L.Errorf("Connection error: %v", err)
	c.isConnected.Store(false)

	// Attempt reconnection with backoff
	for attempt := 0; attempt < maxRetryAttempts; attempt++ {
		select {
		case <-c.ctx.Done():
			return
		case <-time.After(backoff(attempt)):
			ctx, cancel := context.WithTimeout(c.ctx, operationTimeout)
			if err := c.Connect(ctx); err == nil {
				cancel()
				return
			}
			cancel()
		}
	}
}

func (c *WSClient) Send(ctx context.Context, msg Message) error {
	if !c.isConnected.Load() {
		return fmt.Errorf("not connected")
	}

	c.connMu.RLock()
	defer c.connMu.RUnlock()

	return wsjson.Write(ctx, c.conn, &msg)
}

func (c *WSClient) RegisterHandler(msgType string, handler MessageHandler) {
	c.handlerMu.Lock()
	defer c.handlerMu.Unlock()
	c.handlers[msgType] = handler
}

func (c *WSClient) Close() error {
	var closeErr error
	c.closeOnce.Do(func() {
		c.cancel()

		c.connMu.Lock()
		defer c.connMu.Unlock()

		if c.conn != nil {
			closeErr = c.conn.Close(websocket.StatusNormalClosure, "client closing")
		}

		c.isConnected.Store(false)
		c.workerPool.Wait()
	})

	return closeErr
}

func backoff(attempt int) time.Duration {
	// Exponential backoff with jitter
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
