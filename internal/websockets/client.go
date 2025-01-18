package websockets

import (
	"context"
	"crypto/tls"
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
	maxSendBuffer = 256
)

type MessageHandler func(msg *Message)

type Config struct {
	ServerURL string
	ClientID  string
	Headers   http.Header
}

type WSClient struct {
	ClientID  string
	serverURL string
	headers   http.Header

	readLimiter  *rate.Limiter
	writeLimiter *rate.Limiter

	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	conn        *websocket.Conn
	connMu      sync.Mutex
	send        chan Message
	IsConnected bool

	handlers  map[string]MessageHandler
	handlerMu sync.RWMutex

	readCrashed  chan struct{}
	writeCrashed chan struct{}
}

func NewWSClient(ctx context.Context, config Config) (*WSClient, error) {
	ctx, cancel := context.WithCancel(ctx)

	client := &WSClient{
		ClientID:     config.ClientID,
		serverURL:    config.ServerURL,
		headers:      config.Headers,
		ctx:          ctx,
		cancel:       cancel,
		readLimiter:  rate.NewLimiter(rate.Every(time.Millisecond*100), 10),
		writeLimiter: rate.NewLimiter(rate.Every(time.Millisecond*100), 10),
		send:         make(chan Message, maxSendBuffer),
		writeCrashed: make(chan struct{}, 1),
		readCrashed:  make(chan struct{}, 1),
		handlers:     make(map[string]MessageHandler),
	}

	return client, nil
}

func (c *WSClient) Connect() error {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c.IsConnected {
		return nil
	}

	conn, _, err := websocket.Dial(c.ctx, c.serverURL, &websocket.DialOptions{
		Subprotocols: []string{"pbs"},
		HTTPHeader:   c.headers,
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("dial failed: %w", err)
	}
	c.conn = conn
	c.IsConnected = true

	return nil
}

func (c *WSClient) Close() error {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	if !c.IsConnected {
		return nil
	}

	c.cancel()
	c.IsConnected = false

	return c.conn.Close(websocket.StatusNormalClosure, "client closing")
}

func (c *WSClient) Start() {
	receiveCtx, receiveCancel := context.WithCancel(c.ctx)
	sendCtx, sendCancel := context.WithCancel(c.ctx)

	c.wg.Add(2)
	go func() {
		defer c.wg.Done()
		defer receiveCancel()
		c.receiveLoop(receiveCtx)
	}()

	go func() {
		defer c.wg.Done()
		defer sendCancel()
		c.sendLoop(sendCtx)
	}()

	go func() {
		for {
			select {
			case <-c.ctx.Done():
				c.connMu.Lock()
				c.IsConnected = false
				c.connMu.Unlock()
				return
			case <-c.readCrashed:
				c.readCrashed = make(chan struct{}, 1)
				receiveCtx, receiveCancel = context.WithCancel(c.ctx)
				c.wg.Add(1)
				go func() {
					defer c.wg.Done()
					defer receiveCancel()
					c.receiveLoop(receiveCtx)
				}()
			case <-c.writeCrashed:
				c.writeCrashed = make(chan struct{}, 1)
				sendCtx, sendCancel = context.WithCancel(c.ctx)
				c.wg.Add(1)
				go func() {
					defer c.wg.Done()
					defer sendCancel()
					c.sendLoop(sendCtx)
				}()
			}
		}
	}()
}

func (c *WSClient) RegisterHandler(msgType string, handler MessageHandler) {
	c.handlerMu.Lock()
	defer c.handlerMu.Unlock()
	c.handlers[msgType] = handler
}

func (c *WSClient) Send(msg Message) {
	c.send <- msg
}

func (c *WSClient) sendLoop(ctx context.Context) {
	for {
		select {
		case msg := <-c.send:
			err := c.writeMessage(msg)
			if err != nil {
				log.Printf("Error sending message: %v", err)
				close(c.writeCrashed)
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (c *WSClient) receiveLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			err := c.readMessage()
			if err != nil {
				if websocket.CloseStatus(err) == websocket.StatusNormalClosure {
					return
				}
				log.Printf("Error reading message: %v", err)
				close(c.readCrashed)
				return
			}
		}
	}
}

func (c *WSClient) writeMessage(msg Message) error {
	ctx, cancel := context.WithTimeout(c.ctx, time.Second*5)
	defer cancel()

	err := c.writeLimiter.Wait(ctx)
	if err != nil {
		return err
	}

	return wsjson.Write(ctx, c.conn, &msg)
}

func (c *WSClient) readMessage() error {
	ctx, cancel := context.WithTimeout(c.ctx, time.Second*5)
	defer cancel()

	err := c.readLimiter.Wait(ctx)
	if err != nil {
		return err
	}

	var message Message
	err = wsjson.Read(ctx, c.conn, &message)
	if err != nil {
		if websocket.CloseStatus(err) == websocket.StatusNormalClosure {
			return nil
		}
		return fmt.Errorf("failed to read message: %w", err)
	}

	c.handleMessage(message)
	return nil
}

func (c *WSClient) handleMessage(msg Message) {
	c.handlerMu.RLock()
	handler, exists := c.handlers[msg.Type]
	c.handlerMu.RUnlock()

	if exists {
		handler(&msg)
	} else {
		log.Printf("No handler for message type: %s", msg.Type)
	}
}
