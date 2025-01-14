//go:build windows

package websockets

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/billgraziano/dpapi"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"golang.org/x/sys/windows/registry"
	"golang.org/x/time/rate"
)

const (
	maxSendBuffer = 256
)

type MessageHandler func(msg *Message)

type WSClient struct {
	ClientID  string
	serverURL string
	headers   http.Header

	rateLimiter *rate.Limiter

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

func NewWSClient(ctx context.Context) (*WSClient, error) {
	serverURL, err := getServerURLFromRegistry()
	if err != nil {
		return nil, fmt.Errorf("failed to get server URL: %v", err)
	}

	clientID, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("failed to get hostname: %v", err)
	}

	headers, err := buildHeaders(clientID)
	if err != nil {
		return nil, fmt.Errorf("failed to build headers: %v", err)
	}

	l := rate.NewLimiter(rate.Every(time.Millisecond*100), 10)

	ctx, cancel := context.WithCancel(ctx)

	client := &WSClient{
		ClientID:     clientID,
		serverURL:    serverURL,
		headers:      headers,
		ctx:          ctx,
		cancel:       cancel,
		rateLimiter:  l,
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

	initMessage := Message{
		Type:    "init",
		Content: c.ClientID,
	}

	if err := c.writeMessage(initMessage); err != nil {
		c.conn.Close(websocket.StatusPolicyViolation, "init message failed")
		c.cancel()

		return fmt.Errorf("init message failed: %v", err)
	}

	return nil
}

func (c *WSClient) Close() error {
	c.cancel()

	return c.conn.Close(websocket.StatusNormalClosure, "client closing")
}

func (c *WSClient) Start() {
	go c.receiveLoop()
	go c.sendLoop()

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
				go c.receiveLoop()
			case <-c.writeCrashed:
				c.writeCrashed = make(chan struct{}, 1)
				go c.sendLoop()
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

func (c *WSClient) sendLoop() {
	for {
		select {
		case msg := <-c.send:
			err := c.writeMessage(msg)
			if err != nil {
				log.Printf("Error sending message: %v", err)
				close(c.writeCrashed)
				return
			}
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *WSClient) receiveLoop() {
	for {
		select {
		case <-c.ctx.Done():
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

	err := c.rateLimiter.Wait(ctx)
	if err != nil {
		return err
	}

	return wsjson.Write(ctx, c.conn, &msg)
}

func (c *WSClient) readMessage() error {
	ctx, cancel := context.WithTimeout(c.ctx, time.Second*5)
	defer cancel()

	err := c.rateLimiter.Wait(ctx)
	if err != nil {
		return err
	}

	var message Message
	err = wsjson.Read(ctx, c.conn, &message)
	if err != nil {
		return err
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

func validateRegistryValue(value string, maxLength int) (string, error) {
	if len(value) == 0 {
		return "", fmt.Errorf("empty value")
	}
	if len(value) > maxLength {
		return "", fmt.Errorf("value exceeds maximum length of %d", maxLength)
	}
	return value, nil
}

func getServerURLFromRegistry() (string, error) {
	key, err := registry.OpenKey(registry.LOCAL_MACHINE, `Software\PBSPlus\Config`, registry.QUERY_VALUE)
	if err != nil {
		return "", fmt.Errorf("failed to open registry key: %v", err)
	}
	defer key.Close()

	serverURL, _, err := key.GetStringValue("ServerURL")
	if err != nil {
		return "", fmt.Errorf("server URL not found: %v", err)
	}

	serverURL, err = validateRegistryValue(serverURL, 1024)
	if err != nil {
		return "", fmt.Errorf("invalid server URL: %v", err)
	}

	serverURL, err = url.JoinPath(serverURL, "/plus/ws")
	if err != nil {
		return "", fmt.Errorf("invalid server URL path: %v", err)
	}

	parsedURL, err := url.Parse(serverURL)
	if err != nil {
		return "", fmt.Errorf("invalid server URL: %v", err)
	}

	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return "", fmt.Errorf("invalid URL scheme: %s", parsedURL.Scheme)
	}

	parsedURL.Scheme = "wss"
	return parsedURL.String(), nil
}

func buildHeaders(clientID string) (http.Header, error) {
	headers := http.Header{}

	keyStr := "Software\\PBSPlus\\Config\\SFTP-C"
	if driveKey, err := registry.OpenKey(registry.LOCAL_MACHINE, keyStr, registry.QUERY_VALUE); err == nil {
		defer driveKey.Close()

		if publicKey, _, err := driveKey.GetStringValue("ServerKey"); err == nil {
			publicKey, err = validateRegistryValue(publicKey, 4096)
			if err != nil {
				return headers, fmt.Errorf("invalid server key: %v", err)
			}

			if decrypted, err := dpapi.Decrypt(publicKey); err == nil {
				if decoded, err := base64.StdEncoding.DecodeString(decrypted); err == nil {
					encodedKey := base64.StdEncoding.EncodeToString(decoded)
					headers.Set("Authorization", fmt.Sprintf("PBSPlusAPIAgent=%s---C:%s", clientID, encodedKey))
				}
			}
		}
	}

	return headers, nil
}
