//go:build windows

package websockets

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/billgraziano/dpapi"
	"github.com/gorilla/websocket"
	"golang.org/x/sys/windows/registry"
)

const (
	writeTimeout     = 10 * time.Second
	readTimeout      = 60 * time.Second
	handshakeTimeout = 10 * time.Second
	clientPingPeriod = 54 * time.Second
	maxSendBuffer    = 256
	reconnectTimeout = 2 * time.Minute
	initialBackoff   = time.Second
)

type WSClient struct {
	ClientID        string
	ServerURL       string
	Headers         http.Header
	CommandListener func(*websocket.Conn, Message)

	ctx        context.Context
	cancel     context.CancelFunc
	conn       *websocket.Conn
	send       chan Message
	mutex      sync.RWMutex
	writeMutex sync.Mutex

	disconnected chan struct{}
	connected    bool
}

func NewWSClient(commandListener func(*websocket.Conn, Message)) (*WSClient, error) {
	serverURL, err := getServerURLFromRegistry()
	if err != nil {
		return nil, fmt.Errorf("failed to get server URL: %w", err)
	}

	clientID, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("failed to get hostname: %w", err)
	}

	headers, err := buildHeaders(clientID)
	if err != nil {
		return nil, fmt.Errorf("failed to build headers: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	client := &WSClient{
		ClientID:        clientID,
		ServerURL:       serverURL,
		Headers:         headers,
		CommandListener: commandListener,
		ctx:             ctx,
		cancel:          cancel,
		send:            make(chan Message, maxSendBuffer),
		disconnected:    make(chan struct{}),
	}

	return client, nil
}

func (c *WSClient) Connect() error {
	c.mutex.Lock()
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}

	select {
	case <-c.disconnected:
		c.disconnected = make(chan struct{})
	default:
	}
	c.mutex.Unlock()

	dialer := websocket.Dialer{
		TLSClientConfig:  &tls.Config{InsecureSkipVerify: true},
		Proxy:            http.ProxyFromEnvironment,
		HandshakeTimeout: handshakeTimeout,
	}

	ctx, cancel := context.WithTimeout(c.ctx, handshakeTimeout)
	defer cancel()

	conn, _, err := dialer.DialContext(ctx, c.ServerURL, c.Headers)
	if err != nil {
		return fmt.Errorf("dial failed: %w", err)
	}

	initMessage := Message{
		Type:    "init",
		Content: c.ClientID,
	}

	conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	if err := conn.WriteJSON(initMessage); err != nil {
		conn.Close()
		return fmt.Errorf("init message failed: %w", err)
	}

	c.mutex.Lock()
	c.connected = true
	c.conn = conn
	c.mutex.Unlock()

	// Start read and write pumps
	errChan := make(chan error, 2)
	go c.readPump(errChan)
	go c.writePump(errChan)

	// Return any immediate errors
	select {
	case err := <-errChan:
		return err
	case <-time.After(time.Second):
		return nil
	}
}

func (c *WSClient) handleDisconnect() {
	c.mutex.Lock()
	if c.connected {
		c.connected = false
		close(c.disconnected)
	}
	c.mutex.Unlock()
}

func (c *WSClient) readPump(errChan chan<- error) {
	defer func() {
		c.handleDisconnect()
		c.conn.Close()

		if r := recover(); r != nil {
			errChan <- fmt.Errorf("read pump panic: %v\n", r)
		}
	}()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(readTimeout))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(readTimeout))
		return nil
	})

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			var msg Message
			err := c.conn.ReadJSON(&msg)
			if err != nil {
				errChan <- fmt.Errorf("read error: %w", err)
				return
			}

			if c.CommandListener != nil {
				func() {
					defer func() {
						if r := recover(); r != nil {
							errChan <- fmt.Errorf("command listener panic: %v\n", r)
						}
					}()
					c.CommandListener(c.conn, msg)
				}()
			}
		}
	}
}

func (c *WSClient) writePump(errChan chan<- error) {
	ticker := time.NewTicker(clientPingPeriod)
	defer func() {
		ticker.Stop()
		if r := recover(); r != nil {
			errChan <- fmt.Errorf("write pump panic: %v\n", r)
		}
	}()

	for {
		select {
		case message, ok := <-c.send:
			if !ok {
				errChan <- fmt.Errorf("send channel closed")
				return
			}

			c.writeMutex.Lock()
			c.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			err := c.conn.WriteJSON(message)
			c.writeMutex.Unlock()

			if err != nil {
				errChan <- fmt.Errorf("write error: %w", err)
				return
			}

		case <-ticker.C:
			c.writeMutex.Lock()
			c.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			err := c.conn.WriteMessage(websocket.PingMessage, nil)
			c.writeMutex.Unlock()

			if err != nil {
				errChan <- fmt.Errorf("ping error: %w", err)
				return
			}

		case <-c.ctx.Done():
			return
		}
	}
}

func (c *WSClient) Close() error {
	c.cancel()
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.conn != nil {
		// Best effort to send close message
		c.writeMutex.Lock()
		c.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
		c.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		c.writeMutex.Unlock()

		// Give time for close message to be sent
		time.Sleep(time.Second)
		c.conn.Close()
		c.conn = nil
	}

	close(c.send)
	return nil
}

func (c *WSClient) Wait() <-chan struct{} {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.disconnected
}

func (c *WSClient) IsConnected() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.connected
}

func (c *WSClient) SendMessage(msg Message) error {
	c.mutex.RLock()
	if c.conn == nil {
		c.mutex.RUnlock()
		return fmt.Errorf("not connected")
	}
	c.mutex.RUnlock()

	select {
	case c.send <- msg:
		return nil
	case <-time.After(writeTimeout):
		return fmt.Errorf("send timeout")
	case <-c.ctx.Done():
		return fmt.Errorf("client closed")
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
		return "", fmt.Errorf("failed to open registry key: %w", err)
	}
	defer key.Close()

	serverURL, _, err := key.GetStringValue("ServerURL")
	if err != nil {
		return "", fmt.Errorf("server URL not found: %w", err)
	}

	serverURL, err = validateRegistryValue(serverURL, 1024)
	if err != nil {
		return "", fmt.Errorf("invalid server URL: %w", err)
	}

	serverURL, err = url.JoinPath(serverURL, "/plus/ws")
	if err != nil {
		return "", fmt.Errorf("invalid server URL path: %w", err)
	}

	parsedURL, err := url.Parse(serverURL)
	if err != nil {
		return "", fmt.Errorf("invalid server URL: %w", err)
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
				return headers, fmt.Errorf("invalid server key: %w", err)
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
