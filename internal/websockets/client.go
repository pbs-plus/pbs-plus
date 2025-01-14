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
	"os/signal"
	"sync"
	"time"

	"github.com/billgraziano/dpapi"
	"github.com/gorilla/websocket"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
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

	ctx         context.Context
	cancel      context.CancelFunc
	conn        *websocket.Conn
	send        chan Message
	reconnect   chan struct{}
	mutex       sync.RWMutex
	writeMutex  sync.Mutex
	isConnected bool
	closeOnce   sync.Once
}

type exponentialBackoff struct {
	current    time.Duration
	initial    time.Duration
	max        time.Duration
	multiplier float64
}

func newExponentialBackoff(initial, max time.Duration) *exponentialBackoff {
	return &exponentialBackoff{
		initial:    initial,
		max:        max,
		current:    initial,
		multiplier: 2.0,
	}
}

func (b *exponentialBackoff) NextBackOff() time.Duration {
	defer func() {
		b.current = time.Duration(float64(b.current) * b.multiplier)
		if b.current > b.max {
			b.current = b.max
		}
	}()
	return b.current
}

func (b *exponentialBackoff) Reset() {
	b.current = b.initial
}

func NewWSClient(commandListener func(*websocket.Conn, Message)) (*WSClient, error) {
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

	ctx, cancel := context.WithCancel(context.Background())
	client := &WSClient{
		ClientID:        clientID,
		ServerURL:       serverURL,
		Headers:         headers,
		CommandListener: commandListener,
		ctx:             ctx,
		cancel:          cancel,
		send:            make(chan Message, maxSendBuffer),
		reconnect:       make(chan struct{}, 1),
	}

	go client.connectionManager()
	go client.handleSignals()

	return client, nil
}

func (c *WSClient) connectionManager() {
	backoff := newExponentialBackoff(initialBackoff, reconnectTimeout)

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			if err := c.connect(); err != nil {
				delay := backoff.NextBackOff()
				syslog.L.Errorf("Connection failed: %v. Retrying in %v...", err, delay)

				select {
				case <-time.After(delay):
					continue // Continue trying to reconnect
				case <-c.ctx.Done():
					return
				}
			}

			backoff.Reset()

			connCtx, connCancel := context.WithCancel(c.ctx)
			wg := &sync.WaitGroup{}
			wg.Add(2)

			go func() {
				defer wg.Done()
				c.readPump(connCtx)
			}()

			go func() {
				defer wg.Done()
				c.writePump(connCtx)
			}()

			var err error
			<-c.ctx.Done()
			err = c.ctx.Err()

			connCancel()
			wg.Wait()

			c.mutex.Lock()
			if c.conn != nil {
				c.conn.Close()
				c.conn = nil
				c.isConnected = false
			}
			c.mutex.Unlock()

			if err == context.Canceled {
				return
			}

			continue
		}
	}
}

func (c *WSClient) connect() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.isConnected {
		return nil
	}

	dialer := websocket.Dialer{
		TLSClientConfig:  &tls.Config{InsecureSkipVerify: true},
		Proxy:            http.ProxyFromEnvironment,
		HandshakeTimeout: handshakeTimeout,
	}

	ctx, cancel := context.WithTimeout(c.ctx, handshakeTimeout)
	defer cancel()

	conn, _, err := dialer.DialContext(ctx, c.ServerURL, c.Headers)
	if err != nil {
		return fmt.Errorf("dial failed: %v", err)
	}

	initMessage := Message{
		Type:    "init",
		Content: c.ClientID,
	}

	conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	if err := conn.WriteJSON(initMessage); err != nil {
		conn.Close()
		return fmt.Errorf("init message failed: %v", err)
	}

	c.conn = conn
	c.isConnected = true
	return nil
}

func (c *WSClient) readPump(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			syslog.L.Errorf("read pump panic: %v", r)
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
		case <-ctx.Done():
			return
		default:
			var msg Message
			err := c.conn.ReadJSON(&msg)
			if err != nil {
				syslog.L.Errorf("read error: %v", err)
				return
			}

			if c.CommandListener != nil {
				c.CommandListener(c.conn, msg)
			}
		}
	}
}

func (c *WSClient) writePump(ctx context.Context) {
	ticker := time.NewTicker(clientPingPeriod)
	defer func() {
		ticker.Stop()
		if r := recover(); r != nil {
			syslog.L.Errorf("write pump panic: %v", r)
		}
	}()

	for {
		select {
		case message, ok := <-c.send:
			if !ok {
				syslog.L.Errorf("send channel closed")
				return
			}

			c.writeMutex.Lock()
			c.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			err := c.conn.WriteJSON(message)
			c.writeMutex.Unlock()

			if err != nil {
				syslog.L.Errorf("write error: %v", err)
				return
			}

		case <-ticker.C:
			c.writeMutex.Lock()
			c.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			err := c.conn.WriteMessage(websocket.PingMessage, nil)
			c.writeMutex.Unlock()

			if err != nil {
				syslog.L.Errorf("ping error: %v", err)
				return
			}

		case <-ctx.Done():
			return
		}
	}
}

func (c *WSClient) SendMessage(msg Message) error {
	c.mutex.RLock()
	if !c.isConnected {
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

func (c *WSClient) handleSignals() {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	select {
	case <-interrupt:
		c.Close()
	case <-c.ctx.Done():
		signal.Stop(interrupt)
		close(interrupt)
	}
}

func (c *WSClient) Close() {
	c.closeOnce.Do(func() {
		c.cancel() // Cancel context first to stop all goroutines

		c.mutex.Lock()
		defer c.mutex.Unlock()

		if c.conn != nil {
			// Best effort to send close message
			c.writeMutex.Lock()
			c.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			c.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			c.writeMutex.Unlock()

			time.Sleep(time.Second) // Give time for close message to be sent
			c.conn.Close()
			c.conn = nil
			c.isConnected = false
		}

		close(c.send)
	})
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
