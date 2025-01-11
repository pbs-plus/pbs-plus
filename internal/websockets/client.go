//go:build windows

package websockets

import (
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/billgraziano/dpapi"
	"github.com/gorilla/websocket"
	"golang.org/x/sys/windows/registry"
)

type WSClient struct {
	ClientID        string
	ServerURL       string
	Headers         http.Header
	CommandListener func(*websocket.Conn, Message)

	conn        *websocket.Conn
	send        chan Message
	done        chan struct{}
	reconnect   chan struct{}
	mutex       sync.Mutex
	isConnected bool
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

	client := &WSClient{
		ClientID:        clientID,
		ServerURL:       serverURL,
		Headers:         headers,
		CommandListener: commandListener,
		send:            make(chan Message, 256),
		done:            make(chan struct{}),
		reconnect:       make(chan struct{}, 1),
	}

	go client.connectionManager()
	go client.handleSignals()

	return client, nil
}

func (c *WSClient) connectionManager() {
	backoff := newExponentialBackoff(time.Second, 2*time.Minute)

	for {
		select {
		case <-c.done:
			return
		case <-c.reconnect:
			// Reset backoff on manual reconnect
			backoff.Reset()
		default:
			if err := c.connect(); err != nil {
				delay := backoff.NextBackOff()
				log.Printf("Connection failed: %v. Retrying in %v...", err, delay)
				time.Sleep(delay)
				continue
			}

			// Reset backoff after successful connection
			backoff.Reset()

			// Start read/write pumps
			errChan := make(chan error, 2)
			go c.readPump(errChan)
			go c.writePump(errChan)

			// Wait for pump errors
			err := <-errChan
			log.Printf("Connection error: %v", err)

			c.mutex.Lock()
			if c.conn != nil {
				c.conn.Close()
				c.conn = nil
				c.isConnected = false
			}
			c.mutex.Unlock()
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
		HandshakeTimeout: 10 * time.Second,
	}

	conn, _, err := dialer.Dial(c.ServerURL, c.Headers)
	if err != nil {
		return fmt.Errorf("dial failed: %w", err)
	}

	initMessage := Message{
		Type:    "init",
		Content: c.ClientID,
	}

	if err := conn.WriteJSON(initMessage); err != nil {
		conn.Close()
		return fmt.Errorf("init message failed: %w", err)
	}

	c.conn = conn
	c.isConnected = true
	return nil
}

func (c *WSClient) readPump(errChan chan<- error) {
	defer func() {
		if r := recover(); r != nil {
			errChan <- fmt.Errorf("read pump panic: %v", r)
		}
	}()

	c.conn.SetReadLimit(512 * 1024)
	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		var msg Message
		err := c.conn.ReadJSON(&msg)
		if err != nil {
			errChan <- fmt.Errorf("read error: %w", err)
			return
		}

		if c.CommandListener != nil {
			c.CommandListener(c.conn, msg)
		}
	}
}

func (c *WSClient) writePump(errChan chan<- error) {
	ticker := time.NewTicker(54 * time.Second)
	defer func() {
		ticker.Stop()
		if r := recover(); r != nil {
			errChan <- fmt.Errorf("write pump panic: %v", r)
		}
	}()

	for {
		select {
		case message, ok := <-c.send:
			if !ok {
				errChan <- fmt.Errorf("send channel closed")
				return
			}
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteJSON(message); err != nil {
				errChan <- fmt.Errorf("write error: %w", err)
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				errChan <- fmt.Errorf("ping error: %w", err)
				return
			}
		case <-c.done:
			return
		}
	}
}

func (c *WSClient) SendMessage(msg Message) error {
	c.mutex.Lock()
	if !c.isConnected {
		c.mutex.Unlock()
		return fmt.Errorf("not connected")
	}
	c.mutex.Unlock()

	select {
	case c.send <- msg:
		return nil
	case <-time.After(time.Second):
		return fmt.Errorf("send timeout")
	}
}

func (c *WSClient) handleSignals() {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	<-interrupt
	c.Close()
}

func (c *WSClient) Close() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.conn != nil {
		c.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		time.Sleep(time.Second)
		c.conn.Close()
		c.conn = nil
		c.isConnected = false
	}

	close(c.done)
}

// Helper functions

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

func getServerURLFromRegistry() (string, error) {
	key, err := registry.OpenKey(registry.LOCAL_MACHINE, `Software\PBSPlus\Config`, registry.QUERY_VALUE)
	if err != nil {
		return "", fmt.Errorf("failed to open registry key: %w", err)
	}
	defer key.Close()

	serverURL, _, err := key.GetStringValue("ServerURL")
	if err != nil || serverURL == "" {
		return "", fmt.Errorf("server URL not found: %w", err)
	}

	serverURL, err = url.JoinPath(serverURL, "/plus/ws")
	if err != nil {
		return "", fmt.Errorf("invalid server URL path: %w", err)
	}

	parsedURL, err := url.Parse(serverURL)
	if err != nil {
		return "", fmt.Errorf("invalid server URL: %w", err)
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
