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
	"time"

	"github.com/billgraziano/dpapi"
	"github.com/gorilla/websocket"
	"golang.org/x/sys/windows/registry"
)

type WSClient struct {
	ClientID        string
	Conn            *websocket.Conn
	ServerURL       string
	Headers         http.Header
	CommandListener func(*websocket.Conn, Message)
	done            chan struct{}
    dialer          *websocket.Dialer
}

func NewWSClient(commandListener func(*websocket.Conn, Message)) (*WSClient, error) {
	serverUrl := ""
	key, err := registry.OpenKey(registry.LOCAL_MACHINE, `Software\PBSPlus\Config`, registry.QUERY_VALUE)
	if err != nil {
		return nil, fmt.Errorf("NewWSClient: server url not found -> %w", err)
	}
	defer key.Close()

	var drivePublicKey *string
	keyStr := "Software\\PBSPlus\\Config\\SFTP-C"
	if driveKey, err := registry.OpenKey(registry.LOCAL_MACHINE, keyStr, registry.QUERY_VALUE); err == nil {
		defer driveKey.Close()
		if publicKey, _, err := driveKey.GetStringValue("ServerKey"); err == nil {
			if decrypted, err := dpapi.Decrypt(publicKey); err == nil {
				if decoded, err := base64.StdEncoding.DecodeString(decrypted); err == nil {
					decodedStr := string(decoded)
					drivePublicKey = &decodedStr
				}
			}
		}
	}

	if serverUrl, _, err = key.GetStringValue("ServerURL"); err != nil || serverUrl == "" {
		return nil, fmt.Errorf("NewWSClient: server url not found -> %w", err)
	}

	clientID, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("NewWSClient: hostname not found -> %w", err)
	}

	headers := http.Header{}
	if drivePublicKey != nil {
		encodedKey := base64.StdEncoding.EncodeToString([]byte(*drivePublicKey))
		headers.Set("Authorization", fmt.Sprintf("PBSPlusAPIAgent=%s---C:%s", clientID, encodedKey))
	}

	serverUrl, err = url.JoinPath(serverUrl, "/plus/ws")
	if err != nil {
		return nil, fmt.Errorf("NewWSClient: invalid server url path -> %w", err)
	}

	parsedUrl, err := url.Parse(serverUrl)
	if err != nil {
		return nil, fmt.Errorf("NewWSClient: invalid server url path -> %w", err)
	}
	parsedUrl.Scheme = "wss"
	serverUrl = parsedUrl.String()

	client := &WSClient{
		ClientID:        clientID,
		ServerURL:       serverUrl,
		Headers:         headers,
		CommandListener: commandListener,
		done:            make(chan struct{}),
        dialer: &websocket.Dialer{
            TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
        },
	}

	go client.maintainConnection()

	return client, nil
}

func (client *WSClient) connect() error {
	conn, _, err := client.dialer.Dial(client.ServerURL, client.Headers)
	if err != nil {
		return fmt.Errorf("connect: ws dial invalid -> %w", err)
	}

	client.Conn = conn

	initMessage := Message{
		Type:    "init",
		Content: client.ClientID,
	}
	if err := conn.WriteJSON(initMessage); err != nil {
		conn.Close()
		return fmt.Errorf("connect: ws write message error -> %w", err)
	}

	return nil
}

func (client *WSClient) maintainConnection() {
	backoff := time.Second
	maxBackoff := time.Minute * 2

	for {
		err := client.connect()
		if err != nil {
			log.Printf("Connection failed: %v. Retrying in %v...", err, backoff)
			time.Sleep(backoff)
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		backoff = time.Second

		// Start message listener
		for {
			var serverMessage Message
			err := client.Conn.ReadJSON(&serverMessage)
			if err != nil {
				log.Printf("Connection closed: %v. Reconnecting...", err)
				client.Conn.Close()
				break
			}
			log.Printf("Received message: Type=%s, Content=%s", serverMessage.Type, serverMessage.Content)
			client.CommandListener(client.Conn, serverMessage)
		}

		select {
		case <-client.done:
			return
		default:
			continue
		}
	}
}

func (client *WSClient) Close() {
	close(client.done)
	if client.Conn != nil {
		client.Conn.Close()
	}
}
