//go:build windows

package websockets

import (
	"encoding/base64"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"time"

	"github.com/billgraziano/dpapi"
	"github.com/gorilla/websocket"
	"golang.org/x/sys/windows/registry"
)

type WSClient struct {
	ClientID string
	Conn     *websocket.Conn
}

func NewWSClient(commandListener func(*websocket.Conn, Message)) error {
	serverUrl := ""
	key, err := registry.OpenKey(registry.LOCAL_MACHINE, `Software\PBSPlus\Config`, registry.QUERY_VALUE)
	if err != nil {
		return fmt.Errorf("NewWSClient: server url not found -> %w", err)
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
		return fmt.Errorf("NewWSClient: server url not found -> %w", err)
	}

	clientID, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("NewWSClient: hostname not found -> %w", err)
	}

	headers := http.Header{}
	if drivePublicKey != nil {
		encodedKey := base64.StdEncoding.EncodeToString([]byte(*drivePublicKey))
		headers.Set("Authorization", fmt.Sprintf("PBSPlusAPIAgent=%s---C:%s", clientID, encodedKey))
	}

	serverUrl, err = url.JoinPath(serverUrl, "/plus/ws")
	if err != nil {
		return fmt.Errorf("NewWSClient: invalid server url path -> %w", err)
	}

	parsedUrl, err := url.Parse(serverUrl)
	if err != nil {
		return fmt.Errorf("NewWSClient: invalid server url path -> %w", err)
	}
	parsedUrl.Scheme = "wss"

	serverUrl = parsedUrl.String()

	conn, _, err := websocket.DefaultDialer.Dial(serverUrl, headers)
	if err != nil {
		return fmt.Errorf("NewWSClient: ws dial invalid -> %w", err)
	}

	initMessage := Message{
		Type:    "init",
		Content: clientID,
	}

	err = conn.WriteJSON(initMessage)
	if err != nil {
		return fmt.Errorf("NewWSClient: ws write message error -> %w", err)
	}

	// Listen for messages from the server
	go func() {
		for {
			var serverMessage Message
			err := conn.ReadJSON(&serverMessage)
			if err != nil {
				log.Printf("Connection closed: %v", err)
				return
			}
			log.Printf("Received message: Type=%s, Content=%s", serverMessage.Type, serverMessage.Content)

			commandListener(conn, serverMessage)

		}
	}()

	newClient := &WSClient{
		ClientID: clientID,
		Conn:     conn,
	}

	go newClient.WaitInterrupt()

	return nil
}

func (client *WSClient) WaitInterrupt() {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	for {
		select {
		case <-interrupt:
			// Gracefully close the connection
			client.Conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			time.Sleep(time.Second)
			return
		}
	}
}

func (client *WSClient) Close() {
	client.Conn.Close()
}
