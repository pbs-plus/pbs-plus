//go:build windows

package controllers

import (
	"context"
	"fmt"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/sftp"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"github.com/sonroyaalmerol/pbs-plus/internal/websockets"
)

var sftpSessions sync.Map

func WSHandler(ctx context.Context, c *websocket.Conn, m websockets.Message, infoChan chan string, errChan chan string) {
	if m.Type == "ping" {
		response := websockets.Message{
			Type:    "ping",
			Content: "pong",
		}
		c.WriteJSON(response)
	} else if m.Type == "backup_start" {
		infoChan <- fmt.Sprintf("Received backup request for drive %s.", m.Content)

		snapshot, err := snapshots.Snapshot(m.Content)
		if err != nil {
			errChan <- fmt.Sprintf("Snapshot error: %v", err)
			return
		}
		infoChan <- fmt.Sprintf("Snapshot of drive %s has been made.", m.Content)

		sftpSession := sftp.NewSFTPSession(ctx, snapshot, m.Content)
		if sftpSession == nil {
			errChan <- "Unable to find initialized SFTP config. Try restarting the service."
		}

		if existing, ok := sftpSessions.LoadAndDelete(m.Content); ok && existing != nil {
			existing.(*sftp.SFTPSession).Close()
		}

		go sftpSession.Serve(errChan)

		infoChan <- fmt.Sprintf("SFTP access to snapshot of drive %s has been made.", m.Content)

		sftpSessions.Store(m.Content, sftpSession)

		response := websockets.Message{
			Type:    "response-backup_start",
			Content: "Acknowledged: " + m.Content,
		}
		c.WriteJSON(response)
	} else if m.Type == "backup_close" {
		infoChan <- fmt.Sprintf("Received closure request for drive %s.", m.Content)

		if existing, ok := sftpSessions.LoadAndDelete(m.Content); ok && existing != nil {
			infoChan <- fmt.Sprintf("Cancelled existing backup context of drive %s.", m.Content)
			existing.(*sftp.SFTPSession).Close()
		}

		response := websockets.Message{
			Type:    "response-backup_close",
			Content: "Acknowledged: " + m.Content,
		}
		c.WriteJSON(response)
	}

	return
}
