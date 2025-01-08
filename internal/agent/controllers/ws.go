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

func WSHandler(ctx context.Context, c *websocket.Conn, m websockets.Message, errChan chan string) {
	if m.Type == "ping" {
		response := websockets.Message{
			Type:    "ping",
			Content: "pong",
		}
		c.WriteJSON(response)
	} else if m.Type == "backup_start" {
		snapshot, err := snapshots.Snapshot(m.Content)
		if err != nil {
			errChan <- fmt.Sprintf("Snapshot error: %v", err)
			return
		}

		if existing, ok := sftpSessions.LoadAndDelete(m.Content); ok {
			existing.(context.CancelFunc)()
		}

		sftpCtx, sftpClose := context.WithCancel(ctx)
		go sftp.Serve(sftpCtx, errChan, snapshot, m.Content)

		sftpSessions.Store(m.Content, sftpClose)

		response := websockets.Message{
			Type:    "response-backup_start",
			Content: "Acknowledged: " + m.Content,
		}
		c.WriteJSON(response)
	} else if m.Type == "backup_close" {
		if existing, ok := sftpSessions.LoadAndDelete(m.Content); ok {
			existing.(context.CancelFunc)()
		}

		response := websockets.Message{
			Type:    "response-backup_close",
			Content: "Acknowledged: " + m.Content,
		}
		c.WriteJSON(response)
	}

	return
}
