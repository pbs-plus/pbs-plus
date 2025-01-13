//go:build windows

package controllers

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/sftp"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"github.com/sonroyaalmerol/pbs-plus/internal/websockets"
)

var (
	sftpSessions    sync.Map
	ErrNoSFTPConfig = errors.New("unable to find initialized SFTP config")
)

func sendResponse(c *websocket.Conn, msgType, content string) error {
	response := websockets.Message{
		Type:    "response-" + msgType,
		Content: "Acknowledged: " + content,
	}
	return c.WriteJSON(response)
}

func cleanupExistingSession(drive string, infoChan chan<- string) {
	if existing, ok := sftpSessions.LoadAndDelete(drive); ok && existing != nil {
		if session, ok := existing.(*sftp.SFTPSession); ok && session != nil {
			session.Close()
			infoChan <- fmt.Sprintf("Cancelled existing backup context of drive %s.", drive)
		}
	}
}

func handleBackupStart(ctx context.Context, c *websocket.Conn, drive string, infoChan chan<- string, errChan chan<- string) error {
	infoChan <- fmt.Sprintf("Received backup request for drive %s.", drive)

	snapshot, err := snapshots.Snapshot(drive)
	if err != nil {
		return fmt.Errorf("snapshot error: %w", err)
	}
	infoChan <- fmt.Sprintf("Snapshot of drive %s has been made.", drive)

	sftpSession := sftp.NewSFTPSession(ctx, snapshot, drive)
	if sftpSession == nil {
		return ErrNoSFTPConfig
	}

	cleanupExistingSession(drive, infoChan)

	go func() {
		defer cleanupExistingSession(drive, infoChan)
		sftpSession.Serve(errChan)
	}()

	infoChan <- fmt.Sprintf("SFTP access to snapshot of drive %s has been made.", drive)
	sftpSessions.Store(drive, sftpSession)

	return sendResponse(c, "backup_start", drive)
}

func handleBackupClose(c *websocket.Conn, drive string, infoChan chan<- string) error {
	infoChan <- fmt.Sprintf("Received closure request for drive %s.", drive)
	cleanupExistingSession(drive, infoChan)
	return sendResponse(c, "backup_close", drive)
}

func WSHandler(ctx context.Context, c *websocket.Conn, m websockets.Message, infoChan chan<- string, errChan chan<- string) {
	if c == nil {
		errChan <- "nil WebSocket connection"
		return
	}

	// Add timeout to context if needed
	// ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	// defer cancel()

	var err error
	switch m.Type {
	case "backup_start":
		err = handleBackupStart(ctx, c, m.Content, infoChan, errChan)
	case "backup_close":
		err = handleBackupClose(c, m.Content, infoChan)
	default:
		err = fmt.Errorf("unknown message type: %s", m.Type)
	}

	if err != nil {
		errChan <- err.Error()
	}
}
