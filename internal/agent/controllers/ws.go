//go:build windows

package controllers

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/sftp"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
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

func cleanupExistingSession(drive string) {
	if existing, ok := sftpSessions.LoadAndDelete(drive); ok && existing != nil {
		if session, ok := existing.(*sftp.SFTPSession); ok && session != nil {
			session.Close()
			syslog.L.Infof("Cancelled existing backup context of drive %s.", drive)
		}
	}
}

func handleBackupStart(ctx context.Context, c *websocket.Conn, drive string) error {
	syslog.L.Infof("Received backup request for drive %s.", drive)

	// Get backup status singleton and mark backup as started
	backupStatus := agent.GetBackupStatus()
	backupStatus.StartBackup(drive)
	defer backupStatus.EndBackup(drive) // Ensure we mark backup as complete even if there's an error

	snapshot, err := snapshots.Snapshot(drive)
	if err != nil {
		return fmt.Errorf("snapshot error: %w", err)
	}
	syslog.L.Infof("Snapshot of drive %s has been made.", drive)

	sftpSession := sftp.NewSFTPSession(ctx, snapshot, drive)
	if sftpSession == nil {
		return ErrNoSFTPConfig
	}

	cleanupExistingSession(drive)

	go func() {
		defer func() {
			cleanupExistingSession(drive)
			backupStatus.EndBackup(drive)
		}()
		sftpSession.Serve()
	}()

	syslog.L.Infof("SFTP access to snapshot of drive %s has been made.", drive)
	sftpSessions.Store(drive, sftpSession)

	return sendResponse(c, "backup_start", drive)
}

func handleBackupClose(c *websocket.Conn, drive string) error {
	syslog.L.Infof("Received closure request for drive %s.", drive)
	cleanupExistingSession(drive)

	// Mark backup as complete
	backupStatus := agent.GetBackupStatus()
	backupStatus.EndBackup(drive)

	return sendResponse(c, "backup_close", drive)
}

func WSHandler(ctx context.Context, c *websocket.Conn, m websockets.Message) {
	if c == nil {
		syslog.L.Error("nil WebSocket connection")
		return
	}

	// Add timeout to context if needed
	// ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	// defer cancel()

	var err error
	switch m.Type {
	case "backup_start":
		err = handleBackupStart(ctx, c, m.Content)
	case "backup_close":
		err = handleBackupClose(c, m.Content)
	default:
		err = fmt.Errorf("unknown message type: %s", m.Type)
	}

	if err != nil {
		syslog.L.Error(err.Error())
	}
}
