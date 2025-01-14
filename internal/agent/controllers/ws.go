//go:build windows

package controllers

import (
	"context"
	"errors"
	"sync"

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

func sendResponse(c *websockets.WSClient, msgType, content string) {
	response := websockets.Message{
		Type:    "response-" + msgType,
		Content: "Acknowledged: " + content,
	}

	c.Send(response)
}

func cleanupExistingSession(drive string) {
	if existing, ok := sftpSessions.LoadAndDelete(drive); ok && existing != nil {
		if session, ok := existing.(*sftp.SFTPSession); ok && session != nil {
			session.Close()
			syslog.L.Infof("Cancelled existing backup context of drive %s.", drive)
		}
	}
}

func BackupStartHandler(c *websockets.WSClient) func(msg *websockets.Message) {
	return func(msg *websockets.Message) {
		drive := msg.Content
		syslog.L.Infof("Received backup request for drive %s.", drive)

		// Get backup status singleton and mark backup as started
		backupStatus := agent.GetBackupStatus()
		backupStatus.StartBackup(drive)
		defer backupStatus.EndBackup(drive) // Ensure we mark backup as complete even if there's an error

		snapshot, err := snapshots.Snapshot(drive)
		if err != nil {
			syslog.L.Errorf("snapshot error: %v", err)
			return
		}
		syslog.L.Infof("Snapshot of drive %s has been made.", drive)

		sftpSession := sftp.NewSFTPSession(context.Background(), snapshot, drive)
		if sftpSession == nil {
			syslog.L.Error("SFTP session is nil.")
			return
		}

		cleanupExistingSession(drive)

        go func() {
            defer func() {
                if r := recover(); r != nil {
                    syslog.L.Errorf("Panic in SFTP session for drive %s: %v", drive, r)
                }
                cleanupExistingSession(drive)
                backupStatus.EndBackup(drive)
            }()
            sftpSession.Serve()
        }()

		syslog.L.Infof("SFTP access to snapshot of drive %s has been made.", drive)
		sftpSessions.Store(drive, sftpSession)

		sendResponse(c, "backup_start", drive)
	}
}

func BackupCloseHandler(c *websockets.WSClient) func(msg *websockets.Message) {
	return func(msg *websockets.Message) {
		drive := msg.Content

		syslog.L.Infof("Received closure request for drive %s.", drive)
		cleanupExistingSession(drive)

		backupStatus := agent.GetBackupStatus()
		backupStatus.EndBackup(drive)

		sendResponse(c, "backup_close", drive)
	}
}
