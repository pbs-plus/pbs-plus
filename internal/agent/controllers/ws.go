//go:build windows

package controllers

import (
	"context"
	"sync"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/nfs"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/websockets"
)

var (
	nfsSessions sync.Map
)

func sendResponse(c *websockets.WSClient, msgType, content string) {
	response := websockets.Message{
		Type:    "response-" + msgType,
		Content: "Acknowledged: " + content,
	}

	c.Send(response)
}

func cleanupExistingSession(drive string) {
	if existing, ok := nfsSessions.LoadAndDelete(drive); ok && existing != nil {
		if session, ok := existing.(*nfs.NFSSession); ok && session != nil {
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

		nfsSession := nfs.NewNFSSession(context.Background(), snapshot, drive)
		if nfsSession == nil {
			syslog.L.Error("NFS session is nil.")
			return
		}

		cleanupExistingSession(drive)

		go func() {
			defer func() {
				if r := recover(); r != nil {
					syslog.L.Errorf("Panic in NFS session for drive %s: %v", drive, r)
				}
				cleanupExistingSession(drive)
				backupStatus.EndBackup(drive)
			}()
			nfsSession.Serve()
		}()

		syslog.L.Infof("NFS access to snapshot of drive %s has been made.", drive)
		nfsSessions.Store(drive, nfsSession)

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
