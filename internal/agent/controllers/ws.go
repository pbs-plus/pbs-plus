//go:build windows

package controllers

import (
	"context"
	"fmt"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/nfs"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/websockets"
)

func sendResponse(c *websockets.WSClient, msgType, content string) {
	response := websockets.Message{
		Type:    "response-" + msgType,
		Content: "Acknowledged: " + content,
	}

	c.Send(context.Background(), response)
}

func BackupStartHandler(c *websockets.WSClient) func(ctx context.Context, msg *websockets.Message) error {
	return func(ctx context.Context, msg *websockets.Message) error {
		drive := msg.Content
		syslog.L.Infof("Received backup request for drive %s.", drive)

		store := GetNFSSessionStore()
		if err := store.Delete(drive); err != nil {
			syslog.L.Errorf("Error cleaning up session store: %v", err)
		}

		backupStatus := agent.GetBackupStatus()
		backupStatus.StartBackup(drive)
		defer backupStatus.EndBackup(drive)

		snapshot, err := snapshots.Snapshot(drive)
		if err != nil {
			syslog.L.Errorf("snapshot error: %v", err)
			return err
		}

		nfsSession := nfs.NewNFSSession(context.Background(), snapshot, drive)
		if nfsSession == nil {
			syslog.L.Error("NFS session is nil.")
			return fmt.Errorf("NFS session is nil.")
		}

		if err := store.Store(drive, nfsSession); err != nil {
			syslog.L.Errorf("Error storing session: %v", err)
		}

		go func() {
			defer func() {
				if r := recover(); r != nil {
					syslog.L.Errorf("Panic in NFS session for drive %s: %v", drive, r)
				}
				if err := store.Delete(drive); err != nil {
					syslog.L.Errorf("Error cleaning up session store: %v", err)
				}
				backupStatus.EndBackup(drive)
			}()
			nfsSession.Serve()
		}()

		sendResponse(c, "backup_start", drive)
		return nil
	}
}

func BackupCloseHandler(c *websockets.WSClient) func(ctx context.Context, msg *websockets.Message) error {
	return func(ctx context.Context, msg *websockets.Message) error {
		drive := msg.Content
		syslog.L.Infof("Received closure request for drive %s.", drive)

		store := GetNFSSessionStore()
		if err := store.Delete(drive); err != nil {
			syslog.L.Errorf("Error cleaning up session store: %v", err)
			return err
		}

		backupStatus := agent.GetBackupStatus()
		backupStatus.EndBackup(drive)

		sendResponse(c, "backup_close", drive)
		return nil
	}
}
