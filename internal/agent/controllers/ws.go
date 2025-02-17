//go:build windows

package controllers

import (
	"context"
	"fmt"
	"sync"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/nfs"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/websockets"
)

var (
	activeBackupsCtx       map[string]context.Context
	activeBackupsCtxCancel map[string]context.CancelFunc
	activeBackupsMu        sync.Mutex
)

func sendResponse(c *websockets.WSClient, msgType, content string) {
	response := websockets.Message{
		Type:    "response-" + msgType,
		Content: "Acknowledged: " + content,
	}

	c.Send(context.Background(), response)
}

func sendError(c *websockets.WSClient, msgType, drive, content string) {
	response := websockets.Message{
		Type:    "error-" + msgType,
		Content: drive + ": " + content,
	}

	c.Send(context.Background(), response)
}

func BackupStartHandler(c *websockets.WSClient) func(ctx context.Context, msg *websockets.Message) error {
	return func(ctx context.Context, msg *websockets.Message) error {
		drive := msg.Content
		syslog.L.Infof("Received backup request for drive %s.", drive)

		store, err := agent.NewBackupStore()
		if err != nil {
			syslog.L.Errorf("backup store error: %v", err)
			sendError(c, "backup_start", drive, err.Error())
			return err
		}

		if hasActive, err := store.HasActiveBackupForDrive(drive); hasActive || err != nil {
			if err != nil {
				syslog.L.Errorf("backup store error: %v", err)
				sendError(c, "backup_start", drive, err.Error())
				return err
			}

			syslog.L.Errorf("an attempt to backup drive %s was cancelled due to existing session", drive)
			sendError(c, "backup_start", drive, "An existing backup for requested drive is currently running. Only one instance is allowed at a time.")
		}

		activeBackupsMu.Lock()

		if activeBackupsCtx == nil {
			activeBackupsCtx = make(map[string]context.Context)
		}
		if activeBackupsCtxCancel == nil {
			activeBackupsCtxCancel = make(map[string]context.CancelFunc)
		}

		if cancel, ok := activeBackupsCtxCancel[drive]; ok {
			cancel()
		}

		activeBackupsCtx[drive], activeBackupsCtxCancel[drive] = context.WithCancel(context.Background())

		activeBackupsMu.Unlock()

		err = store.StartBackup(drive)
		if err != nil {
			syslog.L.Errorf("backup store error: %v", err)
			sendError(c, "backup_start", drive, err.Error())
			return err
		}

		snapshot, err := snapshots.Snapshot(drive)
		if err != nil {
			syslog.L.Errorf("snapshot error: %v", err)
			sendError(c, "backup_start", drive, err.Error())
			_ = store.EndBackup(drive)
			return err
		}

		activeBackupsMu.Lock()
		currentCtx := activeBackupsCtx[drive]
		currentCtxCancel := activeBackupsCtxCancel[drive]
		activeBackupsMu.Unlock()

		go func() {
			<-currentCtx.Done()
			_ = store.EndBackup(drive)
			snapshot.Close()
		}()

		nfsSession := nfs.NewNFSSession(currentCtx, snapshot, drive)
		if nfsSession == nil {
			syslog.L.Error("NFS session is nil.")
			sendError(c, "backup_start", drive, "NFS session is nil.")
			_ = store.EndBackup(drive)
			snapshot.Close()
			return fmt.Errorf("NFS session is nil.")
		}

		err = store.StartNFS(drive)
		if err != nil {
			syslog.L.Errorf("backup store error: %v", err)
			sendError(c, "backup_start", drive, err.Error())
			snapshot.Close()
			return err
		}

		go func() {
			defer func() {
				if r := recover(); r != nil {
					syslog.L.Errorf("Panic in NFS session for drive %s: %v", drive, r)
				}
				_ = store.EndBackup(drive)
				snapshot.Close()
				currentCtxCancel()
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

		activeBackupsMu.Lock()
		defer activeBackupsMu.Unlock()

		if cancel, ok := activeBackupsCtxCancel[drive]; ok {
			cancel()
			sendResponse(c, "backup_close", drive)
			return nil
		} else {
			sendError(c, "backup_close", drive, "No ongoing backup for drive")
			return fmt.Errorf("No ongoing backup for drive %s", drive)
		}
	}
}
