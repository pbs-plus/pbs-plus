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
	activeSessions   map[string]*backupSession
	activeSessionsMu sync.Mutex
)

type backupSession struct {
	drive      string
	ctx        context.Context
	cancel     context.CancelFunc
	store      *agent.BackupStore
	snapshot   *snapshots.WinVSSSnapshot
	nfsSession *nfs.NFSSession
	once       sync.Once
}

func (s *backupSession) Close() {
	s.once.Do(func() {
		if s.nfsSession != nil {
			s.nfsSession.Close()
		}
		if s.snapshot != nil {
			s.snapshot.Close()
		}
		if s.store != nil {
			_ = s.store.EndBackup(s.drive)
		}
		activeSessionsMu.Lock()
		delete(activeSessions, s.drive)
		activeSessionsMu.Unlock()
		s.cancel()
	})
}

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
			sendError(c, "backup_start", drive, err.Error())
			return err
		}

		activeSessionsMu.Lock()
		if activeSessions == nil {
			activeSessions = make(map[string]*backupSession)
		}
		if existingSession, ok := activeSessions[drive]; ok {
			existingSession.Close()
		}
		sessionCtx, cancel := context.WithCancel(context.Background())
		session := &backupSession{
			drive:  drive,
			ctx:    sessionCtx,
			cancel: cancel,
			store:  store,
		}
		activeSessions[drive] = session
		activeSessionsMu.Unlock()

		if hasActive, err := store.HasActiveBackupForDrive(drive); hasActive || err != nil {
			if err != nil {
				sendError(c, "backup_start", drive, err.Error())
				return err
			}
			sendError(c, "backup_start", drive, "existing backup")
			return fmt.Errorf("existing backup")
		}

		if err := store.StartBackup(drive); err != nil {
			session.Close()
			sendError(c, "backup_start", drive, err.Error())
			return err
		}

		snapshot, err := snapshots.Snapshot(drive)
		if err != nil {
			session.Close()
			sendError(c, "backup_start", drive, err.Error())
			return err
		}
		session.snapshot = snapshot

		nfsSession := nfs.NewNFSSession(session.ctx, snapshot, drive)
		if nfsSession == nil {
			session.Close()
			sendError(c, "backup_start", drive, "NFS session failed")
			return fmt.Errorf("NFS session failed")
		}
		session.nfsSession = nfsSession

		if err := store.StartNFS(drive); err != nil {
			session.Close()
			sendError(c, "backup_start", drive, err.Error())
			return err
		}

		go func() {
			defer func() {
				if r := recover(); r != nil {
					syslog.L.Errorf("Panic in NFS session for drive %s: %v", drive, r)
				}
				session.Close()
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

		activeSessionsMu.Lock()
		session, ok := activeSessions[drive]
		activeSessionsMu.Unlock()

		if !ok {
			sendError(c, "backup_close", drive, "no ongoing backup")
			return fmt.Errorf("no ongoing backup")
		}

		session.Close()
		sendResponse(c, "backup_close", drive)
		return nil
	}
}
