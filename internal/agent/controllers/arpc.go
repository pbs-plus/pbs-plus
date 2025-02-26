//go:build windows

package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/vssfs"
	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
)

var (
	activeSessions   map[string]*backupSession
	activeSessionsMu sync.Mutex
)

type backupSession struct {
	jobId    string
	ctx      context.Context
	cancel   context.CancelFunc
	store    *agent.BackupStore
	snapshot *snapshots.WinVSSSnapshot
	fs       *vssfs.VSSFSServer
	once     sync.Once
}

func (s *backupSession) Close() {
	s.once.Do(func() {
		if s.fs != nil {
			s.fs.Close()
		}
		if s.snapshot != nil {
			s.snapshot.Close()
		}
		if s.store != nil {
			_ = s.store.EndBackup(s.jobId)
		}
		activeSessionsMu.Lock()
		delete(activeSessions, s.jobId)
		activeSessionsMu.Unlock()
		s.cancel()
	})
}

type BackupReq struct {
	JobId string `json:"job_id"`
	Drive string `json:"drive"`
}

func BackupStartHandler(req arpc.Request, router *arpc.Router) (arpc.Response, error) {
	var reqData BackupReq
	if err := json.Unmarshal(req.Payload, &reqData); err != nil {
		return arpc.Response{Status: 400, Message: "invalid payload"}, err
	}

	syslog.L.Infof("Received backup request for job: %s.", reqData.JobId)

	store, err := agent.NewBackupStore()
	if err != nil {
		return arpc.Response{Status: 500, Message: err.Error()}, err
	}
	activeSessionsMu.Lock()
	if activeSessions == nil {
		activeSessions = make(map[string]*backupSession)
	}
	if existingSession, ok := activeSessions[reqData.JobId]; ok {
		existingSession.Close()
	}
	sessionCtx, cancel := context.WithCancel(context.Background())
	session := &backupSession{
		jobId:  reqData.JobId,
		ctx:    sessionCtx,
		cancel: cancel,
		store:  store,
	}
	activeSessions[reqData.JobId] = session
	activeSessionsMu.Unlock()

	if hasActive, err := store.HasActiveBackupForJob(reqData.JobId); hasActive || err != nil {
		if err != nil {
			return arpc.Response{Status: 500, Message: err.Error()}, err
		}
		err = fmt.Errorf("existing backup")
		return arpc.Response{Status: 500, Message: err.Error()}, err
	}

	if err := store.StartBackup(reqData.JobId); err != nil {
		session.Close()
		return arpc.Response{Status: 500, Message: err.Error()}, err
	}

	snapshot, err := snapshots.Snapshot(reqData.JobId, reqData.Drive)
	if err != nil {
		session.Close()
		return arpc.Response{Status: 500, Message: err.Error()}, err
	}
	session.snapshot = snapshot

	fs := vssfs.NewVSSFSServer(reqData.JobId, snapshot.SnapshotPath)
	fs.RegisterHandlers(router)
	session.fs = fs

	return arpc.Response{Status: 200, Message: "success"}, nil
}

func BackupCloseHandler(req arpc.Request) (arpc.Response, error) {
	var reqData BackupReq
	if err := json.Unmarshal(req.Payload, &reqData); err != nil {
		return arpc.Response{Status: 400, Message: "invalid payload"}, err
	}

	syslog.L.Infof("Received closure request for job %s.", reqData.JobId)

	activeSessionsMu.Lock()
	session, ok := activeSessions[reqData.JobId]
	activeSessionsMu.Unlock()

	if !ok {
		err := fmt.Errorf("no ongoing backup")
		return arpc.Response{Status: 500, Message: err.Error()}, err
	}

	session.Close()
	return arpc.Response{Status: 200, Message: "success"}, nil
}
