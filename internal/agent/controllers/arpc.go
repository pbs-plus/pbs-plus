//go:build windows

package controllers

import (
	"context"
	"fmt"
	"sync"

	"github.com/alphadose/haxmap"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/vssfs"
	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils/hashmap"
)

var (
	activeSessions *haxmap.Map[string, *backupSession]
)

func init() {
	activeSessions = hashmap.New[*backupSession]()
}

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
		activeSessions.Del(s.jobId)
		s.cancel()
	})
}

func BackupStartHandler(req arpc.Request, rpcSess *arpc.Session) (*arpc.Response, error) {
	var reqData vssfs.BackupReq
	_, err := reqData.UnmarshalMsg(req.Payload)
	if err != nil {
		return nil, err
	}

	syslog.L.Infof("Received backup request for job: %s.", reqData.JobId)

	store, err := agent.NewBackupStore()
	if err != nil {
		return nil, err
	}
	if existingSession, ok := activeSessions.Get(reqData.JobId); ok {
		existingSession.Close()
	}
	sessionCtx, cancel := context.WithCancel(context.Background())
	session := &backupSession{
		jobId:  reqData.JobId,
		ctx:    sessionCtx,
		cancel: cancel,
		store:  store,
	}
	activeSessions.Set(reqData.JobId, session)

	if hasActive, err := store.HasActiveBackupForJob(reqData.JobId); hasActive || err != nil {
		if err != nil {
			return nil, err
		}
		err = fmt.Errorf("existing backup")
		return nil, err
	}

	if err := store.StartBackup(reqData.JobId); err != nil {
		session.Close()
		return nil, err
	}

	snapshot, err := snapshots.Snapshot(reqData.JobId, reqData.Drive)
	if err != nil {
		session.Close()
		return nil, err
	}
	session.snapshot = snapshot

	fs := vssfs.NewVSSFSServer(reqData.JobId, snapshot)
	if fs == nil {
		session.Close()
		return nil, fmt.Errorf("fs is nil")
	}
	fs.RegisterHandlers(rpcSess.GetRouter())
	session.fs = fs

	return &arpc.Response{Status: 200, Message: "success"}, nil
}

func BackupCloseHandler(req arpc.Request) (*arpc.Response, error) {
	var reqData vssfs.BackupReq
	_, err := reqData.UnmarshalMsg(req.Payload)
	if err != nil {
		return nil, err
	}

	syslog.L.Infof("Received closure request for job %s.", reqData.JobId)

	session, ok := activeSessions.Get(reqData.JobId)
	if !ok {
		err := fmt.Errorf("no ongoing backup")
		return nil, err
	}

	session.Close()
	return &arpc.Response{Status: 200, Message: "success"}, nil
}
