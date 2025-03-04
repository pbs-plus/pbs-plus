//go:build windows

package controllers

import (
	"context"
	"fmt"
	"sync"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/vssfs"
	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils/safemap"
)

var (
	activeSessions *safemap.Map[string, *backupSession]
)

func init() {
	activeSessions = safemap.New[string, *backupSession]()
}

type backupSession struct {
	jobId    string
	ctx      context.Context
	cancel   context.CancelFunc
	store    *agent.BackupStore
	snapshot snapshots.WinVSSSnapshot
	fs       *vssfs.VSSFSServer
	once     sync.Once
}

func (s *backupSession) Close() {
	s.once.Do(func() {
		if s.fs != nil {
			s.fs.Close()
		}
		if s.snapshot != (snapshots.WinVSSSnapshot{}) {
			s.snapshot.Close()
		}
		if s.store != nil {
			_ = s.store.EndBackup(s.jobId)
		}
		activeSessions.Del(s.jobId)
		s.cancel()
	})
}

func BackupStartHandler(req arpc.Request, rpcSess *arpc.Session) (arpc.Response, error) {
	var reqData vssfs.BackupReq
	_, err := reqData.UnmarshalMsg(req.Payload)
	if err != nil {
		return arpc.Response{}, err
	}

	syslog.L.Infof("Received backup request for job: %s.", utils.ToString(reqData.JobId))

	store, err := agent.NewBackupStore()
	if err != nil {
		return arpc.Response{}, err
	}
	if existingSession, ok := activeSessions.Get(utils.ToString(reqData.JobId)); ok {
		existingSession.Close()
	}
	sessionCtx, cancel := context.WithCancel(context.Background())
	session := &backupSession{
		jobId:  utils.ToString(reqData.JobId),
		ctx:    sessionCtx,
		cancel: cancel,
		store:  store,
	}
	activeSessions.Set(utils.ToString(reqData.JobId), session)

	if hasActive, err := store.HasActiveBackupForJob(utils.ToString(reqData.JobId)); hasActive || err != nil {
		if err != nil {
			return arpc.Response{}, err
		}
		err = fmt.Errorf("existing backup")
		return arpc.Response{}, err
	}

	if err := store.StartBackup(utils.ToString(reqData.JobId)); err != nil {
		session.Close()
		return arpc.Response{}, err
	}

	snapshot, err := snapshots.Snapshot(utils.ToString(reqData.JobId), utils.ToString(reqData.Drive))
	if err != nil {
		session.Close()
		return arpc.Response{}, err
	}
	session.snapshot = snapshot

	fs := vssfs.NewVSSFSServer(utils.ToString(reqData.JobId), snapshot)
	if fs == nil {
		session.Close()
		return arpc.Response{}, fmt.Errorf("fs is nil")
	}
	fs.RegisterHandlers(rpcSess.GetRouter())
	session.fs = fs

	return arpc.Response{Status: 200, Message: utils.ToBytes("success")}, nil
}

func BackupCloseHandler(req arpc.Request) (arpc.Response, error) {
	var reqData vssfs.BackupReq
	_, err := reqData.UnmarshalMsg(req.Payload)
	if err != nil {
		return arpc.Response{}, err
	}

	syslog.L.Infof("Received closure request for job %s.", utils.ToString(reqData.JobId))

	session, ok := activeSessions.Get(utils.ToString(reqData.JobId))
	if !ok {
		err := fmt.Errorf("no ongoing backup")
		return arpc.Response{}, err
	}

	session.Close()
	return arpc.Response{Status: 200, Message: utils.ToBytes("success")}, nil
}
