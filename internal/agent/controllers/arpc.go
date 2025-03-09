package controllers

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/agentfs"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/agentfs/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
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
	snapshot snapshots.Snapshot
	fs       *agentfs.AgentFSServer
	once     sync.Once
}

func (s *backupSession) Close() {
	s.once.Do(func() {
		if s.fs != nil {
			s.fs.Close()
		}
		if s.snapshot != (snapshots.Snapshot{}) && !s.snapshot.Direct && s.snapshot.Handler != nil {
			s.snapshot.Handler.DeleteSnapshot(s.snapshot)
		}
		if s.store != nil {
			_ = s.store.EndBackup(s.jobId)
		}
		activeSessions.Del(s.jobId)
		s.cancel()
	})
}

func BackupStartHandler(req arpc.Request, rpcSess *arpc.Session) (arpc.Response, error) {
	var reqData types.BackupReq
	err := reqData.Decode(req.Payload)
	if err != nil {
		return arpc.Response{}, err
	}

	syslog.L.Info().WithMessage("received backup request for job").WithField("id", reqData.JobId).Write()

	store, err := agent.NewBackupStore()
	if err != nil {
		return arpc.Response{}, err
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
			return arpc.Response{}, err
		}
		err = fmt.Errorf("existing backup")
		return arpc.Response{}, err
	}

	if err := store.StartBackup(reqData.JobId); err != nil {
		session.Close()
		return arpc.Response{}, err
	}

	var snapshot snapshots.Snapshot

	switch reqData.SourceMode {
	case "direct":
		path := reqData.Drive
		if runtime.GOOS == "windows" {
			volName := filepath.VolumeName(fmt.Sprintf("%s:", reqData.Drive))
			path = volName + "\\"
		}
		snapshot = snapshots.Snapshot{
			Path:        path,
			TimeStarted: time.Now(),
			SourcePath:  reqData.Drive,
			Direct:      true,
		}
	default:
		var err error
		snapshot, err = snapshots.Manager.CreateSnapshot(reqData.JobId, reqData.Drive)
		if err != nil && snapshot == (snapshots.Snapshot{}) {
			syslog.L.Error(err).WithMessage("Warning: VSS snapshot failed and has switched to direct backup mode.").Write()

			path := reqData.Drive
			if runtime.GOOS == "windows" {
				volName := filepath.VolumeName(fmt.Sprintf("%s:", reqData.Drive))
				path = volName + "\\"
			}
			snapshot = snapshots.Snapshot{
				Path:        path,
				TimeStarted: time.Now(),
				SourcePath:  reqData.Drive,
				Direct:      true,
			}
		}
	}

	session.snapshot = snapshot

	fs := agentfs.NewAgentFSServer(reqData.JobId, snapshot)
	if fs == nil {
		session.Close()
		return arpc.Response{}, fmt.Errorf("fs is nil")
	}
	fs.RegisterHandlers(rpcSess.GetRouter())
	session.fs = fs

	return arpc.Response{Status: 200, Message: ("success")}, nil
}

func BackupCloseHandler(req arpc.Request) (arpc.Response, error) {
	var reqData types.BackupReq
	err := reqData.Decode(req.Payload)
	if err != nil {
		return arpc.Response{}, err
	}

	syslog.L.Info().WithMessage("received closure request for job").WithField("id", reqData.JobId).Write()

	session, ok := activeSessions.Get(reqData.JobId)
	if !ok {
		err := fmt.Errorf("no ongoing backup")
		return arpc.Response{}, err
	}

	session.Close()
	return arpc.Response{Status: 200, Message: ("success")}, nil
}
