package controllers

import (
	"os"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent/agentfs/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/forks"
	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils/safemap"
)

var (
	activePids *safemap.Map[string, int]
)

func init() {
	activePids = safemap.New[string, int]()
}

func BackupStartHandler(req arpc.Request, rpcSess *arpc.Session) (arpc.Response, error) {
	var reqData types.BackupReq
	err := reqData.Decode(req.Payload)
	if err != nil {
		return arpc.Response{}, err
	}

	syslog.L.Info().WithMessage("received backup request for job").WithField("id", reqData.JobId).Write()

	backupMode, pid, err := forks.ExecBackup(reqData.SourceMode, reqData.Drive, reqData.JobId)
	if err != nil {
		if pid != -1 {
			process, err := os.FindProcess(pid)
			if err == nil {
				_ = process.Kill()
			}
		}
		return arpc.Response{}, err
	}

	activePids.Set(reqData.JobId, pid)

	return arpc.Response{Status: 200, Message: backupMode}, nil
}

func BackupCloseHandler(req arpc.Request) (arpc.Response, error) {
	var reqData types.BackupReq
	err := reqData.Decode(req.Payload)
	if err != nil {
		return arpc.Response{}, err
	}

	syslog.L.Info().WithMessage("received closure request for job").WithField("id", reqData.JobId).Write()

	pid, ok := activePids.Get(reqData.JobId)
	if ok {
		activePids.Del(reqData.JobId)
		process, err := os.FindProcess(pid)
		if err == nil {
			_ = process.Kill()
		}
	}

	return arpc.Response{Status: 200, Message: "success"}, nil
}
