package controllers

import (
	"os"
	"runtime"
	"syscall"
	"time"

	"github.com/containers/winquit/pkg/winquit"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/agent/forks"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils/safemap"
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

	syslog.L.Info().WithMessage("forking process for backup job").WithField("id", reqData.JobId).Write()
	backupMode, pid, err := forks.ExecBackup(reqData.SourceMode, reqData.Drive, reqData.JobId)
	if err != nil {
		syslog.L.Error(err).WithMessage("forking process for backup job").WithField("id", reqData.JobId).Write()
		if pid != -1 {
			if runtime.GOOS == "windows" {
				timeout := time.Second * 5
				if err := winquit.QuitProcess(pid, timeout); err != nil {
					syslog.L.Error(err).
						WithMessage("failed to send signal for graceful shutdown").
						WithField("jobId", reqData.JobId).
						Write()
				}
			} else {
				process, err := os.FindProcess(pid)
				if err == nil {
					if sigErr := process.Signal(syscall.SIGTERM); sigErr != nil {
						syslog.L.Error(sigErr).
							WithMessage("failed to send SIGTERM").
							WithField("id", reqData.JobId).
							Write()
					}
				}
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
		if runtime.GOOS == "windows" {
			timeout := time.Second * 5
			if err := winquit.QuitProcess(pid, timeout); err != nil {
				syslog.L.Error(err).
					WithMessage("failed to send signal for graceful shutdown").
					WithField("jobId", reqData.JobId).
					Write()
			}
		} else {
			process, err := os.FindProcess(pid)
			if err == nil {
				if sigErr := process.Signal(syscall.SIGTERM); sigErr != nil {
					syslog.L.Error(sigErr).
						WithMessage("failed to send SIGTERM").
						WithField("id", reqData.JobId).
						Write()
				}
			}
		}
	}

	return arpc.Response{Status: 200, Message: "success"}, nil
}
