package controllers

import (
	"fmt"
	"os"
	"runtime"
	"syscall"
	"time"

	"github.com/containers/winquit/pkg/winquit"
	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/agent/cli"
	"github.com/pbs-plus/pbs-plus/internal/agent/volumes"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils/safemap"
)

var (
	activePids *safemap.Map[string, int]
)

func init() {
	activePids = safemap.New[string, int]()
}

func BackupStartHandler(req *arpc.Request, rpcSess *arpc.StreamPipe) (arpc.Response, error) {
	var reqData types.BackupReq
	err := cbor.Unmarshal(req.Payload, &reqData)
	if err != nil {
		return arpc.Response{}, err
	}

	syslog.L.Info().WithMessage("received backup request for job").WithField("id", reqData.BackupId).Write()

	syslog.L.Info().WithMessage("forking process for backup job").WithField("id", reqData.BackupId).Write()
	backupMode, pid, err := cli.ExecBackup(reqData.SourceMode, reqData.ReadMode, reqData.Drive, reqData.BackupId)
	if err != nil {
		syslog.L.Error(err).WithMessage("forking process for backup job").WithField("id", reqData.BackupId).Write()
		if pid != -1 {
			if runtime.GOOS == "windows" {
				timeout := time.Second * 5
				if err := winquit.QuitProcess(pid, timeout); err != nil {
					syslog.L.Error(err).
						WithMessage("failed to send signal for graceful shutdown").
						WithField("jobId", reqData.BackupId).
						Write()
				}
			} else {
				process, err := os.FindProcess(pid)
				if err == nil {
					if sigErr := process.Signal(syscall.SIGTERM); sigErr != nil {
						syslog.L.Error(sigErr).
							WithMessage("failed to send SIGTERM").
							WithField("id", reqData.BackupId).
							Write()
					}
				}
			}
		}
		return arpc.Response{}, err
	}

	activePids.Set(reqData.BackupId, pid)

	return arpc.Response{Status: 200, Message: backupMode}, nil
}

func BackupCloseHandler(req *arpc.Request) (arpc.Response, error) {
	var reqData types.BackupReq
	err := cbor.Unmarshal(req.Payload, &reqData)
	if err != nil {
		return arpc.Response{}, err
	}

	syslog.L.Info().WithMessage("received closure request for job").WithField("id", reqData.BackupId).Write()

	pid, ok := activePids.Get(reqData.BackupId)
	if ok {
		syslog.L.Info().WithMessage("killing child process").
			WithField("id", reqData.BackupId).
			WithField("pid", pid).Write()

		activePids.Del(reqData.BackupId)
		if runtime.GOOS == "windows" {
			timeout := time.Second * 5
			if err := winquit.QuitProcess(pid, timeout); err != nil {
				syslog.L.Error(err).
					WithMessage("failed to send signal for graceful shutdown").
					WithField("jobId", reqData.BackupId).
					Write()
			}
		} else {
			process, err := os.FindProcess(pid)
			if err == nil {
				if sigErr := process.Signal(syscall.SIGTERM); sigErr != nil {
					syslog.L.Error(sigErr).
						WithMessage("failed to send SIGTERM").
						WithField("id", reqData.BackupId).
						Write()
				}
			}
		}
	} else {
		syslog.L.Info().WithMessage("no pid found to kill for cleanup").
			WithField("id", reqData.BackupId).
			Write()
	}

	return arpc.Response{Status: 200, Message: "success"}, nil
}

func StatusHandler(req *arpc.Request) (arpc.Response, error) {
	var reqData types.TargetStatusReq
	if err := cbor.Unmarshal(req.Payload, &reqData); err != nil {
		syslog.L.Error(err).WithMessage("status handler unmarshal error").Write()
		return arpc.Response{}, err
	}

	res, err := volumes.CheckDriveStatus(reqData.Drive, reqData.Subpath)
	if err != nil {
		syslog.L.Error(err).WithMessage("check drive status error").Write()
		return arpc.Response{}, err
	}

	if res.IsLocked {
		err = fmt.Errorf("drive is locked/encrypted")
		syslog.L.Error(err).WithMessage("status handler unmarshal error").Write()
		return arpc.Response{}, err
	}

	if !res.IsReachable {
		err = fmt.Errorf("drive is unreachable")
		syslog.L.Error(err).WithMessage("status handler unmarshal error").Write()
		return arpc.Response{}, err
	}

	return arpc.Response{
		Status:  200,
		Message: fmt.Sprintf("reachable|%s", constants.Version),
	}, nil
}
