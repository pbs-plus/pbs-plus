package controllers

import (
	"os"
	"runtime"
	"syscall"
	"time"

	"github.com/containers/winquit/pkg/winquit"
	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/agent/cli"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

func RestoreStartHandler(req *arpc.Request, rpcSess *arpc.StreamPipe) (arpc.Response, error) {
	var reqData types.RestoreReq
	err := cbor.Unmarshal(req.Payload, &reqData)
	if err != nil {
		return arpc.Response{}, err
	}

	syslog.L.Info().WithMessage("received restore request for job").WithField("id", reqData.RestoreId).Write()

	syslog.L.Info().WithMessage("forking process for restore job").WithField("id", reqData.RestoreId).Write()
	pid, err := cli.ExecRestore(reqData.RestoreId, reqData.SrcPath, reqData.DestPath, reqData.Mode)
	if err != nil {
		syslog.L.Error(err).WithMessage("forking process for restore job").WithField("id", reqData.RestoreId).Write()
		if pid != -1 {
			if runtime.GOOS == "windows" {
				timeout := time.Second * 5
				if err := winquit.QuitProcess(pid, timeout); err != nil {
					syslog.L.Error(err).
						WithMessage("failed to send signal for graceful shutdown").
						WithField("jobId", reqData.RestoreId).
						Write()
				}
			} else {
				process, err := os.FindProcess(pid)
				if err == nil {
					if sigErr := process.Signal(syscall.SIGTERM); sigErr != nil {
						syslog.L.Error(sigErr).
							WithMessage("failed to send SIGTERM").
							WithField("id", reqData.RestoreId).
							Write()
					}
				}
			}
		}
		return arpc.Response{}, err
	}

	activePids.Set(reqData.RestoreId, pid)

	return arpc.Response{Status: 200, Message: "success"}, nil
}

func RestoreCloseHandler(req *arpc.Request) (arpc.Response, error) {
	var reqData types.RestoreCloseReq
	err := cbor.Unmarshal(req.Payload, &reqData)
	if err != nil {
		return arpc.Response{}, err
	}

	syslog.L.Info().WithMessage("received closure request for job").WithField("id", reqData.RestoreId).Write()

	pid, ok := activePids.Get(reqData.RestoreId)
	if ok {
		syslog.L.Info().WithMessage("killing child process").
			WithField("id", reqData.RestoreId).
			WithField("pid", pid).Write()

		activePids.Del(reqData.RestoreId)
		if runtime.GOOS == "windows" {
			timeout := time.Second * 5
			if err := winquit.QuitProcess(pid, timeout); err != nil {
				syslog.L.Error(err).
					WithMessage("failed to send signal for graceful shutdown").
					WithField("jobId", reqData.RestoreId).
					Write()
			}
		} else {
			process, err := os.FindProcess(pid)
			if err == nil {
				if sigErr := process.Signal(syscall.SIGTERM); sigErr != nil {
					syslog.L.Error(sigErr).
						WithMessage("failed to send SIGTERM").
						WithField("id", reqData.RestoreId).
						Write()
				}
			}
		}
	} else {
		syslog.L.Info().WithMessage("no pid found to kill for cleanup").
			WithField("id", reqData.RestoreId).
			Write()
	}

	return arpc.Response{Status: 200, Message: "success"}, nil
}
