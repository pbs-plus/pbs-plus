package sync

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
	"github.com/pbs-plus/pbs-plus/internal/log"
)

func RestoreStartHandler(req *arpc.Request, rpcSess *arpc.StreamPipe) (arpc.Response, error) {
	var reqData types.RestoreReq
	err := cbor.Unmarshal(req.Payload, &reqData)
	if err != nil {
		return arpc.Response{}, err
	}
	log.Info("received restore request for job", "id", reqData.RestoreID)
	log.Info("forking process for restore job", "id", reqData.RestoreID)
	pid, err := cli.ExecRestore(reqData.RestoreID, reqData.SrcPath, reqData.DestPath, reqData.Mode)
	if err != nil {
		log.Error(err, "forking process for restore job", "id", reqData.RestoreID)
		if pid != -1 {
			if runtime.GOOS == "windows" {
				timeout := time.Second * 5
				if err := winquit.QuitProcess(pid, timeout); err != nil {
					log.Error(err,
						"failed to send signal for graceful shutdown",
						"jobID", reqData.RestoreID)

				}
			} else {
				process, err := os.FindProcess(pid)
				if err == nil {
					if sigErr := process.Signal(syscall.SIGTERM); sigErr != nil {
						log.Error(sigErr,
							"failed to send SIGTERM",
							"id", reqData.RestoreID)

					}
				}
			}
		}
		return arpc.Response{}, err
	}

	activePids.Set(reqData.RestoreID, pid)

	return arpc.Response{Status: 200, Message: "success"}, nil
}

func RestoreCloseHandler(req *arpc.Request) (arpc.Response, error) {
	var reqData types.RestoreCloseReq
	err := cbor.Unmarshal(req.Payload, &reqData)
	if err != nil {
		return arpc.Response{}, err
	}
	log.Info("received closure request for job", "id", reqData.RestoreID)

	pid, ok := activePids.Get(reqData.RestoreID)
	if ok {
		log.Info("killing child process",

			"pid", pid, "id", reqData.RestoreID)

		activePids.Del(reqData.RestoreID)
		if runtime.GOOS == "windows" {
			timeout := time.Second * 5
			if err := winquit.QuitProcess(pid, timeout); err != nil {
				log.Error(err,
					"failed to send signal for graceful shutdown",
					"jobID", reqData.RestoreID)

			}
		} else {
			process, err := os.FindProcess(pid)
			if err == nil {
				if sigErr := process.Signal(syscall.SIGTERM); sigErr != nil {
					log.Error(sigErr,
						"failed to send SIGTERM",
						"id", reqData.RestoreID)

				}
			}
		}
	} else {
		log.Info("no pid found to kill for cleanup",
			"id", reqData.RestoreID)

	}

	return arpc.Response{Status: 200, Message: "success"}, nil
}
