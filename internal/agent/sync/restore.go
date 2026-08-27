package sync

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/containers/winquit/pkg/winquit"
	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/fswire"
	"github.com/pbs-plus/pbs-plus/internal/agent/cli"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/log"
)

var (
	restoreStarts sync.Mutex
	restoreRuns   = make(map[string]int)
)

func RestoreStartHandler(req *arpc.Request, rpcSess *arpc.StreamPipe) (arpc.Response, error) {
	var reqData fswire.RestoreReq
	err := cbor.Unmarshal(req.Payload, &reqData)
	if err != nil {
		return arpc.Response{}, err
	}
	key := reqData.IdempotencyKey
	if key == "" {
		key = reqData.RestoreID
	}

	restoreStarts.Lock()
	defer restoreStarts.Unlock()
	if pid := restoreRuns[key]; pid != 0 {
		return arpc.Response{Status: 200, Message: fmt.Sprintf("%d", pid)}, nil
	}
	if pid, ok := activePids.Get(reqData.RestoreID); ok {
		return arpc.Response{Status: 200, Message: fmt.Sprintf("%d", pid)}, nil
	}
	log.Info("received restore request for job", "id", reqData.RestoreID)
	log.Info("forking process for restore job", "id", reqData.RestoreID)
	cmd, err := cli.ExecRestore(reqData.RestoreID, reqData.SrcPath, reqData.DestPath, reqData.Mode)
	if err != nil {
		log.Error(err, "forking process for restore job", "id", reqData.RestoreID)
		return arpc.Response{}, err
	}

	pid := cmd.Process.Pid
	activePids.Set(reqData.RestoreID, pid)
	restoreRuns[key] = pid
	go func() {
		_ = cmd.Wait()
		restoreStarts.Lock()
		if restoreRuns[key] == pid {
			delete(restoreRuns, key)
		}
		restoreStarts.Unlock()
		if current, ok := activePids.Get(reqData.RestoreID); ok && current == pid {
			activePids.Del(reqData.RestoreID)
		}
	}()

	return arpc.Response{Status: 200, Message: fmt.Sprintf("%d", pid)}, nil
}

func RestoreCloseHandler(req *arpc.Request) (arpc.Response, error) {
	var reqData fswire.RestoreCloseReq
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
