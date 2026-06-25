package sync

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

	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/safemap"
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
	log.Info("received backup request for job", "id", reqData.BackupID)
	log.Info("forking process for backup job", "id", reqData.BackupID)
	backupMode, pid, err := cli.ExecBackup(reqData.SourceMode, reqData.ReadMode, reqData.Drive, reqData.BackupID)
	if err != nil {
		log.Error(err, "forking process for backup job", "id", reqData.BackupID)
		if pid != -1 {
			if runtime.GOOS == "windows" {
				timeout := time.Second * 5
				if err := winquit.QuitProcess(pid, timeout); err != nil {
					log.Error(err,
						"failed to send signal for graceful shutdown",
						"jobID", reqData.BackupID)

				}
			} else {
				process, err := os.FindProcess(pid)
				if err == nil {
					if sigErr := process.Signal(syscall.SIGTERM); sigErr != nil {
						log.Error(sigErr,
							"failed to send SIGTERM",
							"id", reqData.BackupID)

					}
				}
			}
		}
		return arpc.Response{}, err
	}

	activePids.Set(reqData.BackupID, pid)

	return arpc.Response{Status: 200, Message: backupMode}, nil
}

func BackupCloseHandler(req *arpc.Request) (arpc.Response, error) {
	var reqData types.BackupReq
	err := cbor.Unmarshal(req.Payload, &reqData)
	if err != nil {
		return arpc.Response{}, err
	}
	log.Info("received closure request for job", "id", reqData.BackupID)

	pid, ok := activePids.Get(reqData.BackupID)
	if ok {
		log.Info("killing child process",

			"pid", pid, "id", reqData.BackupID)

		activePids.Del(reqData.BackupID)
		if runtime.GOOS == "windows" {
			timeout := time.Second * 5
			if err := winquit.QuitProcess(pid, timeout); err != nil {
				log.Error(err,
					"failed to send signal for graceful shutdown",
					"jobID", reqData.BackupID)

			}
		} else {
			process, err := os.FindProcess(pid)
			if err == nil {
				if sigErr := process.Signal(syscall.SIGTERM); sigErr != nil {
					log.Error(sigErr,
						"failed to send SIGTERM",
						"id", reqData.BackupID)

				}
			}
		}
	} else {
		log.Info("no pid found to kill for cleanup",
			"id", reqData.BackupID)

	}

	return arpc.Response{Status: 200, Message: "success"}, nil
}

func StatusHandler(req *arpc.Request) (arpc.Response, error) {
	var reqData types.TargetStatusReq
	if err := cbor.Unmarshal(req.Payload, &reqData); err != nil {
		log.Error(err, "status handler unmarshal error")
		return arpc.Response{}, err
	}

	res, err := CheckDriveStatus(reqData.Drive, reqData.Subpath)
	if err != nil {
		log.Error(err, "check drive status error")
		return arpc.Response{}, err
	}

	if res.IsLocked {
		err = fmt.Errorf("%s", res.Message)
		log.Error(err, "check drive status error")
		return arpc.Response{}, err
	}

	if !res.IsReachable {
		err = fmt.Errorf("%s", res.Message)
		log.Error(err, "check drive status error")
		return arpc.Response{}, err
	}

	return arpc.Response{
		Status:  200,
		Message: fmt.Sprintf("reachable|%s", conf.Version),
	}, nil
}
