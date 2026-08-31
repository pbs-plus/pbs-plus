package sync

import (
	"fmt"
	"os"
	"runtime"
	stdsync "sync"
	"syscall"
	"time"

	"github.com/containers/winquit/pkg/winquit"
	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/fswire"
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
	var reqData fswire.BackupReq
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
	var reqData fswire.BackupReq
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
	var reqData fswire.TargetStatusReq
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

// driveStatusDeadline stays below the server probe timeout so a batch always answers in time.
const driveStatusDeadline = 3 * time.Second

// StatusBatchHandler answers all drives in one response; checks run concurrently so a hung drive reports no verdict without delaying siblings.
func StatusBatchHandler(req *arpc.Request) (arpc.Response, error) {
	var reqData fswire.TargetStatusBatchReq
	if err := cbor.Unmarshal(req.Payload, &reqData); err != nil {
		log.Error(err, "status batch handler unmarshal error")
		return arpc.Response{}, err
	}

	results := make(map[string]fswire.TargetDriveStatus, len(reqData.Drives))
	var mu stdsync.Mutex
	var wg stdsync.WaitGroup
	for _, d := range reqData.Drives {
		wg.Add(1)
		go func(drive, subpath string) {
			defer wg.Done()
			st := checkDriveStatusBounded(drive, subpath)
			mu.Lock()
			results[drive] = st
			mu.Unlock()
		}(d.Drive, d.Subpath)
	}
	wg.Wait()

	data, err := cbor.Marshal(fswire.TargetStatusBatchResp{Version: conf.Version, Drives: results})
	if err != nil {
		return arpc.Response{}, err
	}
	return arpc.Response{Status: 200, Data: data}, nil
}

func checkDriveStatusBounded(drive, subpath string) fswire.TargetDriveStatus {
	done := make(chan fswire.TargetDriveStatus, 1)
	go func() {
		res, err := CheckDriveStatus(drive, subpath)
		switch {
		case err != nil:
			done <- fswire.TargetDriveStatus{Reachable: new(false), Message: err.Error()}
		case res.IsLocked || !res.IsReachable:
			done <- fswire.TargetDriveStatus{Reachable: new(false), Message: res.Message}
		default:
			done <- fswire.TargetDriveStatus{Reachable: new(true), Message: res.Message}
		}
	}()
	select {
	case st := <-done:
		return st
	case <-time.After(driveStatusDeadline):
		return fswire.TargetDriveStatus{Message: "drive check timed out"}
	}
}
