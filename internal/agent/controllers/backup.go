package controllers

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/containers/winquit/pkg/winquit"
	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/agent/cli"
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
	err := cbor.Unmarshal(req.Payload, &reqData)
	if err != nil {
		return arpc.Response{}, err
	}

	prefix := reqData.Drive

	if len(prefix) == 1 {
		prefix += ":/"
	}

	if strings.ToLower(reqData.Drive) == "root" {
		prefix = "/"
	}

	fullPath := filepath.Join(prefix, reqData.Subpath)

	if _, err := os.Stat(fullPath); !os.IsNotExist(err) {
		return arpc.Response{Status: 200, Message: fmt.Sprintf("reachable|%s", constants.Version)}, nil
	} else {
		return arpc.Response{}, err
	}
}
