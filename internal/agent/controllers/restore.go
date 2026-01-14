package controllers

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/containers/winquit/pkg/winquit"
	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/agent/forks"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils"
)

func RestoreFileTreeHandler(req *arpc.Request, rpcSess *arpc.StreamPipe) (arpc.Response, error) {
	var reqData types.FileTreeReq
	err := cbor.Unmarshal(req.Payload, &reqData)
	if err != nil {
		return arpc.Response{}, err
	}

	rawPath := filepath.Clean(reqData.SubPath)
	rawPath = strings.TrimPrefix(rawPath, filepath.VolumeName(rawPath))
	safeRequestedPath := strings.TrimLeft(rawPath, string(filepath.Separator))
	if safeRequestedPath == "." {
		safeRequestedPath = ""
	}

	localFullPath := filepath.Join(reqData.HostPath, safeRequestedPath)

	syslog.L.Info().
		WithMessage("received filetree request").
		WithField("path", localFullPath).
		Write()

	entries, err := os.ReadDir(localFullPath)
	if err != nil {
		return arpc.Response{}, err
	}

	var catalog []types.FileTreeEntry
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue
		}

		virtualItemPath := filepath.Join(safeRequestedPath, entry.Name())
		encodedPath := utils.EncodePath(virtualItemPath)

		item := types.FileTreeEntry{
			Filepath: encodedPath,
			Text:     entry.Name(),
			Leaf:     !entry.IsDir(),
			Type:     "f",
		}

		if entry.IsDir() {
			item.Type = "d"
		} else {
			item.Mtime = info.ModTime().Unix()
			item.Size = info.Size()
		}

		catalog = append(catalog, item)
	}
	resp := types.FileTreeResp{Data: catalog}

	encoded, err := cbor.Marshal(resp)
	if err != nil {
		return arpc.Response{}, err
	}

	return arpc.Response{Status: 200, Data: encoded}, nil
}

func RestoreStartHandler(req *arpc.Request, rpcSess *arpc.StreamPipe) (arpc.Response, error) {
	var reqData types.RestoreReq
	err := cbor.Unmarshal(req.Payload, &reqData)
	if err != nil {
		return arpc.Response{}, err
	}

	syslog.L.Info().WithMessage("received restore request for job").WithField("id", reqData.RestoreId).Write()

	syslog.L.Info().WithMessage("forking process for restore job").WithField("id", reqData.RestoreId).Write()
	pid, err := forks.ExecRestore(reqData.RestoreId, reqData.SrcPath, reqData.DestPath)
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
	var reqData types.RestoreReq
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
