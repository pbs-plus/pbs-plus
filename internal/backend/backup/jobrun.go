//go:build linux

package backup

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/backend/mount"
	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
)

func RunBackup(job *store.Job, storeInstance *store.Store, waitChan chan struct{}) (*store.Task, error) {
	if storeInstance.APIToken == nil {
		return nil, fmt.Errorf("RunBackup: api token is required")
	}

	target, err := storeInstance.GetTarget(job.Target)
	if err != nil {
		return nil, fmt.Errorf("RunBackup -> %w", err)
	}

	if target == nil {
		return nil, fmt.Errorf("RunBackup: Target '%s' does not exist.", job.Target)
	}

	if !target.ConnectionStatus {
		return nil, fmt.Errorf("RunBackup: Target '%s' is unreachable or does not exist.", job.Target)
	}

	srcPath := target.Path
	isAgent := strings.HasPrefix(target.Path, "agent://")

	var agentMount *mount.AgentMount
	if isAgent {
		agentMount, err = mount.Mount(target)
		if err != nil {
			return nil, fmt.Errorf("RunBackup: mount initialization error -> %w", err)
		}
		err = agentMount.Cmd.Wait()
		if err != nil {
			return nil, fmt.Errorf("RunBackup: mount wait error -> %w", err)
		}

		srcPath = agentMount.Path
	}

	jobStore := fmt.Sprintf(
		"%s@localhost:%s",
		storeInstance.APIToken.TokenId,
		job.Store,
	)

	hostname, err := os.Hostname()
	if err != nil {
		hostnameFile, err := os.ReadFile("/etc/hostname")
		if err != nil {
			hostname = "localhost"
		}

		hostname = strings.TrimSpace(string(hostnameFile))
	}

	backupId := hostname
	if isAgent {
		backupId = strings.TrimSpace(strings.Split(target.Name, " - ")[0])
	}

	cmdArgs := []string{
		"backup",
		fmt.Sprintf("%s.pxar:%s", strings.ReplaceAll(job.Target, " ", "-"), srcPath),
		"--repository",
		jobStore,
		"--change-detection-mode=metadata",
		"--backup-id", backupId,
	}

	if job.Namespace != "" {
		cmdArgs = append(cmdArgs, "--ns")
		cmdArgs = append(cmdArgs, job.Namespace)
	} else if isAgent && job.Namespace == "" {
		newNamespace := strings.ReplaceAll(job.Target, " - ", "/")
		cmdArgs = append(cmdArgs, "--ns")
		cmdArgs = append(cmdArgs, strings.ReplaceAll(job.Target, " - ", "/"))

		_ = CreateNamespace(newNamespace, job, storeInstance)
	}

	_ = FixDatastore(job, storeInstance)

	cmd := exec.Command("/usr/bin/proxmox-backup-client", cmdArgs...)
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, fmt.Sprintf("PBS_PASSWORD=%s", storeInstance.APIToken.Value))

	pbsStatus, err := storeInstance.GetPBSStatus()
	if err == nil {
		if fingerprint, ok := pbsStatus.Info["fingerprint"]; ok {
			cmd.Env = append(cmd.Env, fmt.Sprintf("PBS_FINGERPRINT=%s", fingerprint))
		}
	}

	logBuffer := bytes.Buffer{}
	writer := io.MultiWriter(os.Stdout, &logBuffer)

	cmd.Stdout = writer
	cmd.Stderr = writer

	var taskChan chan store.Task
	watchCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		taskChan, err = storeInstance.GetMostRecentTask(watchCtx, job)
		if err != nil {
			log.Printf("RunBackup: unable to monitor tasks folder -> %v\n", err)
			return
		}
	}()

	err = cmd.Start()
	if err != nil {
		if agentMount != nil {
			agentMount.Unmount()
		}
		return nil, fmt.Errorf("RunBackup: proxmox-backup-client start error (%s) -> %w", cmd.String(), err)
	}

	var task *store.Task
	go func() {
		taskC := <-taskChan
		task = &taskC
	}()

	for {
		line, err := logBuffer.ReadString('\n')
		if err != nil && line != "" {
			return nil, fmt.Errorf("RunBackup: log buffer readString error -> %w", err)
		}

		if strings.Contains(line, "Upload directory") {
			break
		}

		time.Sleep(time.Millisecond * 100)
	}

	<-watchCtx.Done()

	if task == nil {
		return nil, fmt.Errorf("RunBackup: task not found -> %w", err)
	}

	job.LastRunUpid = &task.UPID
	job.LastRunState = &task.Status

	err = storeInstance.UpdateJob(*job)
	if err != nil {
		_ = cmd.Process.Kill()
		if agentMount != nil {
			agentMount.Unmount()
		}

		return nil, fmt.Errorf("RunBackup: unable to update job -> %w", err)
	}

	go func() {
		defer func() {
			if waitChan != nil {
				close(waitChan)
			}
		}()
		syslogger, err := syslog.InitializeLogger()
		if err != nil {
			log.Printf("Failed to initialize logger: %s", err)
			return
		}

		if agentMount != nil {
			defer agentMount.Unmount()
		}
		err = cmd.Wait()
		if err != nil {
			syslogger.Errorf("RunBackup (goroutine): error waiting for backup -> %v", err)
			return
		}

		taskFound, err := storeInstance.GetTaskByUPID(task.UPID)
		if err != nil {
			syslogger.Errorf("RunBackup (goroutine): unable to get task by UPID -> %v", err)
			return
		}

		job.LastRunState = &taskFound.Status
		job.LastRunEndtime = &taskFound.EndTime

		err = storeInstance.UpdateJob(*job)
		if err != nil {
			syslogger.Errorf("RunBackup (goroutine): unable to update job -> %v", err)
			return
		}
	}()

	return &task, nil
}
