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
	"sync"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/backend/mount"
	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
)

var backupMutex sync.Mutex

func RunBackup(job *store.Job, storeInstance *store.Store, waitChan chan struct{}) (*store.Task, error) {
	backupMutex.Lock()
	defer backupMutex.Unlock()

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

	taskChan := make(chan store.Task)
	watchCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		err = storeInstance.GetJobTask(watchCtx, taskChan, job)
		if err != nil {
			log.Printf("RunBackup: unable to monitor tasks folder -> %v\n", err)
			return
		}
	}()

	var task *store.Task
	go func() {
		taskC := <-taskChan
		log.Printf("Task received: %s\n", taskC.UPID)
		task = &taskC

		close(taskChan)
		cancel()
	}()

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

	for _, exclusion := range job.Exclusions {
		if isAgent && exclusion.JobID != job.ID {
			continue
		}

		cmdArgs = append(cmdArgs, "--exclude", exclusion.Path)
	}

	if job.Namespace != "" {
		_ = CreateNamespace(job.Namespace, job, storeInstance)

		cmdArgs = append(cmdArgs, "--ns", job.Namespace)
	} else if isAgent && job.Namespace == "" {
		newNamespace := strings.ReplaceAll(job.Target, " - ", "/")
		cmdArgs = append(cmdArgs, "--ns", strings.ReplaceAll(job.Target, " - ", "/"))

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
	err = cmd.Start()
	if err != nil {
		if agentMount != nil {
			agentMount.Unmount()
		}
		cancel()
		return nil, fmt.Errorf("RunBackup: proxmox-backup-client start error (%s) -> %w", cmd.String(), err)
	}

	go func(currJob *store.Job, currTask *store.Task) {
		defer func() {
			if waitChan != nil {
				close(waitChan)
			}
		}()
		syslogger, err := syslog.InitializeLogger()
		if err != nil {
			cancel()
			log.Printf("Failed to initialize logger: %s", err)
			return
		}

		if agentMount != nil {
			defer agentMount.Unmount()
		}
		err = cmd.Wait()
		if err != nil {
			cancel()
			syslogger.Errorf("RunBackup (goroutine): error waiting for backup -> %v", err)
			return
		}

		taskFound, err := storeInstance.GetTaskByUPID(currTask.UPID)
		if err != nil {
			cancel()
			syslogger.Errorf("RunBackup (goroutine): unable to get task by UPID -> %v", err)
			return
		}

		latestJob, err := storeInstance.GetJob(currJob.ID)
		if err != nil {
			cancel()
			syslogger.Errorf("RunBackup (goroutine): unable to update job -> %v", err)
			return
		}

		latestJob.LastRunState = &taskFound.Status
		latestJob.LastRunEndtime = &taskFound.EndTime

		err = storeInstance.UpdateJob(*latestJob)
		if err != nil {
			cancel()
			syslogger.Errorf("RunBackup (goroutine): unable to update job -> %v", err)
			return
		}
	}(job, task)

	log.Printf("Waiting for task: %s\n", job.ID)
	select {
	case <-taskChan:
	case <-watchCtx.Done():
	case <-time.After(time.Second * 60):
		_ = cmd.Process.Kill()
		if agentMount != nil {
			agentMount.Unmount()
		}

		return nil, fmt.Errorf("RunBackup: timeout, task not found")
	}

	if task == nil {
		_ = cmd.Process.Kill()
		if agentMount != nil {
			agentMount.Unmount()
		}

		return nil, fmt.Errorf("RunBackup: task not found")
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

	return task, nil
}
