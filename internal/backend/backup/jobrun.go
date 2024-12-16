//go:build linux

package backup

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/backend/mount"
	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
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

	srcPath = filepath.Join(srcPath, job.Subpath)

	taskChan := make(chan store.Task)
	logChan := make(chan string, 100)

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

	cmdStdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("RunBackup: error creating stdout pipe -> %w", err)
	}
	cmdStderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("RunBackup: error creating stderr pipe -> %w", err)
	}

	go func() {
		defer close(logChan)
		readers := io.MultiReader(cmdStdout, cmdStderr)
		scanner := bufio.NewScanner(readers)
		for scanner.Scan() {
			logChan <- scanner.Text()
		}
		if err := scanner.Err(); err != nil {
			log.Printf("Log reader error: %v", err)
		}
	}()

	err = cmd.Start()
	if err != nil {
		if agentMount != nil {
			agentMount.Unmount()
		}
		cancel()
		return nil, fmt.Errorf("RunBackup: proxmox-backup-client start error (%s) -> %w", cmd.String(), err)
	}

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

	logCtx, logCtxCancel := context.WithCancel(context.Background())
	go func(ctx context.Context, task *store.Task) {
		logFilePath := utils.GetTaskLogPath(task.UPID)

		logFile, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			log.Printf("Log file for task %s does not exist or cannot be opened: %v", task.UPID, err)
			return
		}
		defer logFile.Close()
		writer := bufio.NewWriter(logFile)

		taskError := ""
		hasLogs := false

		for {
			formattedTime := time.Now().Format(time.RFC3339)

			select {
			case <-ctx.Done():
				if taskError == "" && hasLogs {
					_, err := writer.WriteString(formattedTime + ": TASK OK\n")
					if err != nil {
						log.Printf("Failed to write logs for task %s: %v", task.UPID, err)
						return
					}
				} else if taskError != "" {
					_, err := writer.WriteString(formattedTime + ": " + taskError + "\n")
					if err != nil {
						log.Printf("Failed to write logs for task %s: %v", task.UPID, err)
						return
					}
				}
				writer.Flush()
				return
			case logLine, ok := <-logChan:
				if !ok {
					continue
				}

				hasLogs = true

				if strings.Contains(logLine, "Error: upload failed:") {
					taskError = strings.Replace(logLine, "Error:", "TASK ERROR:", 1)
					continue
				}

				_, err := writer.WriteString(formattedTime + ": " + logLine + "\n")
				if err != nil {
					log.Printf("Failed to write logs for task %s: %v", task.UPID, err)
					return
				}
				writer.Flush()
			}
		}
	}(logCtx, task)

	go func(currJob *store.Job, currTask *store.Task) {
		defer func() {
			if waitChan != nil {
				close(waitChan)
			}
			logCtxCancel()
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
