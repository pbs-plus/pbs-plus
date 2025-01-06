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

	"github.com/alexflint/go-filemutex"
	"github.com/sonroyaalmerol/pbs-plus/internal/backend/mount"
	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

func waitForLogFile(logFilePath string, maxWait time.Duration) (*os.File, error) {
	deadline := time.Now().Add(maxWait)
	for {
		logFile, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_WRONLY, 0644)
		if err == nil {
			return logFile, nil // Successfully opened the file
		}

		if time.Now().After(deadline) {
			return nil, fmt.Errorf("log file %s not writable within %s: %v", logFilePath, maxWait, err)
		}

		time.Sleep(100 * time.Millisecond) // Retry after a short delay
	}
}

func RunBackup(job *store.Job, storeInstance *store.Store, waitChan chan struct{}) (*store.Task, error) {
	backupMutex, err := filemutex.New("/tmp/pbs-plus-mutex-lock")
	if err != nil {
		return nil, fmt.Errorf("RunBackup: failed to create mutex lock -> %w", err)
	}

	backupMutex.Lock()
	defer func() {
		backupMutex.Unlock()
		backupMutex.Close()
	}()

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

	var logLines []string
	var logMu sync.Mutex // Mutex to ensure safe access to logLines across goroutines

	go func() {
		readers := io.MultiReader(cmdStdout, cmdStderr)
		scanner := bufio.NewScanner(readers)
		for scanner.Scan() {
			logLine := scanner.Text()
			logMu.Lock()
			logLines = append(logLines, logLine) // Append log line to the array
			logMu.Unlock()
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

		// Write accumulated logs to the file
		logFilePath := utils.GetTaskLogPath(currTask.UPID)
		logFile, err := waitForLogFile(logFilePath, 5*time.Second)
		if err != nil {
			log.Printf("Log file for task %s does not exist or cannot be opened: %v", currTask.UPID, err)
			return
		}
		defer logFile.Close()

		writer := bufio.NewWriter(logFile)
		_, err = writer.WriteString("--- proxmox-backup-client log starts here ---\n")
		if err != nil {
			log.Printf("Failed to write logs for task %s: %v", currTask.UPID, err)
			return
		}

		logMu.Lock()
		for _, logLine := range logLines {
			formattedTime := time.Now().Format(time.RFC3339)
			_, err = writer.WriteString(fmt.Sprintf("%s: %s\n", formattedTime, logLine))
			if err != nil {
				logMu.Unlock()
				log.Printf("Failed to write logs for task %s: %v", currTask.UPID, err)
				return
			}
		}
		logMu.Unlock()

		writer.Flush()
	}(job, task)

	return task, nil
}
