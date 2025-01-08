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

// Constants for configuration
const (
	mutexPath     = "/tmp/pbs-plus-mutex-lock"
	taskTimeout   = 20 * time.Second
	logRetryDelay = 100 * time.Millisecond
	logWaitTime   = 5 * time.Second
)

// logBuffer handles concurrent log writing
type logBuffer struct {
	lines []string
	mu    sync.Mutex
}

func (lb *logBuffer) append(line string) {
	lb.mu.Lock()
	lb.lines = append(lb.lines, line)
	lb.mu.Unlock()
}

func (lb *logBuffer) getLines() []string {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	return append([]string{}, lb.lines...)
}

func waitForLogFile(logFilePath string, maxWait time.Duration) (*os.File, error) {
	deadline := time.Now().Add(maxWait)
	for time.Now().Before(deadline) {
		if logFile, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_WRONLY, 0644); err == nil {
			return logFile, nil
		}
		time.Sleep(logRetryDelay)
	}
	return nil, fmt.Errorf("log file %s not writable within %s", logFilePath, maxWait)
}

func RunBackup(job *store.Job, storeInstance *store.Store) (*store.Task, error) {
	// Create and acquire mutex
	backupMutex, err := filemutex.New(mutexPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create mutex lock: %w", err)
	}
	backupMutex.Lock()
	defer func() {
		backupMutex.Unlock()
		backupMutex.Close()
	}()

	// Validate prerequisites
	if storeInstance.APIToken == nil {
		return nil, fmt.Errorf("api token is required")
	}

	target, err := storeInstance.GetTarget(job.Target)
	if err != nil {
		return nil, fmt.Errorf("target error: %w", err)
	}
	if target == nil || !target.ConnectionStatus {
		return nil, fmt.Errorf("target '%s' is unreachable or does not exist", job.Target)
	}

	// Handle agent mounting
	srcPath := target.Path
	var agentMount *mount.AgentMount
	if strings.HasPrefix(target.Path, "agent://") {
		if agentMount, err = mountAgent(storeInstance, target); err != nil {
			return nil, err
		}
		srcPath = agentMount.Path
	}
	srcPath = filepath.Join(srcPath, job.Subpath)

	// Start task monitoring before executing backup
	taskChan := make(chan store.Task)
	watchCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := storeInstance.GetJobTask(watchCtx, taskChan, job); err != nil {
			log.Printf("unable to monitor tasks folder: %v", err)
			cancel()
		}
	}()

	// Prepare backup command
	cmd, buffer, err := prepareBackupCommand(job, storeInstance, srcPath)
	if err != nil {
		cleanup(agentMount)
		return nil, err
	}

	// Start the backup command
	if err := cmd.Start(); err != nil {
		cleanup(agentMount)
		return nil, fmt.Errorf("proxmox-backup-client start error: %w", err)
	}

	// Wait for task creation with timeout
	var task *store.Task
	select {
	case taskReceived := <-taskChan:
		task = &taskReceived
		log.Printf("Task received: %s\n", task.UPID)
	case <-time.After(taskTimeout):
		_ = cmd.Process.Kill()
		cleanup(agentMount)
		return nil, fmt.Errorf("timeout waiting for task creation")
	}

	if task == nil {
		_ = cmd.Process.Kill()
		cleanup(agentMount)
		return nil, fmt.Errorf("task not found")
	}

	// Update job with task information
	job.LastRunUpid = &task.UPID
	job.LastRunState = &task.Status
	if err := storeInstance.UpdateJob(*job); err != nil {
		_ = cmd.Process.Kill()
		cleanup(agentMount)
		return nil, fmt.Errorf("unable to update job: %w", err)
	}

	// Handle backup completion in background
	go handleBackupCompletion(cmd, buffer, job, task, storeInstance, agentMount)

	return task, nil
}

func mountAgent(storeInstance *store.Store, target *store.Target) (*mount.AgentMount, error) {
	agentMount, err := mount.Mount(storeInstance.WSHub, target)
	if err != nil {
		return nil, fmt.Errorf("mount initialization error: %w", err)
	}
	if err := agentMount.Cmd.Wait(); err != nil {
		return nil, fmt.Errorf("mount wait error: %w", err)
	}
	return agentMount, nil
}

func prepareBackupCommand(job *store.Job, storeInstance *store.Store, srcPath string) (*exec.Cmd, *logBuffer, error) {
	cmdArgs := buildCommandArgs(job, storeInstance, srcPath)
	cmd := exec.Command("/usr/bin/proxmox-backup-client", cmdArgs...)
	
	if err := setupCommandEnvironment(cmd, storeInstance); err != nil {
		return nil, nil, err
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, fmt.Errorf("error creating stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, nil, fmt.Errorf("error creating stderr pipe: %w", err)
	}

	buffer := &logBuffer{}
	go monitorCommandOutput(stdout, stderr, buffer)

	return cmd, buffer, nil
}

func buildCommandArgs(job *store.Job, storeInstance *store.Store, srcPath string) []string {
	hostname := getHostname()
	backupId := hostname
	if strings.HasPrefix(job.Target, "agent://") {
		backupId = strings.TrimSpace(strings.Split(job.Target, " - ")[0])
	}

	args := []string{
		"backup",
		fmt.Sprintf("%s.pxar:%s", strings.ReplaceAll(job.Target, " ", "-"), srcPath),
		"--repository", fmt.Sprintf("%s@localhost:%s", storeInstance.APIToken.TokenId, job.Store),
		"--change-detection-mode=metadata",
		"--backup-id", backupId,
		"--crypt-mode=none",
		"--skip-e2big-xattr", "true",
		"--skip-lost-and-found", "true",
	}

	for _, exclusion := range job.Exclusions {
		if strings.HasPrefix(job.Target, "agent://") && exclusion.JobID != job.ID {
			continue
		}
		args = append(args, "--exclude", exclusion.Path)
	}

	if job.Namespace != "" {
		_ = CreateNamespace(job.Namespace, job, storeInstance)
		args = append(args, "--ns", job.Namespace)
	}

	return args
}

func monitorCommandOutput(stdout, stderr io.ReadCloser, buffer *logBuffer) {
	scanner := bufio.NewScanner(io.MultiReader(stdout, stderr))
	for scanner.Scan() {
		buffer.append(scanner.Text())
	}
}

func handleBackupCompletion(cmd *exec.Cmd, buffer *logBuffer, job *store.Job, task *store.Task, storeInstance *store.Store, agentMount *mount.AgentMount) {
	syslogger, err := syslog.InitializeLogger()
	if err != nil {
		log.Printf("Failed to initialize logger: %v", err)
		return
	}

	if agentMount != nil {
		defer agentMount.Unmount()
		defer agentMount.CloseSFTP()
	}

	if err := cmd.Wait(); err != nil {
		syslogger.Errorf("error waiting for backup: %v", err)
		return
	}

	if err := writeLogsToFile(task.UPID, buffer.getLines()); err != nil {
		syslogger.Errorf("failed to write logs: %v", err)
		return
	}

	if err := updateJobStatus(job, task, storeInstance); err != nil {
		syslogger.Errorf("failed to update job status: %v", err)
	}
}

func cleanup(agentMount *mount.AgentMount) {
	if agentMount != nil {
		agentMount.Unmount()
		agentMount.CloseSFTP()
	}
}

func getHostname() string {
	if hostname, err := os.Hostname(); err == nil {
		return hostname
	}
	if hostnameFile, err := os.ReadFile("/etc/hostname"); err == nil {
		return strings.TrimSpace(string(hostnameFile))
	}
	return "localhost"
}

func setupCommandEnvironment(cmd *exec.Cmd, storeInstance *store.Store) error {
	cmd.Env = append(os.Environ(), 
		fmt.Sprintf("PBS_PASSWORD=%s", storeInstance.APIToken.Value))

	if pbsStatus, err := storeInstance.GetPBSStatus(); err == nil {
		if fingerprint, ok := pbsStatus.Info["fingerprint"]; ok {
			cmd.Env = append(cmd.Env, fmt.Sprintf("PBS_FINGERPRINT=%s", fingerprint))
		}
	}
	return nil
}

func writeLogsToFile(upid string, logLines []string) error {
	logFile, err := waitForLogFile(utils.GetTaskLogPath(upid), logWaitTime)
	if err != nil {
		return err
	}
	defer logFile.Close()

	writer := bufio.NewWriter(logFile)
	if _, err := writer.WriteString("--- proxmox-backup-client log starts here ---\n"); err != nil {
		return err
	}

	hasError := false
	errorString := ""
	formattedTime := time.Now().Format(time.RFC3339)

	for _, line := range logLines {
		if strings.Contains(line, "Error: upload failed:") {
			errorString = strings.Replace(line, "Error:", "TASK ERROR:", 1)
			hasError = true
			continue
		}
		if _, err := writer.WriteString(fmt.Sprintf("%s: %s\n", formattedTime, line)); err != nil {
			return err
		}
	}

	if hasError {
		_, err = writer.WriteString(fmt.Sprintf("%s: %s", formattedTime, errorString))
	} else {
		_, err = writer.WriteString(formattedTime + ": TASK OK")
	}

	if err != nil {
		return err
	}

	return writer.Flush()
}

func updateJobStatus(job *store.Job, task *store.Task, storeInstance *store.Store) error {
	taskFound, err := storeInstance.GetTaskByUPID(task.UPID)
	if err != nil {
		return err
	}

	latestJob, err := storeInstance.GetJob(job.ID)
	if err != nil {
		return err
	}

	latestJob.LastRunState = &taskFound.Status
	latestJob.LastRunEndtime = &taskFound.EndTime

	return storeInstance.UpdateJob(*latestJob)
}