//go:build linux

package backup

import (
	"bufio"
	"context"
	"fmt"
	"io"
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
	"golang.org/x/sys/unix"
)

func waitForLogFile(logFilePath string, maxWait time.Duration) (*os.File, error) {
	// Validate inputs to prevent potential issues
	if maxWait <= 0 || maxWait > 5*time.Minute {
		return nil, fmt.Errorf("invalid timeout value: must be between 0 and 5 minutes")
	}

	if len(logFilePath) == 0 || len(logFilePath) > unix.PathMax {
		return nil, fmt.Errorf("invalid path length")
	}

	// Try immediate open first with timeout
	openChan := make(chan openResult, 1)
	go func() {
		file, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_WRONLY, 0644)
		openChan <- openResult{file: file, err: err}
	}()

	select {
	case result := <-openChan:
		if result.err == nil {
			return result.file, nil
		}
	case <-time.After(100 * time.Millisecond):
		// Initial open timed out, continue with waiting
	}

	// Ensure parent directory exists with timeout
	dirPath := filepath.Dir(logFilePath)
	mkdirChan := make(chan error, 1)
	go func() {
		mkdirChan <- os.MkdirAll(dirPath, 0755)
	}()

	select {
	case err := <-mkdirChan:
		if err != nil {
			return nil, fmt.Errorf("failed to create log directory: %w", err)
		}
	case <-time.After(500 * time.Millisecond):
		return nil, fmt.Errorf("timeout creating directory")
	}

	// Initialize inotify with rate limiting
	fd, err := unix.InotifyInit1(unix.IN_CLOEXEC | unix.IN_NONBLOCK)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize inotify: %w", err)
	}
	defer func() {
		if fd > 0 {
			unix.Close(fd)
		}
	}()

	// Add watch for both the directory and file
	wd, err := unix.InotifyAddWatch(fd, dirPath, unix.IN_CREATE|unix.IN_CLOSE_WRITE)
	if err != nil {
		return nil, fmt.Errorf("failed to add inotify watch: %w", err)
	}
	defer unix.InotifyRmWatch(fd, uint32(wd))

	// Create epoll instance
	epfd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("failed to create epoll: %w", err)
	}
	defer func() {
		if epfd > 0 {
			unix.Close(epfd)
		}
	}()

	// Add inotify fd to epoll
	event := unix.EpollEvent{
		Events: unix.EPOLLIN,
		Fd:     int32(fd),
	}
	if err := unix.EpollCtl(epfd, unix.EPOLL_CTL_ADD, fd, &event); err != nil {
		return nil, fmt.Errorf("failed to add fd to epoll: %w", err)
	}

	// Buffer pool for events to prevent repeated allocations
	bufferPool := sync.Pool{
		New: func() interface{} {
			return make([]byte, 4096) // 4KB chunks
		},
	}

	events := make([]unix.EpollEvent, 1)
	deadline := time.Now().Add(maxWait)

	// Rate limiting for file operations
	rateLimiter := time.NewTicker(50 * time.Millisecond)
	defer rateLimiter.Stop()

	// Counter for number of attempts
	attempts := 0
	maxAttempts := 1000 // Prevent infinite loops

	for {
		if attempts >= maxAttempts {
			return nil, fmt.Errorf("exceeded maximum number of attempts")
		}
		attempts++

		// Check deadline
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("timeout waiting for log file: %s", logFilePath)
		}

		// Calculate timeout for epoll
		timeout := int(time.Until(deadline).Milliseconds())
		if timeout <= 0 {
			return nil, fmt.Errorf("timeout waiting for log file: %s", logFilePath)
		}

		// Wait for events with timeout
		n, err := unix.EpollWait(epfd, events, timeout)
		if err != nil {
			if err == unix.EINTR {
				continue
			}
			return nil, fmt.Errorf("epoll wait error: %w", err)
		}

		if n == 0 {
			continue // Timeout on epoll
		}

		// Get buffer from pool
		buffer := bufferPool.Get().([]byte)
		eventProcessed := false

		// Read events with timeout
		readDone := make(chan error, 1)
		go func() {
			_, err := unix.Read(fd, buffer)
			readDone <- err
		}()

		select {
		case err := <-readDone:
			if err != nil {
				if err == unix.EAGAIN {
					bufferPool.Put(buffer)
					continue
				}
				bufferPool.Put(buffer)
				return nil, fmt.Errorf("error reading inotify events: %w", err)
			}
			eventProcessed = true
		case <-time.After(100 * time.Millisecond):
			bufferPool.Put(buffer)
			continue
		}

		if !eventProcessed {
			bufferPool.Put(buffer)
			continue
		}

		// Rate limit our file open attempts
		select {
		case <-rateLimiter.C:
			// Try to open the file
			openChan := make(chan openResult, 1)
			go func() {
				file, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_WRONLY, 0644)
				openChan <- openResult{file: file, err: err}
			}()

			select {
			case result := <-openChan:
				if result.err == nil {
					bufferPool.Put(buffer)
					return result.file, nil
				}
			case <-time.After(100 * time.Millisecond):
				// Timeout on file open, continue waiting
			}
		default:
			// Skip this attempt if we're rate limited
		}

		bufferPool.Put(buffer)
	}
}

type openResult struct {
	file *os.File
	err  error
}

func RunBackup(job *store.Job, storeInstance *store.Store) (*store.Task, error) {
	backupMutex, err := filemutex.New("/tmp/pbs-plus-mutex-lock")
	if err != nil {
		return nil, fmt.Errorf("RunBackup: failed to create mutex lock: %w", err)
	}
	defer backupMutex.Close() // Ensure mutex is always closed

	if err := backupMutex.Lock(); err != nil {
		return nil, fmt.Errorf("RunBackup: failed to acquire lock: %w", err)
	}
	defer backupMutex.Unlock()

	// Create a context with timeout for the entire operation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if storeInstance.APIToken == nil {
		return nil, fmt.Errorf("RunBackup: api token is required")
	}

	// Validate and setup target
	target, err := storeInstance.GetTarget(job.Target)
	if err != nil {
		return nil, fmt.Errorf("RunBackup: failed to get target: %w", err)
	}
	if target == nil {
		return nil, fmt.Errorf("RunBackup: Target '%s' does not exist", job.Target)
	}
	if !target.ConnectionStatus {
		return nil, fmt.Errorf("RunBackup: Target '%s' is unreachable or does not exist", job.Target)
	}

	srcPath := target.Path
	isAgent := strings.HasPrefix(target.Path, "agent://")

	var agentMount *mount.AgentMount
	if isAgent {
		if agentMount, err = mountAgent(ctx, storeInstance, target); err != nil {
			return nil, err
		}
		defer func() {
			defer cancel()
			if agentMount != nil {
				agentMount.Unmount()
				agentMount.CloseSFTP()
			}
		}()
		srcPath = agentMount.Path
	}

	srcPath = filepath.Join(srcPath, job.Subpath)

	// Prepare backup command
	cmd, err := prepareBackupCommand(ctx, job, storeInstance, srcPath, isAgent)
	if err != nil {
		return nil, err
	}

	// Setup command pipes
	stdout, stderr, err := setupCommandPipes(cmd)
	if err != nil {
		return nil, err
	}
	defer stdout.Close()
	defer stderr.Close()

	// Create task monitoring channel with buffer to prevent goroutine leak
	taskChan := make(chan store.Task, 1)

	// Start monitoring in background first
	monitorCtx, monitorCancel := context.WithCancel(ctx)
	defer monitorCancel()

	var task *store.Task
	var monitorErr error
	monitorDone := make(chan struct{})
	monitorInitializedChan := make(chan struct{})

	go func() {
		defer close(monitorDone)
		task, monitorErr = monitorTask(monitorCtx, storeInstance, job, taskChan, monitorInitializedChan)
	}()

	select {
	case <-monitorInitializedChan:
	case <-monitorDone:
		if monitorErr != nil {
			return nil, fmt.Errorf("RunBackup: task monitoring failed: %w", monitorErr)
		}
	case <-monitorCtx.Done():
		return nil, fmt.Errorf("RunBackup: timeout waiting for task")
	}

	// Now start the backup process
	if err := cmd.Start(); err != nil {
		monitorCancel() // Cancel monitoring since backup failed to start
		return nil, fmt.Errorf("RunBackup: proxmox-backup-client start error (%s): %w", cmd.String(), err)
	}

	// Wait for either monitoring to complete or timeout
	select {
	case <-monitorDone:
		if monitorErr != nil {
			_ = cmd.Process.Kill()
			return nil, fmt.Errorf("RunBackup: task monitoring failed: %w", monitorErr)
		}
	case <-monitorCtx.Done():
		_ = cmd.Process.Kill()
		return nil, fmt.Errorf("RunBackup: timeout waiting for task")
	}

	if task == nil {
		_ = cmd.Process.Kill()
		return nil, fmt.Errorf("RunBackup: no task created")
	}

	// Start collecting logs and wait for backup completion
	var logLines []string
	var logMu sync.Mutex

	go collectLogs(ctx, stdout, stderr, &logLines, &logMu)

	// Wait for backup completion
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	// Wait for completion or context cancellation
	select {
	case err := <-done:
		if err != nil {
			return task, fmt.Errorf("RunBackup: backup execution failed: %w", err)
		}
	case <-ctx.Done():
		_ = cmd.Process.Kill()
		return task, fmt.Errorf("RunBackup: backup timeout: %w", ctx.Err())
	}

	// Update job status
	if err := updateJobStatus(job, task, storeInstance, logLines); err != nil {
		return task, fmt.Errorf("RunBackup: failed to update job status: %w", err)
	}

	return task, nil
}

func mountAgent(ctx context.Context, storeInstance *store.Store, target *store.Target) (*mount.AgentMount, error) {
	agentMount, err := mount.Mount(storeInstance, target)
	if err != nil {
		return nil, fmt.Errorf("RunBackup: mount initialization error: %w", err)
	}

	// Use context with timeout for mount wait
	mountDone := make(chan error, 1)
	go func() {
		mountDone <- agentMount.Cmd.Wait()
	}()

	select {
	case err := <-mountDone:
		if err != nil {
			return nil, fmt.Errorf("RunBackup: mount wait error: %w", err)
		}
	case <-ctx.Done():
		return nil, fmt.Errorf("RunBackup: mount timeout: %w", ctx.Err())
	}

	return agentMount, nil
}

func monitorTask(ctx context.Context, storeInstance *store.Store, job *store.Job, taskChan chan store.Task, readyChan chan struct{}) (*store.Task, error) {
	if err := storeInstance.GetJobTask(ctx, taskChan, readyChan, job); err != nil {
		return nil, fmt.Errorf("failed to start task monitoring: %w", err)
	}

	// Wait for monitoring to be ready
	select {
	case <-readyChan:
		// Monitoring is ready, continue
	case <-ctx.Done():
		return nil, fmt.Errorf("context cancelled while waiting for monitoring setup: %w", ctx.Err())
	}

	// Wait for task or context cancellation
	select {
	case taskResult := <-taskChan:
		return &taskResult, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("timeout waiting for task: %w", ctx.Err())
	}
}

func prepareBackupCommand(ctx context.Context, job *store.Job, storeInstance *store.Store, srcPath string, isAgent bool) (*exec.Cmd, error) {
	if srcPath == "" {
		return nil, fmt.Errorf("RunBackup: source path is required")
	}

	backupId, err := getBackupId(isAgent, job.Target)
	if err != nil {
		return nil, fmt.Errorf("RunBackup: failed to get backup ID: %w", err)
	}

	jobStore := fmt.Sprintf("%s@localhost:%s", storeInstance.APIToken.TokenId, job.Store)
	if jobStore == "@localhost:" {
		return nil, fmt.Errorf("RunBackup: invalid job store configuration")
	}

	cmdArgs := buildCommandArgs(storeInstance, job, srcPath, jobStore, backupId, isAgent)
	if len(cmdArgs) == 0 {
		return nil, fmt.Errorf("RunBackup: failed to build command arguments")
	}

	cmd := exec.CommandContext(ctx, "/usr/bin/prlimit", cmdArgs...)
	cmd.Env = buildCommandEnv(storeInstance)

	return cmd, nil
}

func getBackupId(isAgent bool, targetName string) (string, error) {
	if !isAgent {
		hostname, err := os.Hostname()
		if err != nil {
			hostnameBytes, err := os.ReadFile("/etc/hostname")
			if err != nil {
				return "localhost", nil
			}
			return strings.TrimSpace(string(hostnameBytes)), nil
		}
		return hostname, nil
	}
	if targetName == "" {
		return "", fmt.Errorf("target name is required for agent backup")
	}
	return strings.TrimSpace(strings.Split(targetName, " - ")[0]), nil
}

func buildCommandArgs(storeInstance *store.Store, job *store.Job, srcPath string, jobStore string, backupId string, isAgent bool) []string {
	if srcPath == "" || jobStore == "" || backupId == "" {
		return nil
	}

	cmdArgs := []string{
		"--nofile=1024:1024",
		"/usr/bin/proxmox-backup-client",
		"backup",
		fmt.Sprintf("%s.pxar:%s", strings.ReplaceAll(job.Target, " ", "-"), srcPath),
		"--repository", jobStore,
		"--change-detection-mode=metadata",
		"--backup-id", backupId,
		"--crypt-mode=none",
		"--skip-e2big-xattr", "true",
		"--skip-lost-and-found", "true",
	}

	// Add exclusions
	for _, exclusion := range job.Exclusions {
		if isAgent && exclusion.JobID != job.ID {
			continue
		}
		cmdArgs = append(cmdArgs, "--exclude", exclusion.Path)
	}

	// Add namespace if specified
	if job.Namespace != "" {
		_ = CreateNamespace(job.Namespace, job, storeInstance)
		cmdArgs = append(cmdArgs, "--ns", job.Namespace)
	}

	_ = FixDatastore(job, storeInstance)

	return cmdArgs
}

func buildCommandEnv(storeInstance *store.Store) []string {
	if storeInstance == nil || storeInstance.APIToken == nil {
		return os.Environ()
	}

	env := append(os.Environ(),
		fmt.Sprintf("PBS_PASSWORD=%s", storeInstance.APIToken.Value))

	// Add fingerprint if available
	if pbsStatus, err := storeInstance.GetPBSStatus(); err == nil {
		if fingerprint, ok := pbsStatus.Info["fingerprint"]; ok {
			env = append(env, fmt.Sprintf("PBS_FINGERPRINT=%s", fingerprint))
		}
	}

	return env
}

func setupCommandPipes(cmd *exec.Cmd) (io.ReadCloser, io.ReadCloser, error) {
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, fmt.Errorf("error creating stdout pipe: %w", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		stdout.Close() // Clean up stdout if stderr fails
		return nil, nil, fmt.Errorf("error creating stderr pipe: %w", err)
	}

	return stdout, stderr, nil
}

func collectLogs(ctx context.Context, stdout, stderr io.ReadCloser, logLines *[]string, logMu *sync.Mutex) {
	reader := bufio.NewScanner(io.MultiReader(stdout, stderr))
	reader.Buffer(make([]byte, 0, 64*1024), 1024*1024) // Increased buffer size

	done := make(chan struct{})
	go func() {
		defer close(done)
		for reader.Scan() {
			select {
			case <-ctx.Done():
				return
			default:
				logMu.Lock()
				*logLines = append(*logLines, reader.Text())
				logMu.Unlock()
			}
		}
	}()

	select {
	case <-ctx.Done():
		return
	case <-done:
		return
	}
}

func updateJobStatus(job *store.Job, task *store.Task, storeInstance *store.Store, logLines []string) error {
	errChan := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	go func() {
		syslogger, err := syslog.InitializeLogger()
		if err != nil {
			errChan <- fmt.Errorf("failed to initialize logger: %w", err)
			return
		}

		// Write logs to file
		if err := writeLogsToFile(task.UPID, logLines); err != nil {
			syslogger.Errorf("Failed to write logs: %v", err)
			errChan <- err
			return
		}

		// Update task status
		taskFound, err := storeInstance.GetTaskByUPID(task.UPID)
		if err != nil {
			syslogger.Errorf("Unable to get task by UPID: %v", err)
			errChan <- err
			return
		}

		// Update job status
		latestJob, err := storeInstance.GetJob(job.ID)
		if err != nil {
			syslogger.Errorf("Unable to get job: %v", err)
			errChan <- err
			return
		}

		latestJob.LastRunState = &taskFound.Status
		latestJob.LastRunEndtime = &taskFound.EndTime

		if err := storeInstance.UpdateJob(*latestJob); err != nil {
			syslogger.Errorf("Unable to update job: %v", err)
			errChan <- err
			return
		}

		errChan <- nil
	}()

	// Update immediate job status
	job.LastRunUpid = &task.UPID
	job.LastRunState = &task.Status

	if err := storeInstance.UpdateJob(*job); err != nil {
		return fmt.Errorf("unable to update job: %w", err)
	}

	// Wait for background updates to complete with timeout
	select {
	case err := <-errChan:
		if err != nil {
			return fmt.Errorf("background job update failed: %w", err)
		}
	case <-ctx.Done():
		return fmt.Errorf("job status update timed out: %w", ctx.Err())
	}

	return nil
}

func writeLogsToFile(upid string, logLines []string) error {
	logFilePath := utils.GetTaskLogPath(upid)
	logFile, err := waitForLogFile(logFilePath, 5*time.Second)
	if err != nil {
		return fmt.Errorf("log file cannot be opened: %w", err)
	}
	defer logFile.Close()

	writer := bufio.NewWriter(logFile)
	defer writer.Flush()

	if _, err := writer.WriteString("--- proxmox-backup-client log starts here ---\n"); err != nil {
		return fmt.Errorf("failed to write log header: %w", err)
	}

	hasError := false
	var errorString string
	timestamp := time.Now().Format(time.RFC3339)

	for _, logLine := range logLines {
		if strings.Contains(logLine, "Error: upload failed:") {
			errorString = strings.Replace(logLine, "Error:", "TASK ERROR:", 1)
			hasError = true
			continue
		}

		if _, err := writer.WriteString(fmt.Sprintf("%s: %s\n", timestamp, logLine)); err != nil {
			return fmt.Errorf("failed to write log line: %w", err)
		}
	}

	// Write final status
	if hasError {
		_, err = writer.WriteString(fmt.Sprintf("%s: %s", timestamp, errorString))
	} else {
		_, err = writer.WriteString(fmt.Sprintf("%s: TASK OK", timestamp))
	}

	if err != nil {
		return fmt.Errorf("failed to write final status: %w", err)
	}

	return nil
}
