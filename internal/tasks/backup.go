//go:build linux

package tasks

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/store/database"
	"github.com/pbs-plus/pbs-plus/internal/store/proxmox"
)

// GetBackupTask waits for and retrieves a backup task by scanning log files.
func GetBackupTask(
	ctx context.Context,
	readyChan chan struct{},
	job database.Backup,
	target database.Target,
) (proxmox.Task, error) {
	hostname, err := os.Hostname()
	if err != nil {
		if hostnameBytes, err := os.ReadFile("/etc/hostname"); err == nil {
			hostname = strings.TrimSpace(string(hostnameBytes))
		} else {
			hostname = "localhost"
		}
	}

	startTimeThreshold := time.Now().Unix()
	backupId := hostname
	if target.IsAgent() {
		backupId = target.GetHostname()
	}
	backupId = proxmox.NormalizeHostname(backupId)

	searchString := fmt.Sprintf(":backup:%s%shost-%s", proxmox.EncodeToHexEscapes(job.Store), proxmox.EncodeToHexEscapes(":"), proxmox.EncodeToHexEscapes(backupId))

	close(readyChan)

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return proxmox.Task{}, fmt.Errorf("timed out")
		case <-ticker.C:
			if task, found := scanTaskFile(conf.ActiveLogsPath, searchString, startTimeThreshold); found {
				return task, nil
			}
			if task, found := scanTaskFile(conf.ArchivedLogsPath, searchString, startTimeThreshold); found {
				return task, nil
			}
		}
	}
}

// scanTaskFile searches for a task in a log file.
func scanTaskFile(path string, searchString string, threshold int64) (proxmox.Task, bool) {
	file, err := os.Open(path)
	if err != nil {
		return proxmox.Task{}, false
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil || stat.Size() == 0 {
		return proxmox.Task{}, false
	}

	readSize := min(stat.Size(), int64(65536))
	buffer := make([]byte, readSize)
	_, err = file.ReadAt(buffer, stat.Size()-readSize)
	if err != nil && err != io.EOF {
		return proxmox.Task{}, false
	}

	lines := strings.Split(string(buffer), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if line == "" || !strings.Contains(line, searchString) {
			continue
		}
		fields := strings.Fields(line)
		if task, err := proxmox.ParseUPID(fields[0]); err == nil {
			if task.StartTime >= (threshold-1) && task.WorkerType == "backup" {
				return task, true
			}
		}
	}
	return proxmox.Task{}, false
}

// GenerateBackupTaskErrorFile creates an error task file with detailed error information.
func GenerateBackupTaskErrorFile(job database.Backup, pbsError error, additionalData []string) (proxmox.Task, error) {
	targetName := job.Target.GetHostname()
	wid := fmt.Sprintf("%s%shost-%s", proxmox.EncodeToHexEscapes(job.Store), proxmox.EncodeToHexEscapes(":"), proxmox.EncodeToHexEscapes(targetName))
	startTime := now()
	startTimeHex := fmt.Sprintf("%08X", uint32(startTime.Unix()))

	task := proxmox.Task{
		Node:       "pbsplusgen-error",
		PID:        os.Getpid(),
		PStart:     proxmox.GetPStart(),
		StartTime:  startTime.Unix(),
		WorkerType: "backup",
		WID:        wid,
		User:       proxmox.AUTH_ID,
	}

	pid := fmt.Sprintf("%08X", task.PID)
	pstart := fmt.Sprintf("%08X", task.PStart)
	taskID := fmt.Sprintf("%08X", rand.Uint32())
	task.UPID = fmt.Sprintf("UPID:%s:%s:%s:%s:%s:%s:%s:%s:", task.Node, pid, pstart, taskID, startTimeHex, task.WorkerType, wid, proxmox.AUTH_ID)

	file, _, err := createTaskLogFile(task.UPID)
	if err != nil {
		return proxmox.Task{}, err
	}
	defer file.Close()

	timestamp := now().Format(time.RFC3339)

	// Write additional data
	for _, data := range additionalData {
		fmt.Fprintf(file, "%s: %s\n", timestamp, data)
	}

	// Process error message and extract first non-empty line
	fullError := pbsError.Error()
	errorLines := strings.Split(fullError, "\n")
	firstNonEmptyLine := ""
	for _, line := range errorLines {
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			firstNonEmptyLine = trimmed
			break
		}
	}
	if firstNonEmptyLine == "" {
		firstNonEmptyLine = fullError
	}

	// Write all error lines
	for _, line := range errorLines {
		if line != "" {
			fmt.Fprintf(file, "%s: %s\n", timestamp, line)
		}
	}

	// Write final error summary
	fmt.Fprintf(file, "%s: TASK ERROR: %s\n", timestamp, firstNonEmptyLine)

	// Archive with the error
	writeArchive(task.UPID, task.StartTime, firstNonEmptyLine)

	task.Status = "stopped"
	task.ExitStatus = firstNonEmptyLine
	task.EndTime = now().Unix()
	return task, nil
}

// GenerateBackupTaskOKFile creates an OK task file.
func GenerateBackupTaskOKFile(job database.Backup, additionalData []string) (proxmox.Task, error) {
	targetName := job.Target.GetHostname()
	wid := fmt.Sprintf("%s%shost-%s", proxmox.EncodeToHexEscapes(job.Store), proxmox.EncodeToHexEscapes(":"), proxmox.EncodeToHexEscapes(targetName))
	startTime := now()
	startTimeHex := fmt.Sprintf("%08X", uint32(startTime.Unix()))

	task := proxmox.Task{
		Node:       "pbsplusgen-ok",
		PID:        os.Getpid(),
		PStart:     proxmox.GetPStart(),
		StartTime:  startTime.Unix(),
		WorkerType: "backup",
		WID:        wid,
		User:       proxmox.AUTH_ID,
	}

	pid := fmt.Sprintf("%08X", task.PID)
	pstart := fmt.Sprintf("%08X", task.PStart)
	taskID := fmt.Sprintf("%08X", rand.Uint32())
	task.UPID = fmt.Sprintf("UPID:%s:%s:%s:%s:%s:%s:%s:%s:", task.Node, pid, pstart, taskID, startTimeHex, task.WorkerType, wid, proxmox.AUTH_ID)

	file, _, err := createTaskLogFile(task.UPID)
	if err != nil {
		return proxmox.Task{}, err
	}
	defer file.Close()

	base := baseTask{Task: task, file: file}
	for _, data := range additionalData {
		base.writeLogLine("%s", data)
	}
	base.writeLogLine("TASK OK")

	writeArchive(task.UPID, task.StartTime, "OK")

	task.Status = "stopped"
	task.ExitStatus = "OK"
	task.EndTime = now().Unix()
	return task, nil
}
