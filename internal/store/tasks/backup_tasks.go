//go:build linux

package tasks

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/store/database"
	"github.com/pbs-plus/pbs-plus/internal/store/proxmox"
)

func GetBackupTask(
	ctx context.Context,
	readyChan chan struct{},
	job database.Backup,
	target database.Target,
) (proxmox.Task, error) {
	hostname, err := os.Hostname()
	if err != nil {
		hostnameFile, err := os.ReadFile("/etc/hostname")
		if err != nil {
			hostname = "localhost"
		} else {
			hostname = strings.TrimSpace(string(hostnameFile))
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
			return proxmox.Task{}, ctx.Err()
		case <-ticker.C:
			if task, found := scanTaskFile(constants.ActiveLogsPath, searchString, startTimeThreshold); found {
				return task, nil
			}

			if task, found := scanTaskFile(constants.ArchivedLogsPath, searchString, startTimeThreshold); found {
				return task, nil
			}
		}
	}
}

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

	// Read last 64KB (safely handles very busy servers)
	readSize := int64(65536)
	if stat.Size() < readSize {
		readSize = stat.Size()
	}

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

func GenerateBackupTaskErrorFile(job database.Backup, pbsError error, additionalData []string) (proxmox.Task, error) {
	targetName := job.Target.GetHostname()
	wid := fmt.Sprintf("%s%shost-%s", proxmox.EncodeToHexEscapes(job.Store), proxmox.EncodeToHexEscapes(":"), proxmox.EncodeToHexEscapes(targetName))
	startTime := fmt.Sprintf("%08X", uint32(time.Now().Unix()))

	wtype := "backup"
	node := "pbsplusgen-error"

	task := proxmox.Task{
		Node:       node,
		PID:        os.Getpid(),
		PStart:     proxmox.GetPStart(),
		StartTime:  time.Now().Unix(),
		WorkerType: wtype,
		WID:        wid,
		User:       proxmox.AUTH_ID,
	}

	pid := fmt.Sprintf("%08X", task.PID)
	pstart := fmt.Sprintf("%08X", task.PStart)
	taskID := fmt.Sprintf("%08X", rand.Uint32())

	upid := fmt.Sprintf("UPID:%s:%s:%s:%s:%s:%s:%s:%s:", node, pid, pstart, taskID, startTime, wtype, wid, proxmox.AUTH_ID)

	task.UPID = upid

	path, err := proxmox.GetLogPath(upid)
	if err != nil {
		return proxmox.Task{}, err
	}

	_ = os.MkdirAll(filepath.Dir(path), 0755)
	_ = os.Chown(filepath.Dir(path), 34, 34)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return proxmox.Task{}, err
	}

	err = file.Chown(34, 34)
	if err != nil {
		return proxmox.Task{}, err
	}
	defer file.Close()

	timestamp := time.Now().Format(time.RFC3339)

	for _, data := range additionalData {
		dataLine := fmt.Sprintf("%s: %s\n", timestamp, data)
		if _, err := file.WriteString(dataLine); err != nil {
			return proxmox.Task{}, fmt.Errorf("failed to write additional data line: %w", err)
		}
	}

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

	for _, line := range errorLines {
		if line != "" {
			errorDetailLine := fmt.Sprintf("%s: %s\n", timestamp, line)
			if _, err := file.WriteString(errorDetailLine); err != nil {
				return proxmox.Task{}, fmt.Errorf("failed to write error detail line: %w", err)
			}
		}
	}

	errorLine := fmt.Sprintf("%s: TASK ERROR: %s\n", timestamp, firstNonEmptyLine)
	if _, err := file.WriteString(errorLine); err != nil {
		return proxmox.Task{}, fmt.Errorf("failed to write error line: %w", err)
	}

	archive, err := os.OpenFile(filepath.Join(constants.TaskLogsBasePath, "archive"), os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return proxmox.Task{}, fmt.Errorf("failed to open file archive: %w", err)
	}
	defer archive.Close()

	archiveLine := fmt.Sprintf("%s %s %s\n", upid, startTime, firstNonEmptyLine)
	if _, err := archive.WriteString(archiveLine); err != nil {
		return proxmox.Task{}, fmt.Errorf("failed to write archive line: %w", err)
	}

	task.Status = "stopped"
	task.ExitStatus = firstNonEmptyLine
	task.EndTime = time.Now().Unix()

	return task, nil
}

func GenerateBackupTaskOKFile(job database.Backup, additionalData []string) (proxmox.Task, error) {
	targetName := job.Target.GetHostname()
	wid := fmt.Sprintf("%s%shost-%s", proxmox.EncodeToHexEscapes(job.Store), proxmox.EncodeToHexEscapes(":"), proxmox.EncodeToHexEscapes(targetName))
	startTime := fmt.Sprintf("%08X", uint32(time.Now().Unix()))

	wtype := "backup"
	node := "pbsplusgen-ok"

	task := proxmox.Task{
		Node:       node,
		PID:        os.Getpid(),
		PStart:     proxmox.GetPStart(),
		StartTime:  time.Now().Unix(),
		WorkerType: wtype,
		WID:        wid,
		User:       proxmox.AUTH_ID,
	}

	pid := fmt.Sprintf("%08X", task.PID)
	pstart := fmt.Sprintf("%08X", task.PStart)
	taskID := fmt.Sprintf("%08X", rand.Uint32())

	upid := fmt.Sprintf("UPID:%s:%s:%s:%s:%s:%s:%s:%s:", node, pid, pstart, taskID, startTime, wtype, wid, proxmox.AUTH_ID)

	task.UPID = upid

	path, err := proxmox.GetLogPath(upid)
	if err != nil {
		return proxmox.Task{}, err
	}

	_ = os.MkdirAll(filepath.Dir(path), 0755)
	_ = os.Chown(filepath.Dir(path), 34, 34)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return proxmox.Task{}, err
	}

	err = file.Chown(34, 34)
	if err != nil {
		return proxmox.Task{}, err
	}
	defer file.Close()

	timestamp := time.Now().Format(time.RFC3339)

	for _, data := range additionalData {
		dataLine := fmt.Sprintf("%s: %s\n", timestamp, data)
		if _, err := file.WriteString(dataLine); err != nil {
			return proxmox.Task{}, fmt.Errorf("failed to write additional data line: %w", err)
		}
	}

	errorLine := fmt.Sprintf("%s: TASK OK\n", timestamp)
	if _, err := file.WriteString(errorLine); err != nil {
		return proxmox.Task{}, fmt.Errorf("failed to write ok line: %w", err)
	}

	archive, err := os.OpenFile(filepath.Join(constants.TaskLogsBasePath, "archive"), os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return proxmox.Task{}, fmt.Errorf("failed to open file archive: %w", err)
	}
	defer archive.Close()

	archiveLine := fmt.Sprintf("%s %s OK\n", upid, startTime)
	if _, err := archive.WriteString(archiveLine); err != nil {
		return proxmox.Task{}, fmt.Errorf("failed to write archive line: %w", err)
	}

	task.Status = "stopped"
	task.ExitStatus = "OK"
	task.EndTime = time.Now().Unix()

	return task, nil
}
