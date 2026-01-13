//go:build linux

package proxmox

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/store/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

func GetBackupTask(
	ctx context.Context,
	readyChan chan struct{},
	job types.Backup,
	target types.Target,
) (Task, error) {
	hostname, err := os.Hostname()
	if err != nil {
		hostnameFile, err := os.ReadFile("/etc/hostname")
		if err != nil {
			hostname = "localhost"
		} else {
			hostname = strings.TrimSpace(string(hostnameFile))
		}
	}

	backupId := hostname
	if target.Path.IsAgent() {
		backupId = target.Name.GetHostname()
	}
	backupId = NormalizeHostname(backupId)

	searchString := fmt.Sprintf(":backup:%s%shost-%s", encodeToHexEscapes(job.Store), encodeToHexEscapes(":"), encodeToHexEscapes(backupId))

	initialUPIDs := make(map[string]struct{})
	tasks, err := ListTasksJSON(ctx)
	if err != nil {
		return Task{}, err
	}

	for _, t := range tasks {
		initialUPIDs[t.UPID] = struct{}{}
	}

	syslog.L.Info().WithMessage("ready to start backup").Write()
	close(readyChan)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return Task{}, ctx.Err()

		case <-ticker.C:
			tasks, err := ListTasksJSON(ctx)
			if err != nil {
				syslog.L.Error(err).Write()
				continue
			}

			for _, t := range tasks {
				if t.WorkerType == "backup" && strings.Contains(t.UPID, searchString) {
					if _, seen := initialUPIDs[t.UPID]; seen {
						continue // skip tasks in the initial set
					}
					return GetTaskByUPID(t.UPID)
				}
			}
		}
	}
}

func GenerateBackupTaskErrorFile(job types.Backup, pbsError error, additionalData []string) (Task, error) {
	targetName := job.Target.GetHostname()
	wid := fmt.Sprintf("%s%shost-%s", encodeToHexEscapes(job.Store), encodeToHexEscapes(":"), encodeToHexEscapes(targetName))
	startTime := fmt.Sprintf("%08X", uint32(time.Now().Unix()))

	wtype := "backup"
	node := "pbsplusgen-error"

	task := Task{
		Node:       node,
		PID:        os.Getpid(),
		PStart:     getPStart(),
		StartTime:  time.Now().Unix(),
		WorkerType: wtype,
		WID:        wid,
		User:       AUTH_ID,
	}

	pid := fmt.Sprintf("%08X", task.PID)
	pstart := fmt.Sprintf("%08X", task.PStart)
	taskID := fmt.Sprintf("%08X", rand.Uint32())

	upid := fmt.Sprintf("UPID:%s:%s:%s:%s:%s:%s:%s:%s:", node, pid, pstart, taskID, startTime, wtype, wid, AUTH_ID)

	task.UPID = upid

	path, err := GetLogPath(upid)
	if err != nil {
		return Task{}, err
	}

	_ = os.MkdirAll(filepath.Dir(path), 0755)
	_ = os.Chown(filepath.Dir(path), 34, 34)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return Task{}, err
	}

	err = file.Chown(34, 34)
	if err != nil {
		return Task{}, err
	}
	defer file.Close()

	timestamp := time.Now().Format(time.RFC3339)

	for _, data := range additionalData {
		dataLine := fmt.Sprintf("%s: %s\n", timestamp, data)
		if _, err := file.WriteString(dataLine); err != nil {
			return Task{}, fmt.Errorf("failed to write additional data line: %w", err)
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
				return Task{}, fmt.Errorf("failed to write error detail line: %w", err)
			}
		}
	}

	errorLine := fmt.Sprintf("%s: TASK ERROR: %s\n", timestamp, firstNonEmptyLine)
	if _, err := file.WriteString(errorLine); err != nil {
		return Task{}, fmt.Errorf("failed to write error line: %w", err)
	}

	archive, err := os.OpenFile(filepath.Join(constants.TaskLogsBasePath, "archive"), os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return Task{}, fmt.Errorf("failed to open file archive: %w", err)
	}
	defer archive.Close()

	archiveLine := fmt.Sprintf("%s %s %s\n", upid, startTime, firstNonEmptyLine)
	if _, err := archive.WriteString(archiveLine); err != nil {
		return Task{}, fmt.Errorf("failed to write archive line: %w", err)
	}

	task.Status = "stopped"
	task.ExitStatus = firstNonEmptyLine
	task.EndTime = time.Now().Unix()

	return task, nil
}

func GenerateBackupTaskOKFile(job types.Backup, additionalData []string) (Task, error) {
	targetName := job.Target.GetHostname()
	wid := fmt.Sprintf("%s%shost-%s", encodeToHexEscapes(job.Store), encodeToHexEscapes(":"), encodeToHexEscapes(targetName))
	startTime := fmt.Sprintf("%08X", uint32(time.Now().Unix()))

	wtype := "backup"
	node := "pbsplusgen-ok"

	task := Task{
		Node:       node,
		PID:        os.Getpid(),
		PStart:     getPStart(),
		StartTime:  time.Now().Unix(),
		WorkerType: wtype,
		WID:        wid,
		User:       AUTH_ID,
	}

	pid := fmt.Sprintf("%08X", task.PID)
	pstart := fmt.Sprintf("%08X", task.PStart)
	taskID := fmt.Sprintf("%08X", rand.Uint32())

	upid := fmt.Sprintf("UPID:%s:%s:%s:%s:%s:%s:%s:%s:", node, pid, pstart, taskID, startTime, wtype, wid, AUTH_ID)

	task.UPID = upid

	path, err := GetLogPath(upid)
	if err != nil {
		return Task{}, err
	}

	_ = os.MkdirAll(filepath.Dir(path), 0755)
	_ = os.Chown(filepath.Dir(path), 34, 34)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return Task{}, err
	}

	err = file.Chown(34, 34)
	if err != nil {
		return Task{}, err
	}
	defer file.Close()

	timestamp := time.Now().Format(time.RFC3339)

	for _, data := range additionalData {
		dataLine := fmt.Sprintf("%s: %s\n", timestamp, data)
		if _, err := file.WriteString(dataLine); err != nil {
			return Task{}, fmt.Errorf("failed to write additional data line: %w", err)
		}
	}

	errorLine := fmt.Sprintf("%s: TASK OK\n", timestamp)
	if _, err := file.WriteString(errorLine); err != nil {
		return Task{}, fmt.Errorf("failed to write ok line: %w", err)
	}

	archive, err := os.OpenFile(filepath.Join(constants.TaskLogsBasePath, "archive"), os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return Task{}, fmt.Errorf("failed to open file archive: %w", err)
	}
	defer archive.Close()

	archiveLine := fmt.Sprintf("%s %s OK\n", upid, startTime)
	if _, err := archive.WriteString(archiveLine); err != nil {
		return Task{}, fmt.Errorf("failed to write archive line: %w", err)
	}

	task.Status = "stopped"
	task.ExitStatus = "OK"
	task.EndTime = time.Now().Unix()

	return task, nil
}
