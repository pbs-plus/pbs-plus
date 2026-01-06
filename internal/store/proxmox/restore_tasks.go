//go:build linux

package proxmox

import (
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/store/types"
)

type RestoreTask struct {
	Task
	sync.Mutex
	closed  atomic.Bool
	file    *os.File
	restore types.Restore
}

func GetRestoreTask(
	job types.Restore,
) (*RestoreTask, error) {
	targetName := strings.TrimSpace(strings.Split(job.DestTarget, " - ")[0])
	wid := fmt.Sprintf("%s%shost-%s", encodeToHexEscapes(job.Store), encodeToHexEscapes(":"), encodeToHexEscapes(targetName))
	startTime := fmt.Sprintf("%08X", uint32(time.Now().Unix()))

	wtype := "reader"
	node := "pbsplus"

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
		return nil, err
	}

	_ = os.MkdirAll(filepath.Dir(path), 0755)
	_ = os.Chown(filepath.Dir(path), 34, 34)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		_ = os.Remove(filepath.Dir(path))
		return nil, err
	}

	return &RestoreTask{
		Task:    task,
		file:    file,
		restore: job,
	}, nil
}

func (t *RestoreTask) WriteString(data string) {
	t.Mutex.Lock()
	defer t.Mutex.Unlock()

	if t.closed.Load() {
		return
	}

	timestamp := time.Now().Format(time.RFC3339)

	dataLine := fmt.Sprintf("%s: %s\n", timestamp, data)
	if _, err := t.file.WriteString(dataLine); err != nil {
		return
	}

	t.file.Sync()
}

func (t *RestoreTask) close() {
	if t.file != nil {
		t.file.Close()
	}

	t.closed.Store(true)
}

func (t *RestoreTask) CloseOK() {
	t.Mutex.Lock()
	defer func() {
		t.close()
		t.Mutex.Unlock()
	}()

	timestamp := time.Now().Format(time.RFC3339)

	errorLine := fmt.Sprintf("%s: TASK OK\n", timestamp)
	if _, err := t.file.WriteString(errorLine); err != nil {
		return
	}

	archive, err := os.OpenFile(filepath.Join(constants.TaskLogsBasePath, "archive"), os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer archive.Close()

	startTime := fmt.Sprintf("%08X", uint32(t.StartTime))
	archiveLine := fmt.Sprintf("%s %s OK\n", t.UPID, startTime)
	if _, err := archive.WriteString(archiveLine); err != nil {
		return
	}
}

func (t *RestoreTask) CloseErr(taskErr error) {
	t.Mutex.Lock()
	defer func() {
		t.close()
		t.Mutex.Unlock()
	}()

	timestamp := time.Now().Format(time.RFC3339)

	errorLine := fmt.Sprintf("%s: TASK ERROR: %s\n", timestamp, taskErr.Error())
	if _, err := t.file.WriteString(errorLine); err != nil {
		return
	}

	archive, err := os.OpenFile(filepath.Join(constants.TaskLogsBasePath, "archive"), os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer archive.Close()

	startTime := fmt.Sprintf("%08X", uint32(t.StartTime))
	archiveLine := fmt.Sprintf("%s %s %s\n", t.UPID, startTime, taskErr.Error())
	if _, err := archive.WriteString(archiveLine); err != nil {
		return
	}
}

func (t *RestoreTask) CloseWarn(warning int) {
	t.Mutex.Lock()
	defer func() {
		t.close()
		t.Mutex.Unlock()
	}()

	timestamp := time.Now().Format(time.RFC3339)

	errorLine := fmt.Sprintf("%s: TASK WARNINGS: %d\n", timestamp, warning)
	if _, err := t.file.WriteString(errorLine); err != nil {
		return
	}

	archive, err := os.OpenFile(filepath.Join(constants.TaskLogsBasePath, "archive"), os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer archive.Close()

	startTime := fmt.Sprintf("%08X", uint32(t.StartTime))
	archiveLine := fmt.Sprintf("%s %s OK\n", t.UPID, startTime)
	if _, err := archive.WriteString(archiveLine); err != nil {
		return
	}
}

func GenerateRestoreTaskErrorFile(job types.Restore, pbsError error, additionalData []string) (Task, error) {
	targetName := strings.TrimSpace(strings.Split(job.DestTarget, " - ")[0])
	wid := fmt.Sprintf("%s%shost-%s", encodeToHexEscapes(job.Store), encodeToHexEscapes(":"), encodeToHexEscapes(targetName))
	startTime := fmt.Sprintf("%08X", uint32(time.Now().Unix()))

	wtype := "reader"
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

func GenerateRestoreTaskOKFile(job types.Restore, additionalData []string) (Task, error) {
	targetName := strings.TrimSpace(strings.Split(job.DestTarget, " - ")[0])
	wid := fmt.Sprintf("%s%shost-%s", encodeToHexEscapes(job.Store), encodeToHexEscapes(":"), encodeToHexEscapes(targetName))
	startTime := fmt.Sprintf("%08X", uint32(time.Now().Unix()))

	wtype := "reader"
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
