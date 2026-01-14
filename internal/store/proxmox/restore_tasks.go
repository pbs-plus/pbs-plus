//go:build linux

package proxmox

import (
	"bufio"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/store/types"
)

func GetRestoreTask(
	job types.Restore,
) (*RestoreTask, error) {
	targetName := job.DestTarget.GetHostname()
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

	rTask := &RestoreTask{
		Task:    task,
		file:    file,
		restore: job,
	}

	rTask.addActiveTask()

	return rTask, nil
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

	t.removeActiveTask()

	t.closed.Store(true)
}

func (t *RestoreTask) addActiveTask() error {
	filePath := constants.ActiveLogsPath
	target := t.UPID

	lockPath := filepath.Join(filepath.Dir(filePath), "."+filepath.Base(filePath)+".lock")
	lockFile, err := os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return fmt.Errorf("could not create/open lock file: %w", err)
	}
	defer lockFile.Close()

	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("could not acquire lock: %w", err)
	}
	defer syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)

	tempFile, err := os.CreateTemp(filepath.Dir(filePath), "active_add_*.tmp")
	if err != nil {
		return err
	}
	tempPath := tempFile.Name()

	success := false
	defer func() {
		if !success {
			tempFile.Close()
			os.Remove(tempPath)
		}
	}()

	exists := true
	f, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			exists = false
		} else {
			return err
		}
	}

	if exists {
		originalInfo, _ := os.Stat(filePath)
		if err := tempFile.Chmod(originalInfo.Mode()); err != nil {
			return err
		}
		if stat, ok := originalInfo.Sys().(*syscall.Stat_t); ok {
			_ = tempFile.Chown(int(stat.Uid), int(stat.Gid))
		}
	} else {
		_ = tempFile.Chmod(0644)
	}

	alreadyExists := false
	writer := bufio.NewWriter(tempFile)

	if exists {
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.Contains(line, target) {
				alreadyExists = true
			}
			if _, err := writer.WriteString(line + "\n"); err != nil {
				f.Close()
				return err
			}
		}
		f.Close()
	}

	if !alreadyExists {
		if _, err := writer.WriteString(target + "\n"); err != nil {
			return err
		}
	}

	if err := writer.Flush(); err != nil {
		return err
	}

	if err := tempFile.Sync(); err != nil {
		return err
	}
	tempFile.Close()

	if err := os.Rename(tempPath, filePath); err != nil {
		return err
	}

	success = true
	return nil
}

func (t *RestoreTask) removeActiveTask() error {
	filePath := constants.ActiveLogsPath
	target := t.UPID

	lockPath := filepath.Join(filepath.Dir(filePath), "."+filepath.Base(filePath)+".lock")
	lockFile, err := os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return fmt.Errorf("could not create/open lock file: %w", err)
	}
	defer lockFile.Close()

	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("could not acquire lock: %w", err)
	}
	defer syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)

	originalInfo, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	tempFile, err := os.CreateTemp(filepath.Dir(filePath), "active_update_*.tmp")
	if err != nil {
		return err
	}
	tempPath := tempFile.Name()

	success := false
	defer func() {
		if !success {
			tempFile.Close()
			os.Remove(tempPath)
		}
	}()

	if err := tempFile.Chmod(originalInfo.Mode()); err != nil {
		return err
	}

	if stat, ok := originalInfo.Sys().(*syscall.Stat_t); ok {
		if err := tempFile.Chown(int(stat.Uid), int(stat.Gid)); err != nil {
			return err
		}
	}

	scanner := bufio.NewScanner(f)
	writer := bufio.NewWriter(tempFile)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.Contains(line, target) {
			if _, err := writer.WriteString(line + "\n"); err != nil {
				return err
			}
		}
	}

	if err := writer.Flush(); err != nil {
		return err
	}

	if err := tempFile.Sync(); err != nil {
		return err
	}
	tempFile.Close()

	if err := os.Rename(tempPath, filePath); err != nil {
		return err
	}

	success = true
	return nil
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
	targetName := job.DestTarget.GetHostname()
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
	targetName := job.DestTarget.GetHostname()
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
