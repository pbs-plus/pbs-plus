//go:build linux

package proxmox

import (
	"bufio"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/store/types"
)

func findProxyProcess() (int, uint64, error) {
	files, err := os.ReadDir("/proc")
	if err != nil {
		return 0, 0, err
	}

	for _, f := range files {
		if !f.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(f.Name())
		if err != nil {
			continue
		}

		comm, _ := os.ReadFile(filepath.Join("/proc", f.Name(), "comm"))
		if strings.TrimSpace(string(comm)) == "proxmox-backup-" ||
			strings.TrimSpace(string(comm)) == "proxmox-backup-proxy" {

			statData, err := os.ReadFile(filepath.Join("/proc", f.Name(), "stat"))
			if err != nil {
				continue
			}
			fields := strings.Fields(string(statData))
			if len(fields) < 22 {
				continue
			}
			pstart, _ := strconv.ParseUint(fields[21], 10, 64)
			return pid, pstart, nil
		}
	}
	return 0, 0, fmt.Errorf("proxmox-backup-proxy process not found")
}

func GetRestoreTask(
	job types.Restore,
) (*RestoreTask, error) {
	targetName := job.DestTarget.GetHostname()
	wid := fmt.Sprintf("%s%shost-%s", encodeToHexEscapes(job.Store), encodeToHexEscapes(":"), encodeToHexEscapes(targetName))
	startTime := fmt.Sprintf("%08X", uint32(time.Now().Unix()))

	proxyPID, proxyPStart, err := findProxyProcess()
	if err != nil {
		// Fallback to our own if proxy isn't found, though it likely won't persist
		proxyPID = os.Getpid()
		proxyPStart = uint64(getPStart())
	}

	wtype := "reader"
	node := "pbsplus"

	task := Task{
		Node:       node,
		PID:        proxyPID,
		PStart:     proxyPStart,
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

	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("could not open active tasks file: %w", err)
	}
	defer f.Close()

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("could not acquire lock: %w", err)
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)

	var lines []string
	alreadyExists := false
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, target) {
			alreadyExists = true
		}
		lines = append(lines, line)
	}

	if alreadyExists {
		return nil // Task already registered
	}

	lines = append(lines, target)

	if err := f.Truncate(0); err != nil {
		return err
	}
	if _, err := f.Seek(0, 0); err != nil {
		return err
	}

	writer := bufio.NewWriter(f)
	for _, line := range lines {
		if _, err := writer.WriteString(line + "\n"); err != nil {
			return err
		}
	}

	if err := writer.Flush(); err != nil {
		return err
	}
	return f.Sync()
}

func (t *RestoreTask) removeActiveTask() error {
	filePath := constants.ActiveLogsPath
	target := t.UPID

	f, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("could not acquire lock: %w", err)
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)

	var lines []string
	found := false
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, target) {
			found = true
			continue // Skip the line to be removed
		}
		lines = append(lines, line)
	}

	if !found {
		return nil // Nothing changed
	}

	if err := f.Truncate(0); err != nil {
		return err
	}
	if _, err := f.Seek(0, 0); err != nil {
		return err
	}

	writer := bufio.NewWriter(f)
	for _, line := range lines {
		if _, err := writer.WriteString(line + "\n"); err != nil {
			return err
		}
	}

	if err := writer.Flush(); err != nil {
		return err
	}
	return f.Sync()
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
