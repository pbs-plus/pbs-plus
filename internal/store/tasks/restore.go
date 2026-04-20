//go:build linux

package tasks

import (
	"bufio"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"syscall"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/store/database"
	"github.com/pbs-plus/pbs-plus/internal/store/proxmox"
)

// RestoreTask manages restore job logging and lifecycle.
type RestoreTask struct {
	baseTask
	restore database.Restore
}

// GetRestoreTask creates a new restore task with log file setup.
func GetRestoreTask(job database.Restore) (*RestoreTask, error) {
	targetName := job.DestTarget.GetHostname()
	wid := fmt.Sprintf("%s%shost-%s", proxmox.EncodeToHexEscapes(job.Store), proxmox.EncodeToHexEscapes(":"), proxmox.EncodeToHexEscapes(targetName))
	startTimeHex := fmt.Sprintf("%08X", uint32(now().Unix()))
	pidHex := fmt.Sprintf("%08X", os.Getpid())
	pstartHex := fmt.Sprintf("%08X", proxmox.GetPStart())
	taskID := fmt.Sprintf("%08X", rand.Uint32())

	task := proxmox.Task{
		Node:       "pbsplus",
		PID:        os.Getpid(),
		PStart:     proxmox.GetPStart(),
		StartTime:  now().Unix(),
		WorkerType: "reader",
		WID:        wid,
		User:       proxmox.AUTH_ID,
	}
	task.UPID = fmt.Sprintf("UPID:%s:%s:%s:%s:%s:%s:%s:%s:", task.Node, pidHex, pstartHex, taskID, startTimeHex, task.WorkerType, wid, proxmox.AUTH_ID)

	file, _, err := createTaskLogFile(task.UPID)
	if err != nil {
		return nil, err
	}

	rTask := &RestoreTask{
		baseTask: baseTask{Task: task, file: file},
		restore:  job,
	}
	rTask.addActiveTask()
	return rTask, nil
}

// WriteString writes a timestamped log line to the task file.
func (t *RestoreTask) WriteString(data string) {
	t.Lock()
	defer t.Unlock()

	if t.closed.Load() {
		return
	}
	t.writeLogLine("%s", data)
	t.file.Sync()
}

// close cleans up resources and marks the task closed.
func (t *RestoreTask) close() {
	t.baseTask.close()
	t.removeActiveTask()
}

// addActiveTask registers this task in the active tasks file.
func (t *RestoreTask) addActiveTask() error {
	return modifyActiveTaskFile(t.UPID, true)
}

// removeActiveTask unregisters this task from the active tasks file.
func (t *RestoreTask) removeActiveTask() error {
	return modifyActiveTaskFile(t.UPID, false)
}

// modifyActiveTaskFile adds or removes a task from the active tasks file.
func modifyActiveTaskFile(target string, add bool) error {
	f, err := os.OpenFile(conf.ActiveLogsPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		if !add && os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("could not open active tasks file: %w", err)
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
			if !add { // skip when removing
				continue
			}
		}
		lines = append(lines, line)
	}

	if add {
		if found {
			return nil // already exists
		}
		lines = append(lines, target)
	} else {
		if !found {
			return nil // nothing to remove
		}
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

// CloseOK closes the task with "OK" status.
func (t *RestoreTask) CloseOK() {
	t.closeWithStatus("OK", nil, func() {
		_ = t.removeActiveTask()
	})
}

// CloseErr closes the task with "ERROR: <msg>" status.
func (t *RestoreTask) CloseErr(taskErr error) {
	errMsg := taskErr.Error()
	t.closeWithStatus("ERROR: "+errMsg, nil, func() {
		_ = t.removeActiveTask()
	})
	writeArchive(t.UPID, t.StartTime, errMsg) // archive has raw error without "ERROR:" prefix
}

// CloseWarn closes the task with "WARNINGS: <n>" status.
func (t *RestoreTask) CloseWarn(warning int) {
	t.closeWithStatus(fmt.Sprintf("WARNINGS: %d", warning), nil, func() {
		_ = t.removeActiveTask()
	})
}

// GenerateRestoreTaskOKFile creates a standalone OK task file for restore operations.
func GenerateRestoreTaskOKFile(job database.Restore, additionalData []string) (proxmox.Task, error) {
	targetName := job.DestTarget.GetHostname()
	wid := fmt.Sprintf("%s%shost-%s", proxmox.EncodeToHexEscapes(job.Store), proxmox.EncodeToHexEscapes(":"), proxmox.EncodeToHexEscapes(targetName))
	startTime := now().Unix()
	startTimeHex := fmt.Sprintf("%08X", uint32(startTime))
	pidHex := fmt.Sprintf("%08X", os.Getpid())
	pstartHex := fmt.Sprintf("%08X", proxmox.GetPStart())
	taskID := fmt.Sprintf("%08X", rand.Uint32())

	task := proxmox.Task{
		Node:       "pbsplusgen-ok",
		PID:        os.Getpid(),
		PStart:     proxmox.GetPStart(),
		StartTime:  startTime,
		WorkerType: "reader",
		WID:        wid,
		User:       proxmox.AUTH_ID,
	}
	task.UPID = fmt.Sprintf("UPID:%s:%s:%s:%s:%s:%s:%s:%s:", task.Node, pidHex, pstartHex, taskID, startTimeHex, task.WorkerType, wid, proxmox.AUTH_ID)

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
