//go:build linux

package restore

import (
	"bufio"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"syscall"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/server/tasks"
)

// RestoreTask manages restore job logging and lifecycle.
type RestoreTask struct {
	tasks.BaseTask
	restore database.Restore
}

// GetRestoreTask creates a new restore task with log file setup.
func GetRestoreTask(job database.Restore) (*RestoreTask, error) {
	targetName := job.DestTarget.GetHostname()
	wid := fmt.Sprintf("%s%shost-%s", proxmox.EncodeToHexEscapes(job.Store), proxmox.EncodeToHexEscapes(":"), proxmox.EncodeToHexEscapes(targetName))
	startTimeHex := fmt.Sprintf("%08X", uint32(tasks.Now().Unix()))
	pidHex := fmt.Sprintf("%08X", os.Getpid())
	pstartHex := fmt.Sprintf("%08X", proxmox.GetPStart())
	taskID := fmt.Sprintf("%08X", rand.Uint32())

	task := proxmox.Task{
		Node:       "pbsplus",
		PID:        os.Getpid(),
		PStart:     proxmox.GetPStart(),
		StartTime:  tasks.Now().Unix(),
		WorkerType: "reader",
		WID:        wid,
		User:       proxmox.AUTH_ID,
	}
	task.UPID = fmt.Sprintf("UPID:%s:%s:%s:%s:%s:%s:%s:%s:", task.Node, pidHex, pstartHex, taskID, startTimeHex, task.WorkerType, wid, proxmox.AUTH_ID)

	file, _, err := tasks.CreateTaskLogFile(task.UPID)
	if err != nil {
		return nil, err
	}

	rTask := &RestoreTask{
		BaseTask: tasks.NewBaseTask(task, file),
		restore:  job,
	}
	rTask.addActiveTask()
	return rTask, nil
}

// WriteString delegates to BaseTask.WriteString.
func (t *RestoreTask) WriteString(data string) {
	t.BaseTask.WriteString(data)
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
			if !add {
				continue
			}
		}
		lines = append(lines, line)
	}

	if add {
		if found {
			return nil
		}
		lines = append(lines, target)
	} else {
		if !found {
			return nil
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
	t.CloseWithStatus("OK", nil, func() {
		_ = t.removeActiveTask()
	})
}

// CloseErr closes the task with "ERROR: <msg>" status.
func (t *RestoreTask) CloseErr(taskErr error) {
	errMsg := taskErr.Error()
	t.CloseWithStatus(errMsg, nil, func() {
		_ = t.removeActiveTask()
	})
}

// CloseWarn closes the task with "WARNINGS: <n>" status.
func (t *RestoreTask) CloseWarn(warning int) {
	t.CloseWithStatus("OK", nil, func() {
		_ = t.removeActiveTask()
	})
}

// GenerateRestoreTaskOKFile creates a standalone OK task file for restore operations.
func GenerateRestoreTaskOKFile(job database.Restore, additionalData []string) (proxmox.Task, error) {
	targetName := job.DestTarget.GetHostname()
	wid := fmt.Sprintf("%s%shost-%s", proxmox.EncodeToHexEscapes(job.Store), proxmox.EncodeToHexEscapes(":"), proxmox.EncodeToHexEscapes(targetName))
	startTime := tasks.Now().Unix()
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

	file, _, err := tasks.CreateTaskLogFile(task.UPID)
	if err != nil {
		return proxmox.Task{}, err
	}
	defer file.Close()

	base := tasks.NewBaseTask(task, file)
	for _, data := range additionalData {
		base.WriteLogLine("%s", data)
	}
	base.WriteLogLine("TASK OK")

	tasks.WriteArchive(task.UPID, task.StartTime, "OK")
	task.Status = "stopped"
	task.ExitStatus = "OK"
	task.EndTime = tasks.Now().Unix()
	return task, nil
}
