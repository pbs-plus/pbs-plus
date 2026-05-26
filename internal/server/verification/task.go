//go:build linux

package verification

import (
	"bufio"
	"fmt"
	"math/rand/v2"
	"os"
	"syscall"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/server/tasks"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

// VerificationTask manages verification job logging and lifecycle.
type VerificationTask struct {
	tasks.BaseTask
	job database.VerificationJob
}

// NewVerificationTask creates a new verification task with log file setup.
func NewVerificationTask(job database.VerificationJob) (*VerificationTask, error) {
	wid := proxmox.EncodeToHexEscapes(job.ID)
	startTimeHex := fmt.Sprintf("%08X", uint32(tasks.Now().Unix()))
	pidHex := fmt.Sprintf("%08X", os.Getpid())
	pstartHex := fmt.Sprintf("%08X", proxmox.GetPStart())
	taskID := fmt.Sprintf("%08X", rand.Uint32())

	task := proxmox.Task{
		Node:       "pbsplus",
		PID:        os.Getpid(),
		PStart:     proxmox.GetPStart(),
		StartTime:  tasks.Now().Unix(),
		WorkerType: "verification",
		WID:        wid,
		User:       proxmox.AUTH_ID,
	}
	task.UPID = fmt.Sprintf("UPID:%s:%s:%s:%s:%s:%s:%s:%s:", task.Node, pidHex, pstartHex, taskID, startTimeHex, task.WorkerType, wid, proxmox.AUTH_ID)

	file, _, err := tasks.CreateTaskLogFile(task.UPID)
	if err != nil {
		return nil, err
	}

	vTask := &VerificationTask{
		BaseTask: tasks.NewBaseTask(task, file),
		job:      job,
	}
	if err := vTask.addActiveTask(); err != nil {
		syslog.L.Error(err).WithField("upid", task.UPID).WithMessage("failed to register active verification task").Write()
	}
	return vTask, nil
}

// WriteString delegates to BaseTask.WriteString.
func (t *VerificationTask) WriteString(data string) {
	t.BaseTask.WriteString(data)
}

// CloseOK closes the task with "OK" status.
func (t *VerificationTask) CloseOK() {
	t.CloseWithStatus("OK", nil, func() {
		_ = t.removeActiveTask()
	})
}

// CloseErr closes the task with "TASK ERROR: <msg>" status.
func (t *VerificationTask) CloseErr(taskErr error) {
	status := "ERROR: " + taskErr.Error()
	t.CloseWithStatus(status, nil, func() {
		_ = t.removeActiveTask()
	})
}

// CloseWarn closes the task with warnings count.
// Matches the PBS pattern: final line is "TASK WARNINGS: N".
func (t *VerificationTask) CloseWarn(warnings int) {
	status := fmt.Sprintf("WARNINGS: %d", warnings)
	t.CloseWithStatus(status, nil, func() {
		_ = t.removeActiveTask()
	})
}

// addActiveTask registers this task in the active tasks file.
func (t *VerificationTask) addActiveTask() error {
	return modifyActiveTaskFile(t.UPID, true)
}

// removeActiveTask unregisters this task from the active tasks file.
func (t *VerificationTask) removeActiveTask() error {
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
		if line == target {
			found = true
			if !add {
				continue
			}
		}
		lines = append(lines, line)
	}

	if add {
		if !found {
			lines = append(lines, target)
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
