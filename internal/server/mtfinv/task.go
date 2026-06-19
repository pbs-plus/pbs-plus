package mtfinv

import (
	"bufio"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/server/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/server/tasks"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

// ScanTask wraps a scan job with full active/archive task pipeline.
type ScanTask struct {
	tasks.BaseTask
}

// NewScanTask creates a scan task, opens its task log, and registers it as active.
func NewScanTask(opts Options) (*ScanTask, error) {
	task := proxmox.Task{
		Node:       "pbsplus",
		PID:        os.Getpid(),
		PStart:     proxmox.GetPStart(),
		StartTime:  tasks.Now().Unix(),
		WorkerType: "mtfscan",
		WID:        scanWID(opts),
		User:       proxmox.AUTH_ID,
	}
	pidHex := fmt.Sprintf("%08X", task.PID)
	pstartHex := fmt.Sprintf("%08X", task.PStart)
	startHex := fmt.Sprintf("%08X", uint32(task.StartTime))
	taskID := fmt.Sprintf("%08X", rand.Uint32())
	task.UPID = fmt.Sprintf("UPID:%s:%s:%s:%s:%s:%s:%s:%s:", task.Node, pidHex, pstartHex, taskID, startHex, task.WorkerType, task.WID, proxmox.AUTH_ID)

	file, _, err := tasks.CreateTaskLogFile(task.UPID)
	if err != nil {
		syslog.L.Error(err).WithMessage("mtf: create scan task log").Write()
		return nil, err
	}

	st := &ScanTask{
		BaseTask: tasks.NewBaseTask(task, file),
	}
	if err := st.addActiveTask(); err != nil {
		syslog.L.Error(err).WithMessage("mtf: add active scan task").Write()
	}
	return st, nil
}

// CloseOK closes the task with "OK" status.
func (t *ScanTask) CloseOK(res *Result) {
	msg := "OK"
	if res != nil {
		msg = fmt.Sprintf("OK: %d cartridges, %d families (%s)", res.Cartridges, res.Families, res.Duration.Truncate(time.Second))
	}
	t.CloseWithStatus(msg, nil, func() {
		_ = t.removeActiveTask()
	})
}

// CloseErr closes the task with error status.
func (t *ScanTask) CloseErr(taskErr error) {
	t.CloseWithStatus("TASK ERROR: "+taskErr.Error(), nil, func() {
		_ = t.removeActiveTask()
	})
}

func scanWID(opts Options) string {
	s := opts.ChangerDevice + opts.TapeDevice + opts.BKFPath
	return fmt.Sprintf("mtfscan-%x", hashStr(s))
}

func hashStr(s string) uint64 {
	var h uint64 = 5381
	for _, c := range []byte(s) {
		h = h*33 + uint64(c)
	}
	return h
}

func (t *ScanTask) addActiveTask() error {
	return modifyActiveFile(t.UPID, true)
}

func (t *ScanTask) removeActiveTask() error {
	return modifyActiveFile(t.UPID, false)
}

func modifyActiveFile(target string, add bool) error {
	f, err := os.OpenFile(conf.ActiveLogsPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		if !add && os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("open active tasks: %w", err)
	}
	defer func() { _ = f.Close() }()

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("lock active tasks: %w", err)
	}
	defer func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }()

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
	if add && !found {
		lines = append(lines, target)
	}
	if !add && !found {
		return nil
	}

	if err := f.Truncate(0); err != nil {
		return err
	}
	if _, err := f.Seek(0, 0); err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	for _, line := range lines {
		if _, err := w.WriteString(line + "\n"); err != nil {
			return err
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}
	return f.Sync()
}
