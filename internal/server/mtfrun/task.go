package mtfrun

import (
	"fmt"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/tasks"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

// ScanTask wraps a scan job with full active/archive task pipeline.
type ScanTask struct {
	tasks.BaseTask
}

// NewScanTask creates a scan task, opens its task log, and registers it as active.
func NewScanTask(opts Options) (*ScanTask, error) {
	task := tasks.NewTask("pbsplus", "mtfscan", scanWID(opts))

	file, _, err := tasks.CreateTaskLogFile(task.UPID)
	if err != nil {
		syslog.L.Error(err).WithMessage("mtf: create scan task log").Write()
		return nil, err
	}

	st := &ScanTask{
		BaseTask: tasks.NewBaseTask(task, file),
	}
	if err := tasks.AddActive(task.UPID); err != nil {
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
		_ = tasks.RemoveActive(t.UPID)
	})
}

// CloseErr closes the task with error status.
func (t *ScanTask) CloseErr(taskErr error) {
	t.CloseWithStatus("TASK ERROR: "+taskErr.Error(), nil, func() {
		_ = tasks.RemoveActive(t.UPID)
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
