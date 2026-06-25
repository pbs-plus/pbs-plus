package mtf

import (
	"fmt"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

type ScanTask struct {
	*tasklog.WorkerTask
}

func NewScanTask(opts Options) (*ScanTask, error) {
	wt, err := tasklog.NewWorkerTask("pbsplus", "mtfscan", scanWID(opts))
	if err != nil {
		syslog.L.Error(err).WithMessage("mtf: create scan task log").Write()
		return nil, err
	}

	return &ScanTask{WorkerTask: wt}, nil
}

func (t *ScanTask) CloseOK(res *Result) {
	msg := "OK"
	if res != nil {
		msg = fmt.Sprintf("OK: %d cartridges, %d families (%s)", res.Cartridges, res.Families, res.Duration.Truncate(time.Second))
	}
	t.CloseWithStatus(tasklog.TaskState{Status: tasklog.StatusOK, EndTime: time.Now().Unix(), Message: msg}, func() {
		if err := tasklog.RemoveActive(t.UPID()); err != nil {
			syslog.L.Error(err).Write()
		}
	})
}

func (t *ScanTask) CloseErr(taskErr error) {
	t.CloseWithStatus(tasklog.TaskState{Status: tasklog.StatusError, EndTime: time.Now().Unix(), Message: taskErr.Error()}, func() {
		if err := tasklog.RemoveActive(t.UPID()); err != nil {
			syslog.L.Error(err).Write()
		}
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
