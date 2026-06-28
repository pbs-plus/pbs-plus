package mtf

import (
	"fmt"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
)

type ScanTask struct {
	*tasklog.WorkerTask
}

func NewScanTask(opts Options) (*ScanTask, error) {
	wt, err := tasklog.NewWorkerTask("pbsplus", "mtfscan", scanWID(opts))
	if err != nil {
		log.Error(err, "mtf: create scan task log")
		return nil, err
	}

	return &ScanTask{WorkerTask: wt}, nil
}

func (t *ScanTask) CloseOK(res *Result) {
	if res != nil {
		t.Log("scan completed: %d cartridges, %d families (%s)", res.Cartridges, res.Families, res.Duration.Truncate(time.Second))
	}
	t.WorkerTask.CloseOK()
}

func (t *ScanTask) CloseErr(taskErr error) {
	t.WorkerTask.CloseErr(taskErr)
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
