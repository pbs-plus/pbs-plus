package tasklog

import (
	"time"

	"github.com/pbs-plus/pbs-plus/internal/proxmox"
)

type ResolvedHistory struct {
	Starttime int64
	Endtime   int64
	Duration  int64
	State     string
}

func ResolveHistoryFields(upid string) (ResolvedHistory, bool) {
	if upid == "" {
		return ResolvedHistory{}, false
	}
	task, err := GetTaskByUPID(upid)
	if err != nil {
		return ResolvedHistory{}, false
	}
	r := ResolvedHistory{Starttime: task.StartTime}
	if task.EndTime > 0 {
		r.Endtime = task.EndTime
		r.Duration = task.EndTime - task.StartTime
	} else if task.StartTime > 0 {
		r.Duration = time.Now().Unix() - task.StartTime
	}
	if task.Status == "stopped" {
		r.State = task.ExitStatus
	}
	return r, true
}

func ApplyResolved(r ResolvedHistory, starttime, endtime, duration *int64, state *string) {
	if r.Starttime > 0 {
		*starttime = r.Starttime
	}
	if r.Endtime > 0 {
		*endtime = r.Endtime
	}
	if r.Duration > 0 {
		*duration = r.Duration
	}
	if r.State != "" {
		*state = r.State
	}
}

var _ = proxmox.Task{}
