package coredb

import (
	"cmp"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
)

type taskEntry struct {
	upid      string
	starttime int64
}

// jobTasks lists the tasks linked in a job's log directory, newest first, keeping at most limit when limit > 0.
func jobTasks(baseDir, id string, limit int) []Tasks {
	logsPath := filepath.Join(baseDir, id)
	if err := os.MkdirAll(logsPath, 0755); err != nil {
		log.Error(fmt.Errorf("jobTasks: failed to get log dir: %w", err), "", "id", id)
		return nil
	}

	logs, err := os.ReadDir(logsPath)
	if err != nil {
		log.Error(fmt.Errorf("jobTasks: failed to read dir: %w", err), "", "id", id)
		return nil
	}

	entries := make([]taskEntry, 0, len(logs))
	for _, entry := range logs {
		if tasklog.IsQueuedUPID(entry.Name()) {
			if err := os.Remove(filepath.Join(logsPath, entry.Name())); err != nil && !os.IsNotExist(err) {
				log.Error(fmt.Errorf("jobTasks: failed removing queued task link: %w", err), "", "id", id)
			}
			continue
		}
		parsed, err := proxmox.ParseUPID(entry.Name())
		if err != nil {
			continue
		}
		entries = append(entries, taskEntry{upid: entry.Name(), starttime: parsed.StartTime})
	}

	slices.SortFunc(entries, func(a, b taskEntry) int { return cmp.Compare(b.starttime, a.starttime) })
	if limit > 0 && len(entries) > limit {
		entries = entries[:limit]
	}

	tasks := make([]Tasks, 0, len(entries))
	for _, e := range entries {
		task, err := tasklog.GetTaskByUPID(e.upid)
		if err != nil {
			continue
		}
		tasks = append(tasks, Tasks{UPID: task.UPID, Endtime: task.EndTime, Status: task.ExitStatus})
	}
	return tasks
}
