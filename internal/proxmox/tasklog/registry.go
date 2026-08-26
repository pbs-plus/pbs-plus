//go:build linux

package tasklog

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

// workerTaskList mirrors PBS's WORKER_TASK_LIST: the in-process registry
// of live workers, keyed by UPID task ID. Liveness of a UPID is decided
// by this registry for tasks of our own process, and by /proc for tasks
// belonging to other processes (e.g. proxmox-backup itself).
var workerTaskList sync.Map

var selfPStart = sync.OnceValues(func() (uint64, error) {
	return processStartTime(os.Getpid())
})

func processStartTime(pid int) (uint64, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, err
	}
	stat := string(data)
	cmdend := strings.LastIndexByte(stat, ')')
	if cmdend < 0 || cmdend+1 >= len(stat) {
		return 0, fmt.Errorf("tasklog: malformed /proc/self/stat")
	}
	fields := strings.Fields(stat[cmdend+1:])
	if len(fields) < 20 {
		return 0, fmt.Errorf("tasklog: /proc/self/stat too short")
	}
	p, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		return 0, err
	}
	return p, nil
}

func normalizeTaskID(id string) string {
	n, err := strconv.ParseUint(id, 16, 64)
	if err != nil {
		return id
	}
	return fmt.Sprintf("%016X", n)
}

func registerWorker(wt *WorkerTask) {
	workerTaskList.Store(normalizeTaskID(wt.Task.TaskId), wt)
}

func unregisterWorker(taskID string) {
	workerTaskList.Delete(normalizeTaskID(taskID))
}

func lookupWorker(taskID string) (*WorkerTask, bool) {
	v, ok := workerTaskList.Load(normalizeTaskID(taskID))
	if !ok {
		return nil, false
	}
	wt, ok := v.(*WorkerTask)
	return wt, ok
}

// processRunningPStart checks /proc/<pid>/stat to see whether the process
// exists and was started at the given pstart, matching PBS's
// check_process_running_pstart.
func processRunningPStart(pid int, pstart uint64) bool {
	start, err := processStartTime(pid)
	return err == nil && start == pstart
}
