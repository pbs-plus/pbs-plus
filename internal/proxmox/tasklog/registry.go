//go:build linux

package tasklog

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/proxmox"
)

// workerTaskList mirrors PBS's WORKER_TASK_LIST: the in-process registry
// of live workers, keyed by UPID task ID. Liveness of a UPID is decided
// by this registry for tasks of our own process, and by /proc for tasks
// belonging to other processes (e.g. proxmox-backup itself).
var workerTaskList sync.Map

var selfPStart = sync.OnceValues(func() (uint64, error) {
	data, err := os.ReadFile("/proc/self/stat")
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
})

// workerIsActiveLocal reports whether the worker behind a UPID is still
// running: registry membership for our own process, /proc pid+pstart for
// any other process. Same contract as PBS's worker_is_active_local.
func workerIsActiveLocal(task proxmox.Task) bool {
	if p, err := selfPStart(); err == nil && task.PID == os.Getpid() && task.PStart == p {
		_, ok := workerTaskList.Load(task.TaskId)
		return ok
	}
	return processRunningPStart(task.PID, task.PStart)
}

// processRunningPStart checks /proc/<pid>/stat to see whether the process
// exists and was started at the given pstart, matching PBS's
// check_process_running_pstart.
func processRunningPStart(pid int, pstart uint64) bool {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return false
	}
	stat := string(data)
	cmdend := strings.LastIndexByte(stat, ')')
	if cmdend < 0 || cmdend+1 >= len(stat) {
		return false
	}
	fields := strings.Fields(stat[cmdend+1:])
	if len(fields) < 20 {
		return false
	}
	start, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		return false
	}
	return start == pstart
}
