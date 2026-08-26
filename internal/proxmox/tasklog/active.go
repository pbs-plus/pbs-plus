//go:build linux

package tasklog

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"log/slog"
)

// Path overrides, initialized from conf; tests point them at temp dirs.
var (
	taskDir     = conf.TaskLogsBasePath
	activeTasks = conf.ActiveLogsPath
	archivePath = conf.ArchivedLogsPath
	lockPath    = conf.TaskLogsBasePath + "/.active.lock"
)

const lockTimeout = 15 * time.Second

// taskListLock is PBS's TaskListLockGuard: an flock on tasks/.active.lock
// held for the duration of a task-list read or update.
type taskListLock struct{ f *os.File }

func lockTaskList(exclusive bool) (*taskListLock, error) {
	// PBS's init_worker_tasks creates the task dir before anyone touches
	// the lock; do the same so a first run can take the lock at all.
	if err := os.MkdirAll(taskDir, 0755); err != nil {
		return nil, fmt.Errorf("tasklog: create task dir: %w", err)
	}
	if err := proxmox.ChownBackupUser(taskDir); err != nil {
		return nil, err
	}

	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0660)
	if err != nil {
		return nil, fmt.Errorf("tasklog: open task list lock: %w", err)
	}
	if err := proxmox.ChownBackupUser(lockPath); err != nil {
		if cerr := f.Close(); cerr != nil {
			slog.Error(cerr.Error())
		}
		return nil, err
	}

	how := syscall.LOCK_SH
	if exclusive {
		how = syscall.LOCK_EX
	}
	deadline := time.Now().Add(lockTimeout)
	for {
		err = syscall.Flock(int(f.Fd()), how|syscall.LOCK_NB)
		if err == nil {
			return &taskListLock{f: f}, nil
		}
		if time.Now().After(deadline) {
			if cerr := f.Close(); cerr != nil {
				slog.Error(cerr.Error())
			}
			return nil, fmt.Errorf("tasklog: acquire task list lock: %w", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (l *taskListLock) Close() {
	if err := syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN); err != nil {
		slog.Error("tasklog: release task list lock", "error", err)
	}
	if err := l.f.Close(); err != nil {
		slog.Error("tasklog: close task list lock", "error", err)
	}
}

func readTaskFile(path string) ([]TaskListInfo, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("tasklog: open task list %s: %w", path, err)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil {
			slog.Error(cerr.Error())
		}
	}()

	var list []TaskListInfo
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		upidStr, state, err := ParseStatusLine(line)
		if err != nil {
			slog.Warn("tasklog: skipping unparsable task list line", "error", err, "line", line)
			continue
		}
		task, err := proxmox.ParseUPID(upidStr)
		if err != nil {
			slog.Warn("tasklog: skipping invalid UPID in task list", "error", err, "line", line)
			continue
		}
		list = append(list, TaskListInfo{UPID: upidStr, Task: task, State: state})
	}
	return list, scanner.Err()
}

func renderTaskList(list []TaskListInfo) string {
	var sb strings.Builder
	for _, info := range list {
		sb.WriteString(RenderStatusLine(info.UPID, info.State))
	}
	return sb.String()
}

// replaceFile atomically rewrites path via temp file + rename, matching
// PBS's replace_file.
func replaceFile(path, content string, perm os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp*")
	if err != nil {
		return fmt.Errorf("tasklog: create temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer func() {
		if cerr := os.Remove(tmpName); cerr != nil && !os.IsNotExist(cerr) {
			slog.Error(cerr.Error())
		}
	}()

	if _, err := tmp.WriteString(content); err != nil {
		if cerr := tmp.Close(); cerr != nil {
			slog.Error(cerr.Error())
		}
		return fmt.Errorf("tasklog: write temp file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		if cerr := tmp.Close(); cerr != nil {
			slog.Error(cerr.Error())
		}
		return fmt.Errorf("tasklog: sync temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("tasklog: close temp file: %w", err)
	}
	if err := os.Chmod(tmpName, perm); err != nil {
		return fmt.Errorf("tasklog: chmod temp file: %w", err)
	}
	if err := proxmox.ChownBackupUser(tmpName); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

func appendArchiveLines(finished []TaskListInfo) error {
	if len(finished) == 0 {
		return nil
	}

	archive, err := os.OpenFile(archivePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0660)
	if err != nil {
		return fmt.Errorf("tasklog: open archive: %w", err)
	}
	if err := proxmox.ChownBackupUser(archivePath); err != nil {
		if cerr := archive.Close(); cerr != nil {
			slog.Error(cerr.Error())
		}
		return err
	}
	defer func() {
		if cerr := archive.Close(); cerr != nil {
			slog.Error(cerr.Error())
		}
	}()

	for _, info := range finished {
		if _, err := archive.WriteString(RenderStatusLine(info.UPID, info.State)); err != nil {
			return fmt.Errorf("tasklog: write archive: %w", err)
		}
	}
	return nil
}

// Reconcile is PBS's update_active_workers: under the exclusive task-list
// lock, move finished and dead workers from the active list into the
// archive, optionally adding newUPID as newly running. It is the single
// write path for tasks/active and the archive, which is what makes
// concurrent access safe against proxmox-backup doing the same dance on
// its side: both sides serialize on tasks/.active.lock.
func Reconcile(newUPID string) error {
	lock, err := lockTaskList(true)
	if err != nil {
		return err
	}
	defer lock.Close()

	activeList, err := readTaskFile(activeTasks)
	if err != nil {
		return err
	}

	var finished []TaskListInfo
	kept := activeList[:0]
	for _, info := range activeList {
		if info.State != nil {
			finished = append(finished, info)
			continue
		}

		active, aerr := workerIsActive(info.Task)
		if aerr != nil || active {
			kept = append(kept, info)
			continue
		}

		now := time.Now().Unix()
		state, serr := ReadStatusFromLog(info.UPID)
		if serr != nil {
			state = TaskState{Status: StatusUnknown, EndTime: now}
		}
		info.State = &state
		finished = append(finished, info)
	}

	if newUPID != "" {
		task, err := proxmox.ParseUPID(newUPID)
		if err != nil {
			return fmt.Errorf("tasklog: parse upid: %w", err)
		}
		kept = append(kept, TaskListInfo{UPID: newUPID, Task: task, State: nil})
	}

	if err := replaceFile(activeTasks, renderTaskList(kept), 0660); err != nil {
		return err
	}

	sort.SliceStable(finished, func(i, j int) bool {
		return finished[i].State.EndTime < finished[j].State.EndTime
	})
	return appendArchiveLines(finished)
}
