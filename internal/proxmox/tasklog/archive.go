//go:build linux

package tasklog

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"log/slog"
)

type TaskListInfo struct {
	UPID  string
	Task  proxmox.Task
	State *TaskState
}

// WriteArchive appends a finished task line to the archive under the
// exclusive task-list lock.
func WriteArchive(upid string, state TaskState) error {
	lock, err := lockTaskList(true)
	if err != nil {
		return err
	}
	defer lock.Close()

	return appendArchiveLines([]TaskListInfo{{
		UPID:  upid,
		Task:  proxmox.Task{},
		State: &state,
	}})
}

// ListTasks is PBS's TaskListInfoIterator: reconcile first (dead workers
// get folded into the archive), then read the active list under a shared
// lock and, unless activeOnly, all archive files including rotated ones.
func ListTasks(activeOnly bool) ([]TaskListInfo, error) {
	if err := Reconcile(""); err != nil {
		return nil, err
	}

	lock, err := lockTaskList(false)
	if err != nil {
		return nil, err
	}
	defer lock.Close()

	active, err := readTaskFile(activeTasks)
	if err != nil {
		return nil, err
	}

	if activeOnly {
		return active, nil
	}

	results := active
	for _, path := range archiveFiles() {
		list, err := readTaskFileAny(path)
		if err != nil {
			slog.Error("tasklog: read archive", "error", err, "path", path)
			continue
		}
		results = append(results, list...)
	}
	return results, nil
}

// GetTaskByUPID resolves a UPID's current state: running when the worker
// is still alive (registry or /proc), otherwise the finished state read
// from the log tail.
func GetTaskByUPID(upid string) (proxmox.Task, error) {
	parsed, err := proxmox.ParseUPID(upid)
	if err != nil {
		return proxmox.Task{}, fmt.Errorf("tasklog: parse upid: %w", err)
	}

	parsed.Status = "stopped"
	if workerIsActiveLocal(parsed) {
		parsed.Status = "running"
		return parsed, nil
	}

	state, err := ReadStatusFromLog(upid)
	if err != nil {
		parsed.ExitStatus = "unknown"
	} else {
		parsed.ExitStatus = state.String()
		parsed.EndTime = state.EndTime
	}

	return parsed, nil
}

// ReadStatusFromLog parses time and exit status from the last log line,
// scanning backward through an 8 KiB tail exactly like PBS's
// upid_read_status. Only correct for finished tasks.
func ReadStatusFromLog(upid string) (TaskState, error) {
	logPath, err := UPIDLogPath(upid)
	if err != nil {
		return TaskState{Status: StatusUnknown}, err
	}

	f, err := os.Open(logPath)
	if err != nil {
		return TaskState{Status: StatusUnknown}, err
	}
	defer func() {
		if cerr := f.Close(); cerr != nil {
			slog.Error(cerr.Error())
		}
	}()

	info, statErr := f.Stat()
	if statErr != nil {
		return TaskState{Status: StatusUnknown}, statErr
	}
	const tailSize = 8192
	offset := max(info.Size()-tailSize, 0)
	if _, seekErr := f.Seek(offset, io.SeekStart); seekErr != nil {
		return TaskState{Status: StatusUnknown}, seekErr
	}

	data, readErr := io.ReadAll(f)
	if readErr != nil {
		return TaskState{Status: StatusUnknown}, readErr
	}

	lines := strings.Split(string(data), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimRight(lines[i], "\r")
		if line == "" {
			continue
		}

		ts, after, found := strings.Cut(line, ": ")
		if !found {
			continue
		}
		endtime, terr := time.Parse(time.RFC3339, ts)
		if terr != nil {
			continue
		}

		rest, isTask := strings.CutPrefix(after, "TASK ")
		if !isTask {
			return TaskState{Status: StatusUnknown, EndTime: endtime.Unix()}, nil
		}

		state, stErr := FromEndtimeAndMessage(endtime.Unix(), rest)
		if stErr != nil {
			continue
		}
		return state, nil
	}

	parsed, parseErr := proxmox.ParseUPID(upid)
	if parseErr != nil {
		return TaskState{Status: StatusUnknown}, nil
	}
	return TaskState{Status: StatusUnknown, EndTime: parsed.StartTime}, nil
}

// FindRunningTask searches the active list and archive for a task of
// workerType whose UPID contains searchString and whose starttime is at
// least the threshold, under a shared task-list lock.
func FindRunningTask(workerType string, searchString string, startTimeThreshold int64) (proxmox.Task, bool) {
	lock, err := lockTaskList(false)
	if err != nil {
		return proxmox.Task{}, false
	}
	defer lock.Close()

	if task, found := findTaskInFile(activeTasks, workerType, searchString, startTimeThreshold); found {
		return task, true
	}
	task, found := findTaskInFile(archivePath, workerType, searchString, startTimeThreshold)
	return task, found
}

func findTaskInFile(path string, workerType string, searchString string, threshold int64) (proxmox.Task, bool) {
	f, err := os.Open(path)
	if err != nil {
		return proxmox.Task{}, false
	}
	defer func() {
		if cerr := f.Close(); cerr != nil {
			slog.Error(cerr.Error())
		}
	}()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || !strings.Contains(line, searchString) {
			continue
		}
		fields := strings.Fields(line)
		if task, parseErr := proxmox.ParseUPID(fields[0]); parseErr == nil {
			if task.StartTime >= (threshold-1) && task.WorkerType == workerType {
				return task, true
			}
		}
	}
	return proxmox.Task{}, false
}

// archiveFiles returns the archive plus its rotated variants, newest
// first: archive, archive.1, archive.1.gz, archive.2, archive.2.gz, ...
func archiveFiles() []string {
	var files []string
	if _, err := os.Stat(archivePath); err == nil {
		files = append(files, archivePath)
	}
	for i := 1; ; i++ {
		plain := fmt.Sprintf("%s.%d", archivePath, i)
		gz := plain + ".gz"
		found := false
		if _, err := os.Stat(plain); err == nil {
			files = append(files, plain)
			found = true
		}
		if _, err := os.Stat(gz); err == nil {
			files = append(files, gz)
			found = true
		}
		if !found {
			break
		}
	}
	return files
}

// openTaskListFile opens a task-list file, transparently decompressing
// rotated .gz variants.
func openTaskListFile(path string) (io.ReadCloser, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if !strings.HasSuffix(path, ".gz") {
		return f, nil
	}
	gz, err := gzip.NewReader(f)
	if err != nil {
		if cerr := f.Close(); cerr != nil {
			slog.Error(cerr.Error())
		}
		return nil, err
	}
	return gz, nil
}

func readTaskFileAny(path string) ([]TaskListInfo, error) {
	r, err := openTaskListFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer func() {
		if cerr := r.Close(); cerr != nil {
			slog.Error(cerr.Error())
		}
	}()
	return readTaskLines(r)
}

func readTaskLines(r io.Reader) ([]TaskListInfo, error) {
	var list []TaskListInfo
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		upidStr, state, err := ParseStatusLine(line)
		if err != nil {
			slog.Warn("tasklog: skipping unparsable task list line", "error", err)
			continue
		}
		task, err := proxmox.ParseUPID(upidStr)
		if err != nil {
			slog.Warn("tasklog: skipping invalid UPID in task list", "error", err)
			continue
		}
		list = append(list, TaskListInfo{UPID: upidStr, Task: task, State: state})
	}
	return list, scanner.Err()
}
