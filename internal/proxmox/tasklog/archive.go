//go:build linux

package tasklog

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

func WriteArchive(upid string, state TaskState) error {
	archive, err := os.OpenFile(filepath.Join(conf.TaskLogsBasePath, "archive"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0660)
	if err != nil {
		return fmt.Errorf("tasklog: open archive: %w", err)
	}
	defer func() {
		if cerr := archive.Close(); cerr != nil {
			syslog.L.Error(cerr).Write()
		}
	}()

	line := RenderStatusLine(upid, &state)
	if _, err := archive.WriteString(line); err != nil {
		return fmt.Errorf("tasklog: write archive: %w", err)
	}
	return nil
}

type TaskListInfo struct {
	UPID  string
	Task  proxmox.Task
	State *TaskState
}

func ListTasks(activeOnly bool) ([]TaskListInfo, error) {
	var results []TaskListInfo

	activeFile, err := os.Open(conf.ActiveLogsPath)
	if err == nil {
		defer func() {
			if cerr := activeFile.Close(); cerr != nil {
				syslog.L.Error(cerr).Write()
			}
		}()
		scanner := bufio.NewScanner(activeFile)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			upidStr, state, err := ParseStatusLine(line)
			if err != nil {
				continue
			}
			task, err := proxmox.ParseUPID(upidStr)
			if err != nil {
				continue
			}
			if state == nil && !activeOnly {
				st, serr := ReadStatusFromLog(upidStr)
				if serr == nil {
					state = &st
				}
			}
			results = append(results, TaskListInfo{
				UPID:  upidStr,
				Task:  task,
				State: state,
			})
		}
	}

	if activeOnly {
		return results, nil
	}

	archiveFile, err := os.Open(filepath.Join(conf.TaskLogsBasePath, "archive"))
	if err == nil {
		defer func() {
			if cerr := archiveFile.Close(); cerr != nil {
				syslog.L.Error(cerr).Write()
			}
		}()
		scanner := bufio.NewScanner(archiveFile)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			upidStr, state, err := ParseStatusLine(line)
			if err != nil {
				continue
			}
			task, err := proxmox.ParseUPID(upidStr)
			if err != nil {
				continue
			}
			results = append(results, TaskListInfo{
				UPID:  upidStr,
				Task:  task,
				State: state,
			})
		}
	}

	return results, nil
}

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
			syslog.L.Error(cerr).Write()
		}
	}()

	const tailSize = 8192
	info, statErr := f.Stat()
	if statErr != nil {
		return TaskState{Status: StatusUnknown}, statErr
	}
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
		line := lines[i]
		line = strings.TrimRight(line, "\r")
		if line == "" {
			continue
		}

		ts, after, found := strings.Cut(line, ": ")
		if !found {
			continue
		}
		if _, terr := time.Parse(time.RFC3339, ts); terr != nil {
			continue
		}

		endtime, perr := time.Parse(time.RFC3339, ts)
		if perr != nil {
			continue
		}

		rest, isTask := strings.CutPrefix(after, "TASK ")
		if !isTask {
			return TaskState{Status: StatusUnknown, EndTime: endtime.Unix()}, nil
		}

		state, stErr := FromEndtimeAndMessage(endtime.Unix(), rest)
		if stErr != nil {
			return TaskState{Status: StatusUnknown, EndTime: endtime.Unix()}, nil
		}
		return state, nil
	}

	parsed, parseErr := proxmox.ParseUPID(upid)
	if parseErr != nil {
		return TaskState{Status: StatusUnknown}, nil
	}
	return TaskState{Status: StatusUnknown, EndTime: parsed.StartTime}, nil
}

func GetTaskByUPID(upid string) (proxmox.Task, error) {
	parsed, err := proxmox.ParseUPID(upid)
	if err != nil {
		return proxmox.Task{}, fmt.Errorf("tasklog: parse upid: %w", err)
	}

	parsed.Status = "stopped"
	if IsActive(upid) {
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
