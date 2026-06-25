//go:build linux

package tasklog

import (
	"fmt"
	"strconv"
	"strings"
)

type TaskStatus int

const (
	StatusUnknown TaskStatus = iota
	StatusOK
	StatusWarning
	StatusError
)

type TaskState struct {
	Status    TaskStatus
	EndTime   int64
	WarnCount uint64
	Message   string
}

func (s TaskState) String() string {
	switch s.Status {
	case StatusUnknown:
		return "unknown"
	case StatusOK:
		return "OK"
	case StatusWarning:
		return fmt.Sprintf("WARNINGS: %d", s.WarnCount)
	case StatusError:
		return s.Message
	default:
		return "unknown"
	}
}

func (s TaskState) ResultText() string {
	if s.Status == StatusError {
		return fmt.Sprintf("TASK ERROR: %s", s.Message)
	}
	return fmt.Sprintf("TASK %s", s)
}

func FromEndtimeAndMessage(endtime int64, msg string) (TaskState, error) {
	switch {
	case msg == "unknown":
		return TaskState{Status: StatusUnknown, EndTime: endtime}, nil
	case msg == "OK":
		return TaskState{Status: StatusOK, EndTime: endtime}, nil
	case strings.HasPrefix(msg, "WARNINGS: "):
		count, err := strconv.ParseUint(strings.TrimPrefix(msg, "WARNINGS: "), 10, 64)
		if err != nil {
			return TaskState{}, fmt.Errorf("failed to parse warning count: %w", err)
		}
		return TaskState{Status: StatusWarning, EndTime: endtime, WarnCount: count}, nil
	default:
		message := msg
		if after, ok := strings.CutPrefix(message, "ERROR: "); ok {
			message = after
		}
		return TaskState{Status: StatusError, EndTime: endtime, Message: message}, nil
	}
}

func ParseStatusLine(line string) (upidStr string, state *TaskState, err error) {
	line = strings.TrimSpace(line)
	if line == "" {
		return "", nil, fmt.Errorf("empty line")
	}

	parts := strings.SplitN(line, " ", 3)
	switch len(parts) {
	case 1:
		return parts[0], nil, nil
	case 3:
		endtime, err := strconv.ParseInt(parts[1], 16, 64)
		if err != nil {
			return "", nil, fmt.Errorf("failed to parse endtime: %w", err)
		}
		st, err := FromEndtimeAndMessage(endtime, parts[2])
		if err != nil {
			return "", nil, fmt.Errorf("failed to parse status: %w", err)
		}
		return parts[0], &st, nil
	default:
		return "", nil, fmt.Errorf("wrong number of components in status line")
	}
}

func RenderStatusLine(upidStr string, state *TaskState) string {
	if state != nil {
		return fmt.Sprintf("%s %08X %s\n", upidStr, state.EndTime, state)
	}
	return upidStr + "\n"
}
