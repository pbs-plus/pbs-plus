//go:build linux

package logfs

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/server/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

// TaskEventType classifies a task event.
type TaskEventType int

const (
	// TaskStarted is emitted when a new UPID line appears in the active file.
	TaskStarted TaskEventType = iota

	// TaskLogLine is emitted when a line is written to a task log file.
	TaskLogLine

	// TaskFinished is emitted when a TASK OK, TASK WARNINGS, or TASK ERROR
	// line is written to a task log file.
	TaskFinished
)

func (t TaskEventType) String() string {
	switch t {
	case TaskStarted:
		return "started"
	case TaskLogLine:
		return "log_line"
	case TaskFinished:
		return "finished"
	default:
		return "unknown"
	}
}

// TaskEvent is a structured event derived from PBS task log activity.
type TaskEvent struct {
	Type  TaskEventType
	UPID  string
	Line  string // Full line text (for TaskLogLine and TaskFinished)
	Error error  // Set for TASK ERROR
}

// TaskStatus is the parsed final status of a PBS task.
type TaskStatus struct {
	UPID     string
	OK       bool
	Warnings int
	Error    string
}

// TaskWatcher watches the FUSE event stream and emits structured
// TaskEvents for PBS task activity.
type TaskWatcher struct {
	bus    *EventBus
	mu     sync.Mutex
	subs   map[string][]chan TaskEvent // upid -> subscribers
	cancel context.CancelFunc
}

// NewTaskWatcher creates a TaskWatcher that consumes raw filesystem
// events from the given EventBus and produces structured TaskEvents.
func NewTaskWatcher(bus *EventBus) *TaskWatcher {
	tw := &TaskWatcher{
		bus:  bus,
		subs: make(map[string][]chan TaskEvent),
	}
	return tw
}

// Start begins processing events. Call Stop to clean up.
func (tw *TaskWatcher) Start(ctx context.Context) {
	ctx, tw.cancel = context.WithCancel(ctx)

	ch := make(chan Event, 256)
	tw.bus.Subscribe("tasks/", func(ev Event) {
		select {
		case ch <- ev:
		case <-ctx.Done():
		}
	})

	go tw.processLoop(ctx, ch)
}

// Stop cancels event processing.
func (tw *TaskWatcher) Stop() {
	if tw.cancel != nil {
		tw.cancel()
	}
}

// WaitForTaskStatus waits for a task to reach a final state (TASK OK,
// TASK WARNINGS, or TASK ERROR). It returns the parsed status.
// Returns an error if the context is canceled before the task finishes.
func (tw *TaskWatcher) WaitForTaskStatus(ctx context.Context, upid string) (TaskStatus, error) {
	ch := make(chan TaskEvent, 1)

	tw.mu.Lock()
	tw.subs[upid] = append(tw.subs[upid], ch)
	tw.mu.Unlock()

	defer func() {
		tw.mu.Lock()
		defer tw.mu.Unlock()
		subs := tw.subs[upid]
		for i, s := range subs {
			if s == ch {
				tw.subs[upid] = append(subs[:i], subs[i+1:]...)
				break
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return TaskStatus{}, ctx.Err()
		case ev := <-ch:
			if ev.Type == TaskFinished {
				return parseTaskStatus(ev), nil
			}
		}
	}
}

// processLoop reads raw events and dispatches TaskEvents.
func (tw *TaskWatcher) processLoop(ctx context.Context, ch <-chan Event) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-ch:
			tw.handleEvent(ev)
		}
	}
}

// handleEvent processes a single raw filesystem event.
func (tw *TaskWatcher) handleEvent(ev Event) {
	switch ev.Kind {
	case EventWrite:
		if strings.HasSuffix(ev.Path, "/active") {
			// New UPID appended to the active file
			lines := bytes.SplitSeq(ev.Data, []byte("\n"))
			for line := range lines {
				line = bytes.TrimSpace(line)
				if len(line) == 0 {
					continue
				}
				fields := bytes.Fields(line)
				if len(fields) == 0 {
					continue
				}
				upid := string(fields[0])
				if _, err := proxmox.ParseUPID(upid); err == nil {
					tw.emit(TaskEvent{
						Type: TaskStarted,
						UPID: upid,
					})
				}
			}
		} else if isTaskLog(ev.Path) {
			// Lines written to a task log file
			upid := upidFromTaskLogPath(ev.Path)
			if upid == "" {
				return
			}

			lines := bytes.SplitSeq(ev.Data, []byte("\n"))
			for line := range lines {
				line = bytes.TrimSpace(line)
				if len(line) == 0 {
					continue
				}
				lineStr := string(line)
				taskEv := TaskEvent{
					Type: TaskLogLine,
					UPID: upid,
					Line: lineStr,
				}

				if strings.Contains(lineStr, "TASK ERROR:") {
					taskEv.Type = TaskFinished
					if errMsg := extractTaskError(lineStr); errMsg != "" {
						taskEv.Error = fmt.Errorf("%s", errMsg)
					}
				} else if strings.HasPrefix(lineStr, "TASK OK") ||
					strings.HasPrefix(lineStr, "TASK WARNINGS") {
					taskEv.Type = TaskFinished
				}

				tw.emit(taskEv)
			}
		}
	case EventCreate:
		if isTaskLog(ev.Path) {
			upid := upidFromTaskLogPath(ev.Path)
			if upid != "" {
				tw.emit(TaskEvent{
					Type: TaskStarted,
					UPID: upid,
				})
			}
		}
	}
}

// emit sends a TaskEvent to all subscribers for the given UPID.
func (tw *TaskWatcher) emit(ev TaskEvent) {
	tw.mu.Lock()
	subs := make([]chan TaskEvent, len(tw.subs[ev.UPID]))
	copy(subs, tw.subs[ev.UPID])
	tw.mu.Unlock()

	for _, ch := range subs {
		select {
		case ch <- ev:
		default:
			syslog.L.Warn().
				WithMessage("task event subscriber channel full, dropping event").
				WithField("upid", ev.UPID).
				WithField("type", ev.Type.String()).
				Write()
		}
	}
}

// upidFromTaskLogPath extracts the UPID from a relative path like
// "tasks/ab/UPID:...".
func upidFromTaskLogPath(relPath string) string {
	rest := relPath[len(taskLogPrefix):]
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) == 2 {
		return parts[1]
	}
	return ""
}

// extractTaskError returns the error message from a "TASK ERROR: ..."
// line. Returns empty string if the prefix is not found.
func extractTaskError(line string) string {
	const prefix = "TASK ERROR: "
	_, after, ok := strings.Cut(line, prefix)
	if !ok {
		return ""
	}
	// Strip any leading timestamp
	rest := after
	return rest
}

// parseTaskStatus converts a TaskFinished event into a TaskStatus.
func parseTaskStatus(ev TaskEvent) TaskStatus {
	ts := TaskStatus{UPID: ev.UPID}
	line := ev.Line

	if strings.Contains(line, "TASK ERROR:") {
		ts.Error = extractTaskError(line)
	} else if strings.Contains(line, "TASK WARNINGS:") {
		ts.OK = true
		ts.Warnings = parseWarningCount(line)
	} else if strings.Contains(line, "TASK OK") {
		ts.OK = true
	}
	return ts
}

// parseWarningCount extracts the warning count from "TASK WARNINGS: N".
func parseWarningCount(line string) int {
	const prefix = "TASK WARNINGS: "
	_, after, ok := strings.Cut(line, prefix)
	if !ok {
		return 0
	}
	rest := after
	var count int
	_, _ = fmt.Sscanf(strings.TrimSpace(rest), "%d", &count)
	return count
}
