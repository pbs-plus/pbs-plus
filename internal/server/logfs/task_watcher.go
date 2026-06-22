//go:build linux

package logfs

import (
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
	// TaskStarted is emitted when a new UPID appears in the active file
	// or when a task log file is created.
	TaskStarted TaskEventType = iota

	// TaskLogLine is emitted when a line is written to a task log file.
	TaskLogLine

	// TaskFinished is emitted when a TASK OK, TASK WARNINGS, or TASK ERROR
	// line is written to a task log file, or when a UPID with a status
	// appears in the archive file.
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
	bus  *EventBus
	mu   sync.Mutex
	subs map[string][]chan TaskEvent // upid -> subscribers
}

// NewTaskWatcher creates a TaskWatcher that consumes raw filesystem
// events from the given EventBus and produces structured TaskEvents.
func NewTaskWatcher(bus *EventBus) *TaskWatcher {
	return &TaskWatcher{
		bus:  bus,
		subs: make(map[string][]chan TaskEvent),
	}
}

// Start begins processing events. Call Stop to clean up.
func (tw *TaskWatcher) Start(ctx context.Context) {
	ch := make(chan Event, 256)

	// Subscribe to all task-related file activity.
	tw.bus.Subscribe("tasks/", func(ev Event) {
		select {
		case ch <- ev:
		case <-ctx.Done():
		}
	})

	go tw.processLoop(ctx, ch)
}

// Stop is a no-op (cleanup happens via context cancellation in Start).
func (tw *TaskWatcher) Stop() {}

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
		switch {
		case ev.Path == "tasks/active":
			tw.handleActiveWrite(ev.Data)
		case strings.HasPrefix(ev.Path, "tasks/archive"):
			tw.handleArchiveWrite(ev.Data)
		case isTaskLog(ev.Path):
			tw.handleTaskLogWrite(ev.Path, ev.Data)
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

// handleActiveWrite parses writes to the active file.
// PBS rewrites the entire active file on task start and task finish.
// Active entries are just "<UPID>\n". Finished entries are
// "<UPID> <endtime_hex> <status>\n".
func (tw *TaskWatcher) handleActiveWrite(data []byte) {
	lines := splitLines(data)
	for _, line := range lines {
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}

		upid := parts[0]
		if _, err := proxmox.ParseUPID(upid); err != nil {
			continue
		}

		// 1 field = just UPID = newly started task
		// 3 fields = UPID + endtime + status = finished task (briefly in active before archive)
		if len(parts) == 1 {
			tw.emit(TaskEvent{
				Type: TaskStarted,
				UPID: upid,
			})
		} else if len(parts) >= 3 {
			status := strings.Join(parts[2:], " ")
			tw.emit(TaskEvent{
				Type: TaskFinished,
				UPID: upid,
				Line: status,
			})
		}
	}
}

// handleArchiveWrite parses appends to the archive file.
// PBS appends "<UPID> <endtime_hex> <status>\n" for each finished task.
func (tw *TaskWatcher) handleArchiveWrite(data []byte) {
	lines := splitLines(data)
	for _, line := range lines {
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 3 {
			continue
		}

		upid := parts[0]
		if _, err := proxmox.ParseUPID(upid); err != nil {
			continue
		}

		status := strings.Join(parts[2:], " ")
		taskEv := TaskEvent{
			Type: TaskFinished,
			UPID: upid,
			Line: status,
		}

		if after, ok := strings.CutPrefix(status, "ERROR: "); ok {
			taskEv.Error = fmt.Errorf("%s", after)
		}

		tw.emit(taskEv)
	}
}

// handleTaskLogWrite parses writes to individual task log files.
func (tw *TaskWatcher) handleTaskLogWrite(relPath string, data []byte) {
	upid := upidFromTaskLogPath(relPath)
	if upid == "" {
		return
	}

	lines := splitLines(data)
	for _, line := range lines {
		if line == "" {
			continue
		}

		taskEv := TaskEvent{
			Type: TaskLogLine,
			UPID: upid,
			Line: line,
		}

		if strings.Contains(line, "TASK ERROR:") {
			taskEv.Type = TaskFinished
			if errMsg := extractTaskError(line); errMsg != "" {
				taskEv.Error = fmt.Errorf("%s", errMsg)
			}
		} else if strings.Contains(line, "TASK OK") ||
			strings.Contains(line, "TASK WARNINGS") {
			taskEv.Type = TaskFinished
		}

		tw.emit(taskEv)
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
// "tasks/XX/UPID:...".
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
	return after
}

// parseTaskStatus converts a TaskFinished event into a TaskStatus.
func parseTaskStatus(ev TaskEvent) TaskStatus {
	ts := TaskStatus{UPID: ev.UPID}
	line := ev.Line

	if strings.Contains(line, "ERROR:") || ev.Error != nil {
		ts.Error = ev.Error.Error()
		if ts.Error == "" {
			ts.Error = extractTaskError(line)
		}
	} else if strings.Contains(line, "WARNINGS:") {
		ts.OK = true
		var count int
		_, _ = fmt.Sscanf(strings.TrimSpace(line), "WARNINGS: %d", &count)
		ts.Warnings = count
	} else if line == "OK" || strings.Contains(line, "TASK OK") {
		ts.OK = true
	}
	return ts
}

// splitLines splits byte data into trimmed non-empty lines.
func splitLines(data []byte) []string {
	raw := strings.Split(string(data), "\n")
	result := make([]string, 0, len(raw))
	for _, l := range raw {
		l = strings.TrimSpace(l)
		if l != "" {
			result = append(result, l)
		}
	}
	return result
}
