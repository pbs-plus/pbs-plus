package pxarmount

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

// ProgressPhase identifies a commit phase for progress reporting.
type ProgressPhase int

const (
	PhasePrepare ProgressPhase = iota
	PhaseWalk
	PhaseUpload
	PhaseVerify
	PhaseFinalize
	PhaseDone
)

var phaseLabels = map[ProgressPhase]string{
	PhasePrepare:  "Preparing",
	PhaseWalk:     "Scanning files",
	PhaseUpload:   "Uploading",
	PhaseVerify:   "Verifying",
	PhaseFinalize: "Finalizing",
	PhaseDone:     "Done",
}

// ProgressState carries the current progress snapshot.
type ProgressState struct {
	Phase      ProgressPhase
	Files      int64
	Bytes      int64
	TotalFiles int64
	TotalBytes int64
	Msg        string
}

// ProgressReporter sends framed progress updates to a writer.
type ProgressReporter struct {
	mu       sync.Mutex
	w        io.Writer
	state    ProgressState
	lastSend time.Time
}

// NewProgressReporter creates a reporter that writes to w.
func NewProgressReporter(w io.Writer) *ProgressReporter {
	return &ProgressReporter{
		w:     w,
		state: ProgressState{},
	}
}

// SetPhase updates the current phase and sends an update.
func (r *ProgressReporter) SetPhase(phase ProgressPhase) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.Phase = phase
	r.send()
}

// SetMsg sets a custom message and sends an update.
func (r *ProgressReporter) SetMsg(msg string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.Msg = msg
	r.send()
}

// AddFile increments the file counter and optionally sends an update.
func (r *ProgressReporter) AddFile(bytes int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.Files++
	r.state.Bytes += bytes
	if time.Since(r.lastSend) > 100*time.Millisecond {
		r.send()
	}
}

// SetTotals sets the expected totals.
func (r *ProgressReporter) SetTotals(files, bytes int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.TotalFiles = files
	r.state.TotalBytes = bytes
	r.send()
}

// State returns a copy of the current progress state.
func (r *ProgressReporter) State() ProgressState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state
}

// send writes a framed progress update to the underlying writer.
func (r *ProgressReporter) send() {
	label, ok := phaseLabels[r.state.Phase]
	if !ok {
		label = "Working"
	}
	msg := r.state.Msg
	if msg == "" {
		msg = label
	}
	var extra string
	if r.state.Files > 0 {
		extra = fmt.Sprintf(" (%d files", r.state.Files)
		if r.state.Bytes > 0 {
			extra += fmt.Sprintf(", %s", formatBytes(r.state.Bytes))
		}
		extra += ")"
	}
	_, _ = fmt.Fprintf(r.w, "PROGRESS %s%s\n", msg, extra)
	r.lastSend = time.Now()
}

// Done sends the final OK message.
func (r *ProgressReporter) Done(msg string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.Phase = PhaseDone
	_, _ = fmt.Fprintf(r.w, "OK %s\n", msg)
}

// Error sends an error message.
func (r *ProgressReporter) Error(msg string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, _ = fmt.Fprintf(r.w, "ERR %s\n", msg)
}

// ProgressDisplay renders a live progress bar on a terminal.
type ProgressDisplay struct {
	lastLine string
	w        io.Writer
}

// NewProgressDisplay creates a display that writes to w.
func NewProgressDisplay(w io.Writer) *ProgressDisplay {
	return &ProgressDisplay{w: w}
}

// Update renders a progress frame.
func (d *ProgressDisplay) Update(line string) {
	msg := strings.TrimPrefix(line, "PROGRESS ")
	line = fmt.Sprintf("\r  %s", msg)
	if len(line) < len(d.lastLine) {
		line += strings.Repeat(" ", len(d.lastLine)-len(line))
	}
	_, _ = fmt.Fprint(d.w, line)
	d.lastLine = line
}

// Done prints the final status line.
func (d *ProgressDisplay) Done(line string) {
	if len(d.lastLine) > 0 {
		_, _ = fmt.Fprintf(d.w, "\r%s\r", strings.Repeat(" ", len(d.lastLine)))
	}
	msg := strings.TrimPrefix(line, "OK ")
	_, _ = fmt.Fprintf(d.w, "  ✓ %s\n", msg)
	d.lastLine = ""
}

// Error prints the error.
func (d *ProgressDisplay) Error(line string) {
	if len(d.lastLine) > 0 {
		_, _ = fmt.Fprintf(d.w, "\r%s\r", strings.Repeat(" ", len(d.lastLine)))
	}
	msg := strings.TrimPrefix(line, "ERR ")
	_, _ = fmt.Fprintf(d.w, "  ✗ error: %s\n", msg)
	d.lastLine = ""
}

// formatBytes formats a byte count as a human-readable string.
func formatBytes(b int64) string {
	const (
		kiB = 1024
		miB = 1024 * kiB
		giB = 1024 * miB
	)
	switch {
	case b >= giB:
		return fmt.Sprintf("%.1f GiB", float64(b)/float64(giB))
	case b >= miB:
		return fmt.Sprintf("%.1f MiB", float64(b)/float64(miB))
	case b >= kiB:
		return fmt.Sprintf("%.1f KiB", float64(b)/float64(kiB))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
