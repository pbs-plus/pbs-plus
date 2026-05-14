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
	Files      int64 // files processed so far
	Bytes      int64 // bytes processed so far
	TotalFiles int64 // total files (if known)
	TotalBytes int64 // total bytes (if known)
	Msg        string
	Started    time.Time
}

// Percent returns completion percentage (0-100), or -1 if unknown.
func (s ProgressState) Percent() float64 {
	if s.TotalFiles > 0 && s.Files > 0 {
		return float64(s.Files) / float64(s.TotalFiles) * 100
	}
	if s.TotalBytes > 0 && s.Bytes > 0 {
		return float64(s.Bytes) / float64(s.TotalBytes) * 100
	}
	return -1
}

// Elapsed returns time since the operation started.
func (s ProgressState) Elapsed() time.Duration {
	return time.Since(s.Started)
}

// ProgressReporter sends framed progress updates to a writer.
// Protocol: each frame is "PROGRESS <json>\n" or "OK ...\n" / "ERR ...\n".
type ProgressReporter struct {
	mu       sync.Mutex
	w        io.Writer
	state    ProgressState
	lastSend time.Time
}

// NewProgressReporter creates a reporter that writes to w.
func NewProgressReporter(w io.Writer) *ProgressReporter {
	return &ProgressReporter{
		w: w,
		state: ProgressState{
			Started: time.Now(),
		},
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
	// Throttle: send at most every 100ms.
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

func (r *ProgressReporter) send() {
	// Don't spam too fast; callers already check throttle.
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
	fmt.Fprintf(r.w, "PROGRESS %s%s\n", msg, extra)
	r.lastSend = time.Now()
}

// Done sends the final OK message.
func (r *ProgressReporter) Done(msg string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.Phase = PhaseDone
	fmt.Fprintf(r.w, "OK %s\n", msg)
}

// Error sends an error message.
func (r *ProgressReporter) Error(msg string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	fmt.Fprintf(r.w, "ERR %s\n", msg)
}

// --- Terminal progress display (CLI side) ---

// ProgressDisplay renders a live progress bar on a terminal.
type ProgressDisplay struct {
	lastLine string
	w        io.Writer
}

// NewProgressDisplay creates a display that writes to w (usually os.Stderr).
func NewProgressDisplay(w io.Writer) *ProgressDisplay {
	return &ProgressDisplay{w: w}
}

// Update renders a progress frame. Call with each PROGRESS line received.
func (d *ProgressDisplay) Update(line string) {
	// line is "PROGRESS <msg>"
	msg := strings.TrimPrefix(line, "PROGRESS ")
	line = fmt.Sprintf("\r  %s", msg)
	// Pad to clear previous line.
	if len(line) < len(d.lastLine) {
		line += strings.Repeat(" ", len(d.lastLine)-len(line))
	}
	fmt.Fprint(d.w, line)
	d.lastLine = line
}

// Done prints the final status line.
func (d *ProgressDisplay) Done(line string) {
	// Clear the progress line.
	if len(d.lastLine) > 0 {
		fmt.Fprintf(d.w, "\r%s\r", strings.Repeat(" ", len(d.lastLine)))
	}
	// Print final message on its own line.
	msg := strings.TrimPrefix(line, "OK ")
	fmt.Fprintf(d.w, "  ✓ %s\n", msg)
	d.lastLine = ""
}

// Error prints the error.
func (d *ProgressDisplay) Error(line string) {
	if len(d.lastLine) > 0 {
		fmt.Fprintf(d.w, "\r%s\r", strings.Repeat(" ", len(d.lastLine)))
	}
	msg := strings.TrimPrefix(line, "ERR ")
	fmt.Fprintf(d.w, "  ✗ error: %s\n", msg)
	d.lastLine = ""
}

// formatBytes formats byte counts human-readably.
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
