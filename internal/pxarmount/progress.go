package pxarmount

import (
	"fmt"
	"io"
	"strconv"
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
	PhaseWalk:     "Archiving",
	PhaseUpload:   "Flushing upload",
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
	started  time.Time
}

// NewProgressReporter creates a reporter that writes to w.
func NewProgressReporter(w io.Writer) *ProgressReporter {
	return &ProgressReporter{
		w:       w,
		state:   ProgressState{},
		started: time.Now(),
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
	_, _ = fmt.Fprintf(r.w, "PROGRESS [%s] {%s} %s%s\n", label, formatElapsed(time.Since(r.started)), msg, extra)
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

// ProgressDisplay renders a live progress bar with spinner animation on a terminal.
type ProgressDisplay struct {
	mu       sync.Mutex
	w        io.Writer
	lastLine string

	// spinner state
	frames           []string
	frame            int
	phase            string
	msg              string
	files            int64
	bytes            int64
	started          time.Time
	hasServerElapsed bool // true once we've parsed a server-side {elapsed} marker

	ticker *time.Ticker
	done   chan struct{}
}

// NewProgressDisplay creates a display that writes to w with spinner animation.
func NewProgressDisplay(w io.Writer) *ProgressDisplay {
	d := &ProgressDisplay{
		w: w,
		frames: []string{
			"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏",
		},
		started: time.Now(),
		done:    make(chan struct{}),
	}
	d.ticker = time.NewTicker(80 * time.Millisecond)
	go d.spin()
	return d
}

// stop halts the spinner goroutine.
func (d *ProgressDisplay) stop() {
	if d.ticker != nil {
		d.ticker.Stop()
		d.ticker = nil
	}
	select {
	case <-d.done:
	default:
		close(d.done)
	}
}

// spin is the background goroutine that animates the spinner.
func (d *ProgressDisplay) spin() {
	for range d.ticker.C {
		d.mu.Lock()
		d.frame = (d.frame + 1) % len(d.frames)
		line := d.render()
		d.mu.Unlock()
		if line != "" {
			d.writeLine(line)
		}
	}
}

// render builds the current display line (must hold mu).
func (d *ProgressDisplay) render() string {
	if d.phase == "" && d.msg == "" {
		return ""
	}
	spinner := d.frames[d.frame]

	var parts []string

	// Build display label: prefer msg, fall back to phase.
	// Avoid showing both when they're the same.
	switch {
	case d.msg != "" && d.phase != "" && d.msg != d.phase:
		parts = append(parts, d.phase, d.msg)
	case d.msg != "":
		parts = append(parts, d.msg)
	case d.phase != "":
		parts = append(parts, d.phase)
	}

	if d.files > 0 || d.bytes > 0 {
		var detail string
		if d.files > 0 {
			detail = fmt.Sprintf("%d files", d.files)
		}
		if d.bytes > 0 {
			if detail != "" {
				detail += ", "
			}
			detail += formatBytes(d.bytes)
		}
		parts = append(parts, detail)
	}

	// Show throughput for long-running operations
	if elapsed := time.Since(d.started); elapsed > 2*time.Second && d.bytes > 0 {
		rate := float64(d.bytes) / elapsed.Seconds()
		elapsedStr := formatDuration(elapsed)
		parts = append(parts, fmt.Sprintf("%s · %s/s", elapsedStr, formatBytes(int64(rate))))
	}

	body := strings.Join(parts, " · ")
	return fmt.Sprintf("\r  %s %s", spinner, body)
}

// writeLine writes a line to the terminal, clearing previous content.
func (d *ProgressDisplay) writeLine(line string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	padded := line
	if len(padded) < len(d.lastLine) {
		padded += strings.Repeat(" ", len(d.lastLine)-len(padded))
	}
	_, _ = fmt.Fprint(d.w, padded)
	d.lastLine = line
}

// Update parses a PROGRESS line and updates the display.
func (d *ProgressDisplay) Update(line string) {
	msg := strings.TrimPrefix(line, "PROGRESS ")
	d.mu.Lock()

	// Reset phase/msg for this update.
	d.phase = ""
	d.msg = ""

	// Parse phase tag: [PhaseLabel] at the start.
	if len(msg) > 0 && msg[0] == '[' {
		if end := strings.Index(msg, "]"); end > 0 {
			d.phase = msg[1:end]
			msg = strings.TrimSpace(msg[end+1:])
		}
	}

	// Parse server-side elapsed marker: {12.3s}
	if len(msg) > 0 && msg[0] == '{' {
		if end := strings.Index(msg, "}"); end > 0 {
			elapsedStr := msg[1:end]
			msg = strings.TrimSpace(msg[end+1:])
			if secs, err := strconv.ParseFloat(strings.TrimSuffix(elapsedStr, "s"), 64); err == nil {
				d.hasServerElapsed = true
				d.started = time.Now().Add(-time.Duration(secs * float64(time.Second)))
			}
		}
	}

	// Extract trailing parenthesized stats if present.
	hasStats := false
	if idx := strings.LastIndex(msg, "("); idx > 0 {
		stats := msg[idx:]
		if strings.HasSuffix(stats, ")") {
			msg = msg[:idx]
			d.parseStats(stats)
			hasStats = true
		}
	}
	if !hasStats {
		// Preserve previously parsed stats when the line has none.
		// Only reset if the message itself looks like a phase transition.
		for _, label := range phaseLabels {
			if msg == label {
				d.files = 0
				d.bytes = 0
				if !d.hasServerElapsed {
					d.started = time.Now()
				}
				break
			}
		}
	}

	d.msg = strings.TrimSpace(msg)

	d.frame = (d.frame + 1) % len(d.frames)
	d.mu.Unlock()
}

// parseStats extracts files/bytes from "(N files, X.X MiB)" or "(N files)".
func (d *ProgressDisplay) parseStats(stats string) {
	stats = strings.Trim(stats, "()")
	fields := strings.SplitSeq(stats, ",")
	for f := range fields {
		f = strings.TrimSpace(f)
		if strings.HasSuffix(f, " files") {
			_, _ = fmt.Sscanf(f, "%d files", &d.files)
		} else {
			// Try to parse as byte count
			var b int64
			if strings.HasSuffix(f, "GiB") {
				var v float64
				_, _ = fmt.Sscanf(f, "%f GiB", &v)
				b = int64(v * 1024 * 1024 * 1024)
			} else if strings.HasSuffix(f, "MiB") {
				var v float64
				_, _ = fmt.Sscanf(f, "%f MiB", &v)
				b = int64(v * 1024 * 1024)
			} else if strings.HasSuffix(f, "KiB") {
				var v float64
				_, _ = fmt.Sscanf(f, "%f KiB", &v)
				b = int64(v * 1024)
			}
			if b > 0 {
				d.bytes = b
			}
		}
	}
}

// Done prints the final status line.
func (d *ProgressDisplay) Done(line string) {
	d.stop()
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.lastLine) > 0 {
		_, _ = fmt.Fprintf(d.w, "\r%s\r", strings.Repeat(" ", len(d.lastLine)))
	}
	msg := strings.TrimPrefix(line, "OK ")
	_, _ = fmt.Fprintf(d.w, "  ✓ %s\n", msg)
	d.lastLine = ""
}

// Error prints the error.
func (d *ProgressDisplay) Error(line string) {
	d.stop()
	d.mu.Lock()
	defer d.mu.Unlock()
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

// formatDuration formats elapsed time for display.
func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	m := int(d.Minutes())
	s := int(d.Seconds()) % 60
	return fmt.Sprintf("%dm%02ds", m, s)
}
