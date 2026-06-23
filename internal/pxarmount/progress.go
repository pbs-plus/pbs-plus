package pxarmount

import (
	"fmt"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"
)

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

type ProgressState struct {
	Phase      ProgressPhase
	Files      int64
	Bytes      int64
	TotalFiles int64
	TotalBytes int64
	Msg        string
}

type ProgressReporter struct {
	mu       sync.Mutex
	w        io.Writer
	state    ProgressState
	lastSend time.Time
	started  time.Time
}

func NewProgressReporter(w io.Writer) *ProgressReporter {
	return &ProgressReporter{
		w:       w,
		state:   ProgressState{},
		started: time.Now(),
	}
}

func (r *ProgressReporter) SetPhase(phase ProgressPhase) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.Phase = phase
	r.send()
}

func (r *ProgressReporter) SetMsg(msg string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.Msg = msg
	r.send()
}

func (r *ProgressReporter) AddFile(bytes int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.Files++
	r.state.Bytes += bytes
	if time.Since(r.lastSend) > 100*time.Millisecond {
		r.send()
	}
}

func (r *ProgressReporter) SetTotals(files, bytes int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.TotalFiles = files
	r.state.TotalBytes = bytes
	r.send()
}

func (r *ProgressReporter) State() ProgressState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state
}

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
	if _, err := fmt.Fprintf(r.w, "PROGRESS [%s] {%s} %s%s\n", label, formatElapsed(time.Since(r.started)), msg, extra); err != nil {
		syslog.L.Error(err).Write()
	}
	r.lastSend = time.Now()
}

func (r *ProgressReporter) Done(msg string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.Phase = PhaseDone
	if _, err := fmt.Fprintf(r.w, "OK %s\n", msg); err != nil {
		syslog.L.Error(err).Write()
	}
}

func (r *ProgressReporter) Error(msg string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, err := fmt.Fprintf(r.w, "ERR %s\n", msg); err != nil {
		syslog.L.Error(err).Write()
	}
}

type ProgressDisplay struct {
	mu       sync.Mutex
	w        io.Writer
	lastLine string

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

func (d *ProgressDisplay) render() string {
	if d.phase == "" && d.msg == "" {
		return ""
	}
	spinner := d.frames[d.frame]

	var parts []string

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

	if elapsed := time.Since(d.started); elapsed > 2*time.Second && d.bytes > 0 {
		rate := float64(d.bytes) / elapsed.Seconds()
		elapsedStr := formatDuration(elapsed)
		parts = append(parts, fmt.Sprintf("%s · %s/s", elapsedStr, formatBytes(int64(rate))))
	}

	body := strings.Join(parts, " · ")
	return fmt.Sprintf("\r  %s %s", spinner, body)
}

func (d *ProgressDisplay) writeLine(line string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	padded := line
	if len(padded) < len(d.lastLine) {
		padded += strings.Repeat(" ", len(d.lastLine)-len(padded))
	}
	if _, err := fmt.Fprint(d.w, padded); err != nil {
		syslog.L.Error(err).Write()
	}
	d.lastLine = line
}

func (d *ProgressDisplay) Update(line string) {
	msg := strings.TrimPrefix(line, "PROGRESS ")
	d.mu.Lock()

	d.phase = ""
	d.msg = ""

	if len(msg) > 0 && msg[0] == '[' {
		if end := strings.Index(msg, "]"); end > 0 {
			d.phase = msg[1:end]
			msg = strings.TrimSpace(msg[end+1:])
		}
	}

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

func (d *ProgressDisplay) parseStats(stats string) {
	stats = strings.Trim(stats, "()")
	fields := strings.SplitSeq(stats, ",")
	for f := range fields {
		f = strings.TrimSpace(f)
		if strings.HasSuffix(f, " files") {
			if _, err := fmt.Sscanf(f, "%d files", &d.files); err != nil {
				syslog.L.Error(err).Write()
			}
		} else {
			var b int64
			if strings.HasSuffix(f, "GiB") {
				var v float64
				if _, err := fmt.Sscanf(f, "%f GiB", &v); err != nil {
					syslog.L.Error(err).Write()
				}
				b = int64(v * 1024 * 1024 * 1024)
			} else if strings.HasSuffix(f, "MiB") {
				var v float64
				if _, err := fmt.Sscanf(f, "%f MiB", &v); err != nil {
					syslog.L.Error(err).Write()
				}
				b = int64(v * 1024 * 1024)
			} else if strings.HasSuffix(f, "KiB") {
				var v float64
				if _, err := fmt.Sscanf(f, "%f KiB", &v); err != nil {
					syslog.L.Error(err).Write()
				}
				b = int64(v * 1024)
			}
			if b > 0 {
				d.bytes = b
			}
		}
	}
}

func (d *ProgressDisplay) Done(line string) {
	d.stop()
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.lastLine) > 0 {
		if _, err := fmt.Fprintf(d.w, "\r%s\r", strings.Repeat(" ", len(d.lastLine))); err != nil {
			syslog.L.Error(err).Write()
		}
	}
	msg := strings.TrimPrefix(line, "OK ")
	if _, err := fmt.Fprintf(d.w, "  ✓ %s\n", msg); err != nil {
		syslog.L.Error(err).Write()
	}
	d.lastLine = ""
}

func (d *ProgressDisplay) Error(line string) {
	d.stop()
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.lastLine) > 0 {
		if _, err := fmt.Fprintf(d.w, "\r%s\r", strings.Repeat(" ", len(d.lastLine))); err != nil {
			syslog.L.Error(err).Write()
		}
	}
	msg := strings.TrimPrefix(line, "ERR ")
	if _, err := fmt.Fprintf(d.w, "  ✗ error: %s\n", msg); err != nil {
		syslog.L.Error(err).Write()
	}
	d.lastLine = ""
}

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

func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	m := int(d.Minutes())
	s := int(d.Seconds()) % 60
	return fmt.Sprintf("%dm%02ds", m, s)
}
