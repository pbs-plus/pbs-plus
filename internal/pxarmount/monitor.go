package pxarmount

import (
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// commitHub broadcasts commit progress lines to all connected watchers
// on the monitor socket and appends them to a log file for later retrieval.
type commitHub struct {
	mu        sync.Mutex
	sockPath  string // monitor socket path (<mainSocket>.monitor)
	logPath   string // log file path (<mainSocket>.log)
	listener  net.Listener
	logFile   *os.File // append-only log file for commit output
	watchers  map[net.Conn]struct{}
	jobID     atomic.Int64 // 0 = idle, >0 = running
	ended     atomic.Bool  // true after OK/ERR sent
	lastLines []string     // ring buffer of recent output for catch-up
	verbose   bool         // enable verbose/debug logging
}

// globalCommitHub is the singleton hub, created by StartCommitListener.
var globalCommitHub *commitHub

// newCommitHub creates a hub and starts listening on the monitor socket.
// The monitor socket path is <mainSocket>.monitor.
// The log file path is <mainSocket>.log.
func newCommitHub(mainSocketPath string, verbose bool) (*commitHub, error) {
	monPath := mainSocketPath + ".monitor"
	_ = os.Remove(monPath)

	l, err := net.Listen("unix", monPath)
	if err != nil {
		return nil, fmt.Errorf("listen monitor socket %s: %w", monPath, err)
	}
	if err := os.Chmod(monPath, 0o660); err != nil {
		_ = l.Close()
		return nil, err
	}

	logPath := mainSocketPath + ".log"

	h := &commitHub{
		sockPath: monPath,
		logPath:  logPath,
		listener: l,
		watchers: make(map[net.Conn]struct{}),
		verbose:  verbose,
	}

	go h.acceptLoop()
	return h, nil
}

// MonitorSocketPath returns the path to the monitor socket (or "" if no hub).
func MonitorSocketPath() string {
	if globalCommitHub == nil {
		return ""
	}
	return globalCommitHub.sockPath
}

// LogFilePath returns the path to the commit log file (or "" if no hub).
func LogFilePath() string {
	if globalCommitHub == nil {
		return ""
	}
	return globalCommitHub.logPath
}

// IsCommitRunning returns true if a commit job is currently active.
func IsCommitRunning() bool {
	if globalCommitHub == nil {
		return false
	}
	return globalCommitHub.jobID.Load() > 0
}

// acceptLoop accepts watcher connections on the monitor socket.
func (h *commitHub) acceptLoop() {
	for {
		conn, err := h.listener.Accept()
		if err != nil {
			return
		}
		h.mu.Lock()

		if h.jobID.Load() > 0 {
			if h.ended.Load() {
				// Commit already finished — replay all lines and close.
				for _, line := range h.lastLines {
					_, _ = fmt.Fprintln(conn, line)
				}
				h.mu.Unlock()
				_ = conn.Close()
				continue
			}

			// Add to watchers FIRST so that concurrent broadcasts
			// are delivered to this conn while we send catch-up lines.
			// This eliminates the race where lines broadcast between
			// reading lastLines and adding to watchers are lost.
			h.watchers[conn] = struct{}{}

			// Now replay catch-up lines. Any new broadcast() call
			// during this loop will see conn in watchers and write
			// to it directly (duplicate lines are harmless — the
			// display deduplicates by phase transitions).
			for _, line := range h.lastLines {
				_, _ = fmt.Fprintln(conn, line)
			}
			h.mu.Unlock()
		} else {
			// No active commit — nothing to attach to.
			_, _ = fmt.Fprintln(conn, "IDLE")
			h.mu.Unlock()
			_ = conn.Close()
		}
	}
}

// broadcast sends a line to all connected watchers and appends it to the log file.
func (h *commitHub) broadcast(line string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Append to catch-up buffer (keep last 1024 lines for better
	// catch-up coverage on long-running commits).
	h.lastLines = append(h.lastLines, line)
	if len(h.lastLines) > 1024 {
		h.lastLines = h.lastLines[len(h.lastLines)-1024:]
	}

	// Append to log file with immediate sync for durability.
	if h.logFile != nil {
		if _, err := fmt.Fprintln(h.logFile, line); err != nil {
			if h.verbose {
				fmt.Fprintf(os.Stderr, "commit-hub: log write error: %v\n", err)
			}
		} else {
			// Sync every write so the log is always readable.
			// This is cheap: one fsync per progress line (~10/s)
			// and guarantees the log is complete even on crash.
			if err := h.logFile.Sync(); err != nil && h.verbose {
				fmt.Fprintf(os.Stderr, "commit-hub: log sync error: %v\n", err)
			}
		}
	}

	// Detect terminal lines.
	isDone := len(line) >= 3 && (line[:3] == "OK " || line[:3] == "ERR ")
	if isDone {
		h.ended.Store(true)
	}

	for conn := range h.watchers {
		if _, err := fmt.Fprintln(conn, line); err != nil {
			_ = conn.Close()
			delete(h.watchers, conn)
		} else if isDone {
			_ = conn.Close()
			delete(h.watchers, conn)
		}
	}
}

// startJob marks a commit as running, truncates the log file, and returns a job ID.
func (h *commitHub) startJob() int64 {
	h.mu.Lock()
	h.lastLines = nil
	h.ended.Store(false)

	// Truncate/create the log file for the new commit.
	if h.logFile != nil {
		_ = h.logFile.Close()
	}
	f, err := os.Create(h.logPath)
	if err != nil {
		if h.verbose {
			fmt.Fprintf(os.Stderr, "commit-hub: failed to create log file %s: %v\n", h.logPath, err)
		}
	} else {
		_ = f.Chmod(0o660)
		h.logFile = f
		if h.verbose {
			fmt.Fprintf(os.Stderr, "commit-hub: log file created at %s\n", h.logPath)
		}
	}
	h.mu.Unlock()

	id := time.Now().UnixMilli()
	h.jobID.Store(id)

	// Broadcast a start marker so the log always has at least one entry.
	h.broadcast("PROGRESS [Preparing] Commit started")

	return id
}

// endJob clears the running commit state and closes the log file.
func (h *commitHub) endJob() {
	h.mu.Lock()
	if h.logFile != nil {
		// Final sync before close to ensure all data is on disk.
		if err := h.logFile.Sync(); err != nil && h.verbose {
			fmt.Fprintf(os.Stderr, "commit-hub: final log sync error: %v\n", err)
		}
		if err := h.logFile.Close(); err != nil && h.verbose {
			fmt.Fprintf(os.Stderr, "commit-hub: log close error: %v\n", err)
		}
		h.logFile = nil
	}
	if h.verbose {
		fmt.Fprintf(os.Stderr, "commit-hub: job ended, %d lines in catch-up buffer\n", len(h.lastLines))
	}
	h.mu.Unlock()
	h.jobID.Store(0)
}

// close shuts down the hub and the monitor socket listener.
func (h *commitHub) close() {
	if h.listener != nil {
		_ = h.listener.Close()
	}
	h.mu.Lock()
	if h.logFile != nil {
		_ = h.logFile.Close()
		h.logFile = nil
	}
	h.mu.Unlock()
	_ = os.Remove(h.sockPath)
}

// formatElapsed formats a duration for the PROGRESS protocol.
func formatElapsed(d time.Duration) string {
	return fmt.Sprintf("%.1fs", d.Seconds())
}

// progressLine builds a PROGRESS protocol line with embedded elapsed time.
// Format: PROGRESS [Phase] {elapsed} message
func progressLine(phase, msg string, started time.Time) string {
	return fmt.Sprintf("PROGRESS [%s] {%s} %s", phase, formatElapsed(time.Since(started)), msg)
}

// progressLineWithStats builds a PROGRESS protocol line with elapsed time and file stats.
// Format: PROGRESS [Phase] {elapsed} message (N files, X.X MiB)
func progressLineWithStats(phase, msg string, files int64, bytes int64, started time.Time) string {
	extra := fmt.Sprintf(" (%d files, %s)", files, formatBytes(bytes))
	return fmt.Sprintf("PROGRESS [%s] {%s} %s%s", phase, formatElapsed(time.Since(started)), msg, extra)
}

// hubProgressReporter sends progress only to the commit hub (no direct connection).
type hubProgressReporter struct {
	state    ProgressState
	lastSend time.Time
	started  time.Time
}

func newHubProgressReporter() *hubProgressReporter {
	return &hubProgressReporter{started: time.Now()}
}

func (r *hubProgressReporter) SetPhase(phase ProgressPhase) {
	r.state.Phase = phase
	if globalCommitHub != nil {
		label, ok := phaseLabels[phase]
		if !ok {
			label = "Working"
		}
		globalCommitHub.broadcast(progressLine(label, label, r.started))
	}
}

func (r *hubProgressReporter) SetMsg(msg string) {
	r.state.Msg = msg
	if globalCommitHub != nil {
		label, ok := phaseLabels[r.state.Phase]
		if !ok {
			label = "Working"
		}
		globalCommitHub.broadcast(progressLine(label, msg, r.started))
	}
}

func (r *hubProgressReporter) AddFile(bytes int64) {
	r.state.Files++
	r.state.Bytes += bytes
	if globalCommitHub != nil && time.Since(r.lastSend) > 100*time.Millisecond {
		label, ok := phaseLabels[r.state.Phase]
		if !ok {
			label = "Working"
		}
		msg := r.state.Msg
		if msg == "" {
			msg = label
		}
		globalCommitHub.broadcast(progressLineWithStats(label, msg, r.state.Files, r.state.Bytes, r.started))
		r.lastSend = time.Now()
	}
}

func (r *hubProgressReporter) SetTotals(files, bytes int64) {
	r.state.TotalFiles = files
	r.state.TotalBytes = bytes
}

func (r *hubProgressReporter) State() ProgressState {
	return r.state
}

func (r *hubProgressReporter) Done(msg string) {
	r.state.Phase = PhaseDone
	if globalCommitHub != nil {
		globalCommitHub.broadcast("OK " + msg)
	}
}

func (r *hubProgressReporter) Error(msg string) {
	if globalCommitHub != nil {
		globalCommitHub.broadcast("ERR " + msg)
	}
}

// fanoutReporter sends progress to both a primary connection and the hub.
type fanoutReporter struct {
	primary  *ProgressReporter
	hub      *commitHub
	lastSend time.Time
	started  time.Time
}

func (f *fanoutReporter) SetPhase(phase ProgressPhase) {
	f.primary.SetPhase(phase)
	label, ok := phaseLabels[phase]
	if !ok {
		label = "Working"
	}
	f.hub.broadcast(progressLine(label, label, f.started))
}

func (f *fanoutReporter) SetMsg(msg string) {
	f.primary.SetMsg(msg)
	state := f.primary.State()
	label, ok := phaseLabels[state.Phase]
	if !ok {
		label = "Working"
	}
	f.hub.broadcast(progressLine(label, msg, f.started))
}

func (f *fanoutReporter) AddFile(bytes int64) {
	f.primary.AddFile(bytes)
	state := f.primary.State()
	if time.Since(f.lastSend) > 100*time.Millisecond {
		label, ok := phaseLabels[state.Phase]
		if !ok {
			label = "Working"
		}
		msg := state.Msg
		if msg == "" {
			msg = label
		}
		f.hub.broadcast(progressLineWithStats(label, msg, state.Files, state.Bytes, f.started))
		f.lastSend = time.Now()
	}
}

func (f *fanoutReporter) SetTotals(files, bytes int64) {
	f.primary.SetTotals(files, bytes)
}

func (f *fanoutReporter) State() ProgressState {
	return f.primary.State()
}

func (f *fanoutReporter) Done(msg string) {
	f.primary.Done(msg)
	f.hub.broadcast("OK " + msg)
}

func (f *fanoutReporter) Error(msg string) {
	f.primary.Error(msg)
	f.hub.broadcast("ERR " + msg)
}
