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
// and persists recent output so late-attaching clients can catch up.
type commitHub struct {
	mu        sync.Mutex
	sockPath  string // monitor socket path
	listener  net.Listener
	watchers  map[net.Conn]struct{}
	jobID     atomic.Int64 // 0 = idle, >0 = running
	ended     atomic.Bool  // true after OK/ERR sent
	lastLines []string     // ring buffer of recent output for catch-up
}

// globalCommitHub is the singleton hub, created by StartCommitListener.
var globalCommitHub *commitHub

// newCommitHub creates a hub and starts listening on the monitor socket.
// The monitor socket path is <mainSocket>.monitor.
func newCommitHub(mainSocketPath string) (*commitHub, error) {
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

	h := &commitHub{
		sockPath: monPath,
		listener: l,
		watchers: make(map[net.Conn]struct{}),
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

		// Send catch-up lines first so the watcher sees what happened
		// before it connected.
		for _, line := range h.lastLines {
			_, _ = fmt.Fprintln(conn, line)
		}

		// If commit already ended, send a terminal status and close.
		if h.ended.Load() {
			// lastLines already has OK/ERR — just close.
			h.mu.Unlock()
			_ = conn.Close()
			continue
		}

		h.watchers[conn] = struct{}{}
		h.mu.Unlock()
	}
}

// broadcast sends a line to all connected watchers.
func (h *commitHub) broadcast(line string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Append to catch-up buffer (keep last 256 lines).
	h.lastLines = append(h.lastLines, line)
	if len(h.lastLines) > 256 {
		h.lastLines = h.lastLines[len(h.lastLines)-256:]
	}

	// Detect terminal lines.
	isDone := len(line) >= 3 && (line[:3] == "OK " || line[:3] == "ERR")
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

// startJob marks a commit as running and returns a job ID.
func (h *commitHub) startJob() int64 {
	h.mu.Lock()
	h.lastLines = nil
	h.ended.Store(false)
	h.mu.Unlock()
	id := time.Now().UnixMilli()
	h.jobID.Store(id)
	return id
}

// endJob clears the running commit state.
func (h *commitHub) endJob() {
	h.jobID.Store(0)
}

// close shuts down the hub and the monitor socket listener.
func (h *commitHub) close() {
	if h.listener != nil {
		_ = h.listener.Close()
	}
	_ = os.Remove(h.sockPath)
}

// hubProgressReporter sends progress only to the commit hub (no direct connection).
type hubProgressReporter struct {
	state    ProgressState
	lastSend time.Time
}

func newHubProgressReporter() *hubProgressReporter {
	return &hubProgressReporter{}
}

func (r *hubProgressReporter) SetPhase(phase ProgressPhase) {
	r.state.Phase = phase
	if globalCommitHub != nil {
		label, ok := phaseLabels[phase]
		if !ok {
			label = "Working"
		}
		globalCommitHub.broadcast("PROGRESS " + label)
	}
}

func (r *hubProgressReporter) SetMsg(msg string) {
	r.state.Msg = msg
	if globalCommitHub != nil {
		globalCommitHub.broadcast("PROGRESS " + msg)
	}
}

func (r *hubProgressReporter) AddFile(bytes int64) {
	r.state.Files++
	r.state.Bytes += bytes
	if globalCommitHub != nil && time.Since(r.lastSend) > 100*time.Millisecond {
		extra := fmt.Sprintf(" (%d files, %s)", r.state.Files, formatBytes(r.state.Bytes))
		globalCommitHub.broadcast("PROGRESS " + r.state.Msg + extra)
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
}

func (f *fanoutReporter) SetPhase(phase ProgressPhase) {
	f.primary.SetPhase(phase)
	label, ok := phaseLabels[phase]
	if !ok {
		label = "Working"
	}
	f.hub.broadcast("PROGRESS " + label)
}

func (f *fanoutReporter) SetMsg(msg string) {
	f.primary.SetMsg(msg)
	f.hub.broadcast("PROGRESS " + msg)
}

func (f *fanoutReporter) AddFile(bytes int64) {
	f.primary.AddFile(bytes)
	state := f.primary.State()
	if time.Since(f.lastSend) > 100*time.Millisecond {
		extra := fmt.Sprintf(" (%d files, %s)", state.Files, formatBytes(state.Bytes))
		f.hub.broadcast("PROGRESS " + state.Msg + extra)
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
