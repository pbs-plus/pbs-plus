package pxarmount

import (
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/safemap"
)

// on the monitor socket and appends them to a log file for later retrieval.
type commitHub struct {
	mu        sync.Mutex
	sockPath  string
	logPath   string
	listener  net.Listener
	logFile   *os.File
	watchers  *safemap.Map[net.Conn, struct{}]
	jobID     atomic.Int64
	ended     atomic.Bool
	lastLines []string
	verbose   bool
}

var globalCommitHub *commitHub

func newCommitHub(mainSocketPath string, verbose bool) (*commitHub, error) {
	monPath := mainSocketPath + ".monitor"
	if err := os.Remove(monPath); err != nil && !os.IsNotExist(err) {
		log.Error(err, "")
	}

	l, err := net.Listen("unix", monPath)
	if err != nil {
		return nil, fmt.Errorf("listen monitor socket %s: %w", monPath, err)
	}
	if err := os.Chmod(monPath, 0o660); err != nil {
		if err := l.Close(); err != nil {
			log.Error(err, "")
		}
		return nil, err
	}

	logPath := mainSocketPath + ".log"

	h := &commitHub{
		sockPath: monPath,
		logPath:  logPath,
		listener: l,
		watchers: safemap.New[net.Conn, struct{}](),
		verbose:  verbose,
	}

	go h.acceptLoop()
	return h, nil
}

func MonitorSocketPath() string {
	if globalCommitHub == nil {
		return ""
	}
	return globalCommitHub.sockPath
}

func LogFilePath() string {
	if globalCommitHub == nil {
		return ""
	}
	return globalCommitHub.logPath
}

func IsCommitRunning() bool {
	if globalCommitHub == nil {
		return false
	}
	return globalCommitHub.jobID.Load() > 0
}

func (h *commitHub) acceptLoop() {
	for {
		conn, err := h.listener.Accept()
		if err != nil {
			return
		}

		h.mu.Lock()
		jobRunning := h.jobID.Load() > 0
		jobEnded := h.ended.Load()
		if jobRunning && !jobEnded {
			h.watchers.Set(conn, struct{}{})
		}
		catchUp := h.lastLines
		h.mu.Unlock()

		if jobRunning {
			for _, line := range catchUp {
				if _, err := fmt.Fprintln(conn, line); err != nil {
					log.Error(err, "")
				}
			}
			if jobEnded {
				if err := conn.Close(); err != nil {
					log.Error(err, "")
				}
			}
		} else {
			if _, err := fmt.Fprintln(conn, "IDLE"); err != nil {
				log.Error(err, "")
			}
			if err := conn.Close(); err != nil {
				log.Error(err, "")
			}
		}
	}
}

func (h *commitHub) broadcast(line string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Append to catch-up buffer (keep last 1024 lines for better
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

	isDone := len(line) >= 3 && (line[:3] == "OK " || line[:3] == "ERR ")
	if isDone {
		h.ended.Store(true)
	}

	h.watchers.ForEach(func(conn net.Conn, _ struct{}) bool {
		if _, err := fmt.Fprintln(conn, line); err != nil {
			log.Error(err, "")
			if err := conn.Close(); err != nil {
				log.Error(err, "")
			}
			h.watchers.Del(conn)
		} else if isDone {
			if err := conn.Close(); err != nil {
				log.Error(err, "")
			}
			h.watchers.Del(conn)
		}
		return true
	})
}

func (h *commitHub) startJob() int64 {
	h.mu.Lock()
	h.lastLines = nil
	h.ended.Store(false)

	// Truncate/create the log file for the new commit.
	if h.logFile != nil {
		if err := h.logFile.Close(); err != nil {
			log.Error(err, "")
		}
	}
	f, err := os.Create(h.logPath)
	if err != nil {
		if h.verbose {
			fmt.Fprintf(os.Stderr, "commit-hub: failed to create log file %s: %v\n", h.logPath, err)
		}
	} else {
		if err := f.Chmod(0o660); err != nil {
			log.Error(err, "")
		}
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

func (h *commitHub) close() {
	if h.listener != nil {
		if err := h.listener.Close(); err != nil {
			log.Error(err, "")
		}
	}
	h.mu.Lock()
	if h.logFile != nil {
		if err := h.logFile.Close(); err != nil {
			log.Error(err, "")
		}
		h.logFile = nil
	}
	h.mu.Unlock()
	if err := os.Remove(h.sockPath); err != nil && !os.IsNotExist(err) {
		log.Error(err, "")
	}
}

func formatElapsed(d time.Duration) string {
	return fmt.Sprintf("%.1fs", d.Seconds())
}

func progressLine(phase, msg string, started time.Time) string {
	return fmt.Sprintf("PROGRESS [%s] {%s} %s", phase, formatElapsed(time.Since(started)), msg)
}

func progressLineWithStats(phase, msg string, files int64, bytes int64, started time.Time) string {
	extra := fmt.Sprintf(" (%d files, %s)", files, formatBytes(bytes))
	return fmt.Sprintf("PROGRESS [%s] {%s} %s%s", phase, formatElapsed(time.Since(started)), msg, extra)
}

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
