package syslog

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// Global logger instance.
var L *Logger

// Deduplicator tracks recently seen log entries to prevent spam.
type Deduplicator struct {
	mu      sync.RWMutex
	entries map[[32]byte]time.Time // Use fixed-size array as key instead of string
	window  time.Duration
}

func newDeduplicator(window time.Duration) *Deduplicator {
	d := &Deduplicator{
		entries: make(map[[32]byte]time.Time),
		window:  window,
	}
	// Start cleanup goroutine
	go d.cleanup()
	return d
}

// cleanup periodically removes expired entries.
func (d *Deduplicator) cleanup() {
	ticker := time.NewTicker(d.window)
	defer ticker.Stop()

	for range ticker.C {
		d.mu.Lock()
		now := time.Now()
		for key, timestamp := range d.entries {
			if now.Sub(timestamp) > d.window {
				delete(d.entries, key)
			}
		}
		d.mu.Unlock()
	}
}

// shouldLog returns true if the log should be emitted.
func (d *Deduplicator) shouldLog(key [32]byte) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()
	if lastSeen, exists := d.entries[key]; exists {
		if now.Sub(lastSeen) < d.window {
			return false // Duplicate within window
		}
	}

	d.entries[key] = now
	return true
}

// Pre-allocate buffers for key generation
var keyBufferPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 0, 512) // Reasonable initial capacity
		return &b
	},
}

// generateKey creates a unique key for a log entry.
func (e *LogEntry) generateKey() [32]byte {
	// Get buffer from pool
	bufPtr := keyBufferPool.Get().(*[]byte)
	buf := (*bufPtr)[:0] // Reset length but keep capacity
	defer func() {
		*bufPtr = buf
		keyBufferPool.Put(bufPtr)
	}()

	// Append level
	buf = append(buf, e.Level...)
	buf = append(buf, '|')

	// Append message
	buf = append(buf, e.Message...)
	buf = append(buf, '|')

	// Append jobID
	buf = append(buf, e.JobID...)
	buf = append(buf, '|')

	// Append error message if present
	if e.Err != nil {
		buf = append(buf, e.Err.Error()...)
	}
	buf = append(buf, '|')

	// Append fields (sorted for consistency)
	if len(e.Fields) > 0 {
		// Use json.Marshal only if we have fields
		fieldsJSON, _ := json.Marshal(e.Fields)
		buf = append(buf, fieldsJSON...)
	}

	// Return hash directly as fixed-size array
	return sha256.Sum256(buf)
}

func init() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if os.Getenv("DEBUG") == "true" {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	// Configure zerolog to output via our EventLogWriter wrapped in a ConsoleWriter.
	zlogger := zerolog.New(zerolog.NewConsoleWriter(func(w *zerolog.ConsoleWriter) {
		w.NoColor = true
		w.FormatCaller = func(i any) string {
			var c string
			if cc, ok := i.(string); ok {
				c = cc
			}
			if c == "" {
				return ""
			}

			parts := strings.Split(c, "/")
			if len(parts) >= 2 {
				return fmt.Sprintf("%s/%s", parts[len(parts)-2], parts[len(parts)-1])
			}
			return filepath.Base(c)
		}
	})).With().
		CallerWithSkipFrameCount(3).
		Timestamp().
		Logger()

	dedupWindow := 5 * time.Second
	if window := os.Getenv("LOG_DEDUP_WINDOW"); window != "" {
		if d, err := time.ParseDuration(window); err == nil {
			dedupWindow = d
		}
	}

	L = &Logger{
		zlog:         &zlogger,
		dedup:        newDeduplicator(dedupWindow),
		dedupEnabled: true,
	}
}

func (l *Logger) Disable() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.disabled = true
}

func (l *Logger) Enable() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.disabled = false
}

// DisableDeduplication turns off log deduplication.
func (l *Logger) DisableDeduplication() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.dedupEnabled = false
}

// EnableDeduplication turns on log deduplication.
func (l *Logger) EnableDeduplication() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.dedupEnabled = true
}

// Pre-allocated LogEntry pool
var logEntryPool = sync.Pool{
	New: func() interface{} {
		return &LogEntry{
			Fields: make(map[string]any, 8), // Pre-allocate some capacity
		}
	},
}

func getLogEntry() *LogEntry {
	e := logEntryPool.Get().(*LogEntry)
	// Clear previous data
	e.Level = ""
	e.Message = ""
	e.JobID = ""
	e.Err = nil
	e.ErrString = ""
	e.skipDedup = false
	// Clear map but keep allocation
	for k := range e.Fields {
		delete(e.Fields, k)
	}
	return e
}

func putLogEntry(e *LogEntry) {
	logEntryPool.Put(e)
}

// Error creates a new error-level LogEntry.
func (l *Logger) Error(err error) *LogEntry {
	e := getLogEntry()
	e.Level = "error"
	e.Err = err
	e.logger = l
	return e
}

// Warn creates a new warning-level LogEntry.
func (l *Logger) Warn() *LogEntry {
	e := getLogEntry()
	e.Level = "warn"
	e.logger = l
	return e
}

// Info creates a new info-level LogEntry.
func (l *Logger) Info() *LogEntry {
	e := getLogEntry()
	e.Level = "info"
	e.logger = l
	return e
}

func (l *Logger) Debug() *LogEntry {
	e := getLogEntry()
	e.Level = "debug"
	e.logger = l
	return e
}

// WithMessage sets the log message.
func (e *LogEntry) WithMessage(msg string) *LogEntry {
	e.Message = msg
	return e
}

// WithJob sets the job ID.
func (e *LogEntry) WithJob(jobId string) *LogEntry {
	e.JobID = jobId
	return e
}

// WithJSON attempts to unmarshal the input JSON and merge the fields.
func (e *LogEntry) WithJSON(msg string) *LogEntry {
	var parsed map[string]any
	if err := json.Unmarshal([]byte(msg), &parsed); err == nil {
		// Reuse existing map
		for k, v := range parsed {
			e.Fields[k] = v
		}
	} else {
		e.Message = msg
	}
	return e
}

// WithField adds one key-value pair to the LogEntry.
func (e *LogEntry) WithField(key string, value any) *LogEntry {
	e.Fields[key] = value
	return e
}

// WithFields adds multiple key-value pairs to the LogEntry.
func (e *LogEntry) WithFields(fields map[string]any) *LogEntry {
	for k, v := range fields {
		e.Fields[k] = v
	}
	return e
}

// SkipDedup marks this log entry to bypass deduplication.
func (e *LogEntry) SkipDedup() *LogEntry {
	e.skipDedup = true
	return e
}

// parseLogEntry parses a JSON payload (e.g. sent from a Linux system)
// into a LogEntry.
func parseLogEntry(body io.ReadCloser) (*LogEntry, error) {
	entry := getLogEntry()
	if err := json.NewDecoder(body).Decode(entry); err != nil {
		putLogEntry(entry)
		return nil, err
	}
	entry.logger = L
	if entry.ErrString != "" {
		entry.Err = errors.New(entry.ErrString)
	}
	return entry, nil
}

// ParseAndLogWindowsEntry parses a JSON payload using ParseLogEntry
// and then writes it using the Windows logger.
func ParseAndLogWindowsEntry(body io.ReadCloser) error {
	entry, err := parseLogEntry(body)
	if err != nil {
		return err
	}
	defer putLogEntry(entry) // Return to pool when done

	entry.logger.mu.RLock()
	disabled := entry.logger.disabled
	dedupEnabled := entry.logger.dedupEnabled
	entry.logger.mu.RUnlock()

	if disabled {
		return nil
	}

	// Check deduplication
	if !entry.skipDedup && dedupEnabled && entry.logger.dedup != nil {
		key := entry.generateKey()
		if !entry.logger.dedup.shouldLog(key) {
			return nil // Skip duplicate log
		}
	}

	// Log based on level
	switch entry.Level {
	case "info":
		entry.logger.zlog.Info().Fields(entry.Fields).Msg(entry.Message)
	case "warn":
		entry.logger.zlog.Warn().Fields(entry.Fields).Msg(entry.Message)
	case "error":
		entry.logger.zlog.Error().Err(entry.Err).Fields(entry.Fields).Msg(entry.Message)
	case "debug":
		entry.logger.zlog.Debug().Err(entry.Err).Fields(entry.Fields).Msg(entry.Message)
	default:
		entry.logger.zlog.Info().Fields(entry.Fields).Msg(entry.Message)
	}
	return nil
}
