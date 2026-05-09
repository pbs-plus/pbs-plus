package syslog

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"os"
	"sync"
	"time"

	"log/slog"

	"github.com/pbs-plus/pbs-plus/internal/conf"
)

// Global logger instance.
var L *Logger

// Deduplicator tracks recently seen log entries to prevent spam.
type Deduplicator struct {
	mu      sync.RWMutex
	entries map[[32]byte]time.Time
	window  time.Duration
}

func newDeduplicator(window time.Duration) *Deduplicator {
	d := &Deduplicator{
		entries: make(map[[32]byte]time.Time),
		window:  window,
	}
	go d.cleanup()
	return d
}

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

func (d *Deduplicator) shouldLog(key [32]byte) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	if lastSeen, exists := d.entries[key]; exists {
		if now.Sub(lastSeen) < d.window {
			return false
		}
	}
	d.entries[key] = now
	return true
}

var keyBufferPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 512)
		return &b
	},
}

func (e *LogEntry) generateKey() [32]byte {
	bufPtr := keyBufferPool.Get().(*[]byte)
	buf := (*bufPtr)[:0]
	defer func() {
		*bufPtr = buf
		keyBufferPool.Put(bufPtr)
	}()

	buf = append(buf, e.Level...)
	buf = append(buf, '|')
	buf = append(buf, e.Message...)
	buf = append(buf, '|')
	buf = append(buf, e.JobID...)
	buf = append(buf, '|')
	if e.Err != nil {
		buf = append(buf, e.Err.Error()...)
	}
	buf = append(buf, '|')
	if len(e.Fields) > 0 {
		fieldsJSON, _ := json.Marshal(e.Fields)
		buf = append(buf, fieldsJSON...)
	}
	return sha256.Sum256(buf)
}

func init() {
	level := slog.LevelInfo
	if conf.Env.Debug {
		level = slog.LevelDebug
	}

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level:     level,
		AddSource: true,
	})

	zlogger := slog.New(handler)

	dedupWindow := 5 * time.Second
	if conf.Env.LogDedupWindow != "" {
		if d, err := time.ParseDuration(conf.Env.LogDedupWindow); err == nil {
			dedupWindow = d
		}
	}

	L = &Logger{
		zlog:         zlogger,
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

func (l *Logger) DisableDeduplication() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.dedupEnabled = false
}

func (l *Logger) EnableDeduplication() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.dedupEnabled = true
}

var logEntryPool = sync.Pool{
	New: func() any {
		return &LogEntry{
			Fields: make(map[string]any, 8),
		}
	},
}

func getLogEntry() *LogEntry {
	e := logEntryPool.Get().(*LogEntry)
	e.Level = ""
	e.Message = ""
	e.JobID = ""
	e.Err = nil
	e.ErrString = ""
	e.skipDedup = false
	for k := range e.Fields {
		delete(e.Fields, k)
	}
	return e
}

func putLogEntry(e *LogEntry) {
	logEntryPool.Put(e)
}

func (l *Logger) Error(err error) *LogEntry {
	e := getLogEntry()
	e.Level = "error"
	e.Err = err
	e.logger = l
	return e
}

func (l *Logger) Warn() *LogEntry {
	e := getLogEntry()
	e.Level = "warn"
	e.logger = l
	return e
}

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

func (e *LogEntry) WithMessage(msg string) *LogEntry {
	e.Message = msg
	return e
}

func (e *LogEntry) WithJob(jobId string) *LogEntry {
	e.JobID = jobId
	return e
}

func (e *LogEntry) WithJSON(msg string) *LogEntry {
	var parsed map[string]any
	if err := json.Unmarshal([]byte(msg), &parsed); err == nil {
		maps.Copy(e.Fields, parsed)
	} else {
		e.Message = msg
	}
	return e
}

func (e *LogEntry) WithField(key string, value any) *LogEntry {
	e.Fields[key] = value
	return e
}

func (e *LogEntry) WithFields(fields map[string]any) *LogEntry {
	maps.Copy(e.Fields, fields)
	return e
}

func (e *LogEntry) SkipDedup() *LogEntry {
	e.skipDedup = true
	return e
}

// slogAttrs converts the LogEntry fields to slog key-value pairs
// for use with slog.Logger methods.
func (e *LogEntry) slogAttrs() []any {
	var args []any
	for k, v := range e.Fields {
		args = append(args, k, v)
	}
	if e.Err != nil {
		args = append(args, "error", e.Err.Error())
	}
	return args
}

func (e *LogEntry) dedupCheck() bool {
	e.logger.mu.RLock()
	disabled := e.logger.disabled
	dedupEnabled := e.logger.dedupEnabled
	e.logger.mu.RUnlock()

	if disabled {
		return false
	}
	if e.skipDedup || !dedupEnabled || e.logger.dedup == nil {
		return true
	}
	key := e.generateKey()
	return e.logger.dedup.shouldLog(key)
}

// writeSlog emits the log entry through the underlying slog.Logger.
func (e *LogEntry) writeSlog() {
	if !e.dedupCheck() {
		return
	}

	attrs := e.slogAttrs()
	switch e.Level {
	case "info":
		e.logger.zlog.Info(e.Message, attrs...)
	case "warn":
		e.logger.zlog.Warn(e.Message, attrs...)
	case "error":
		e.logger.zlog.Error(e.Message, attrs...)
	case "debug":
		e.logger.zlog.Debug(e.Message, attrs...)
	default:
		e.logger.zlog.Info(e.Message, attrs...)
	}
}

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

// ParseAndLogWindowsEntry parses a JSON payload and writes it using the configured logger.
func ParseAndLogWindowsEntry(body io.ReadCloser) error {
	entry, err := parseLogEntry(body)
	if err != nil {
		return err
	}
	defer putLogEntry(entry)

	entry.writeSlog()
	return nil
}
