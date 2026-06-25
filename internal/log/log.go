package log

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"maps"
	"os"
	"sync"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/conf"
)

var L *Logger

type entry struct {
	level   string
	message string
	err     error
	jobID   string
	fields  map[string]any
	dedup   bool
	logger  *Logger
}

type Logger struct {
	mu           sync.RWMutex
	zlog         *slog.Logger
	hostname     string
	Server       bool
	disabled     bool
	dedup        *deduplicator
	dedupEnabled bool
}

type deduplicator struct {
	mu      sync.RWMutex
	entries map[[32]byte]time.Time
	window  time.Duration
}

func newDeduplicator(window time.Duration) *deduplicator {
	d := &deduplicator{
		entries: make(map[[32]byte]time.Time),
		window:  window,
	}
	go d.cleanup()
	return d
}

func (d *deduplicator) cleanup() {
	ticker := time.NewTicker(d.window)
	defer ticker.Stop()
	for range ticker.C {
		d.mu.Lock()
		now := time.Now()
		for key, t := range d.entries {
			if now.Sub(t) > d.window {
				delete(d.entries, key)
			}
		}
		d.mu.Unlock()
	}
}

func (d *deduplicator) shouldLog(key [32]byte) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	if t, ok := d.entries[key]; ok && now.Sub(t) < d.window {
		return false
	}
	d.entries[key] = now
	return true
}

func init() {
	level := slog.LevelInfo
	if conf.Env.Debug {
		level = slog.LevelDebug
	}

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level:     level,
		AddSource: false,
	})

	dedupWindow := 5 * time.Second
	if conf.Env.LogDedupWindow != "" {
		if d, err := time.ParseDuration(conf.Env.LogDedupWindow); err == nil {
			dedupWindow = d
		}
	}

	L = &Logger{
		zlog:         slog.New(handler),
		dedup:        newDeduplicator(dedupWindow),
		dedupEnabled: true,
	}
}

func (l *Logger) SetServiceLogger() error { return setServiceLogger(l) }

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

func parseFields(args ...any) map[string]any {
	m := make(map[string]any, len(args)/2)
	for i := 0; i+1 < len(args); i += 2 {
		key, ok := args[i].(string)
		if !ok {
			continue
		}
		m[key] = args[i+1]
	}
	return m
}

func (l *Logger) newEntry(level string, err error, msg string, args ...any) *entry {
	e := &entry{
		level:   level,
		message: msg,
		err:     err,
		fields:  parseFields(args...),
		logger:  l,
		dedup:   true,
	}
	if jobID, ok := e.fields["job"].(string); ok {
		delete(e.fields, "job")
		e.jobID = jobID
	}
	return e
}

func (e *entry) write() {
	l := e.logger
	l.mu.RLock()
	disabled := l.disabled
	dedupEnabled := l.dedupEnabled
	l.mu.RUnlock()

	if disabled {
		return
	}

	if e.dedup && dedupEnabled && l.dedup != nil {
		key := e.dedupKey()
		if !l.dedup.shouldLog(key) {
			return
		}
	}

	if _, ok := e.fields["hostname"]; !ok {
		e.fields["hostname"] = l.hostname
	}
	if e.jobID != "" {
		e.fields["jobID"] = e.jobID
	}

	attrs := make([]any, 0, len(e.fields)*2+2)
	for k, v := range e.fields {
		attrs = append(attrs, k, v)
	}
	if e.err != nil {
		attrs = append(attrs, "error", e.err.Error())
	}

	l.writePlatform(e, attrs)
}

func (e *entry) dedupKey() [32]byte {
	h := sha256.New()
	h.Write([]byte(e.level))
	h.Write([]byte("|"))
	h.Write([]byte(e.message))
	h.Write([]byte("|"))
	h.Write([]byte(e.jobID))
	h.Write([]byte("|"))
	if e.err != nil {
		h.Write([]byte(e.err.Error()))
	}
	h.Write([]byte("|"))
	if len(e.fields) > 0 {
		fieldsJSON, err := json.Marshal(e.fields)
		if err == nil {
			h.Write(fieldsJSON)
		}
	}
	return [32]byte(h.Sum(nil))
}

func (e *entry) SkipDedup() *entry {
	e.dedup = false
	return e
}

func (e *entry) WithJob(jobID string) *entry {
	e.jobID = jobID
	return e
}

func (e *entry) WithField(key string, value any) *entry {
	e.fields[key] = value
	return e
}

func (e *entry) WithFields(fields map[string]any) *entry {
	maps.Copy(e.fields, fields)
	return e
}

func (e *entry) WithMessage(msg string) *entry {
	e.message = msg
	return e
}

func (e *entry) WithJSON(msg string) *entry {
	var parsed map[string]any
	if err := json.Unmarshal([]byte(msg), &parsed); err == nil {
		maps.Copy(e.fields, parsed)
	} else {
		e.message = msg
	}
	return e
}

func ParseAndLogWindowsEntry(body io.ReadCloser) error {
	type jsonEntry struct {
		Level     string         `json:"level"`
		Message   string         `json:"message"`
		Hostname  string         `json:"hostname,omitempty"`
		JobID     string         `json:"job_id,omitempty"`
		ErrString string         `json:"error,omitempty"`
		Fields    map[string]any `json:"fields,omitempty"`
	}

	e := &jsonEntry{}
	if err := json.NewDecoder(body).Decode(e); err != nil {
		return err
	}

	var err error
	if e.ErrString != "" {
		err = errors.New(e.ErrString)
	}

	L.newEntry(e.Level, err, e.Message).WithFields(e.Fields).WithJob(e.JobID).write()
	return nil
}

func levelFromEntry(e *entry) slog.Level {
	switch e.level {
	case "error":
		return slog.LevelError
	case "warn":
		return slog.LevelWarn
	case "debug":
		return slog.LevelDebug
	default:
		return slog.LevelInfo
	}
}

var ctxNone = context.Background()

func Info(msg string, args ...any) {
	L.newEntry("info", nil, msg, args...).write()
}

func Error(err error, msg string, args ...any) {
	L.newEntry("error", err, msg, args...).write()
}

func Warn(msg string, args ...any) {
	L.newEntry("warn", nil, msg, args...).write()
}

func Debug(msg string, args ...any) {
	L.newEntry("debug", nil, msg, args...).write()
}

func WithJob(jobID string) *entry {
	return L.newEntry("", nil, "").WithJob(jobID)
}
