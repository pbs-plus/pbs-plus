package syslog

import (
	"encoding/json"
	"errors"
	"io"
)

// Global logger instance.
var L *Logger

// Error creates a new error-level LogEntry.
func (l *Logger) Error(err error) *LogEntry {
	return &LogEntry{
		Level:  "error",
		Err:    err,
		Fields: make(map[string]interface{}),
		logger: l,
	}
}

// Warn creates a new warning-level LogEntry.
func (l *Logger) Warn() *LogEntry {
	return &LogEntry{
		Level:  "warn",
		Fields: make(map[string]interface{}),
		logger: l,
	}
}

// Info creates a new info-level LogEntry.
func (l *Logger) Info() *LogEntry {
	return &LogEntry{
		Level:  "info",
		Fields: make(map[string]interface{}),
		logger: l,
	}
}

// WithMessage sets the log message.
func (e *LogEntry) WithMessage(msg string) *LogEntry {
	e.Message = msg
	return e
}

// WithJSON attempts to unmarshal the input JSON and merge the fields.
func (e *LogEntry) WithJSON(msg string) *LogEntry {
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(msg), &parsed); err == nil {
		for k, v := range parsed {
			e.Fields[k] = v
		}
	} else {
		e.Message = msg
	}
	return e
}

// WithField adds one key-value pair to the LogEntry.
func (e *LogEntry) WithField(key string, value interface{}) *LogEntry {
	e.Fields[key] = value
	return e
}

// WithFields adds multiple key-value pairs to the LogEntry.
func (e *LogEntry) WithFields(fields map[string]interface{}) *LogEntry {
	for k, v := range fields {
		e.Fields[k] = v
	}
	return e
}

// parseLogEntry parses a JSON payload (e.g. sent from a Linux system)
// into a LogEntry.
func parseLogEntry(body io.ReadCloser) (*LogEntry, error) {
	var entry LogEntry
	if err := json.NewDecoder(body).Decode(&entry); err != nil {
		return nil, err
	}
	entry.logger = L
	if entry.ErrString != "" {
		entry.Err = errors.New(entry.ErrString)
	}
	return &entry, nil
}

// ParseAndLogWindowsEntry parses a JSON payload using ParseLogEntry
// and then writes it using the Windows logger.
func ParseAndLogWindowsEntry(body io.ReadCloser) error {
	entry, err := parseLogEntry(body)
	if err != nil {
		return err
	}

	entry.logger.mu.RLock()
	defer entry.logger.mu.RUnlock()

	switch entry.Level {
	case "info":
		entry.logger.zlog.Info().Msg(entry.Message)
	case "warn":
		entry.logger.zlog.Warn().Msg(entry.Message)
	case "error":
		entry.logger.zlog.Error().Msg(entry.Message)
	default:
		entry.logger.zlog.Info().Msg(entry.Message)
	}
	return nil
}
