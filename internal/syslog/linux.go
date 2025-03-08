//go:build linux

package syslog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/syslog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
)

// Logger struct for Linux syslog integration.
type Logger struct {
	syslogWriter *syslog.Writer
	mu           sync.Mutex // Protects syslogWriter
}

// LogEntry represents a single log entry with additional context.
// It now includes Hostname to match the Windows log format.
type LogEntry struct {
	Level    string `json:"level"`
	Message  string `json:"message"`
	Hostname string `json:"hostname,omitempty"`
	// Err is omitted on JSON unmarshaling (it can be added as needed).
	Err       error                  `json:"-"`
	ErrString string                 `json:"error,omitempty"`
	Fields    map[string]interface{} `json:"fields,omitempty"`
	// logger is omitted from JSON.
	logger *Logger `json:"-"`
}

// Global logger instance.
var L *Logger

func init() {
	// Attempt to initialize the syslog writer.
	syslogWriter, err := syslog.New(syslog.LOG_ERR|syslog.LOG_LOCAL7, "pbs-plus")
	if err != nil {
		// If syslog initialization fails, fallback to zerolog with stdout.
		zlog.Logger = zerolog.New(zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: time.RFC3339,
		}).
			With().
			Timestamp().
			CallerWithSkipFrameCount(3).
			Logger()
		zlog.Warn().Err(err).Msg("Failed to initialize syslog, falling back to stdout")
		L = &Logger{syslogWriter: nil}
	} else {
		L = &Logger{syslogWriter: syslogWriter}
	}

	// Launch a goroutine to close the syslog writer on shutdown.
	go func() {
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		<-ctx.Done()
		stop()
		if L != nil {
			if err := L.Close(); err != nil {
				zlog.Error().Err(err).Msg("Failed to close syslog writer")
			}
		}
	}()
}

// Close gracefully closes the syslog writer if available.
func (l *Logger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.syslogWriter != nil {
		return l.syslogWriter.Close()
	}
	return nil
}

// Error starts a new log entry for an error.
func (l *Logger) Error(err error) *LogEntry {
	return &LogEntry{
		Level:  "error",
		Err:    err,
		Fields: make(map[string]interface{}),
		logger: l,
	}
}

// Warn starts a new log entry for a warning.
func (l *Logger) Warn() *LogEntry {
	return &LogEntry{
		Level:  "warn",
		Fields: make(map[string]interface{}),
		logger: l,
	}
}

// Info starts a new log entry for informational messages.
func (l *Logger) Info() *LogEntry {
	return &LogEntry{
		Level:  "info",
		Fields: make(map[string]interface{}),
		logger: l,
	}
}

// WithMessage adds a message to the log entry.
func (e *LogEntry) WithMessage(msg string) *LogEntry {
	e.Message = msg
	return e
}

// WithJSON attempts to unmarshal the provided string into JSON fields.
// If successful, it merges the parsed fields; otherwise, it sets the raw message.
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

// WithField adds a single key-value pair to the log entry.
func (e *LogEntry) WithField(key string, value interface{}) *LogEntry {
	e.Fields[key] = value
	return e
}

// WithFields adds multiple key-value pairs to the log entry.
func (e *LogEntry) WithFields(fields map[string]interface{}) *LogEntry {
	for k, v := range fields {
		e.Fields[k] = v
	}
	return e
}

// Write finalizes the log entry and sends it to syslog (or falls back to stdout).
func (e *LogEntry) Write() {
	// Preformat the entire log entry as JSON (with timestamp, fields, and error, etc.)
	formattedMsg := e.formatLogAsJSON()

	e.logger.mu.Lock()
	defer e.logger.mu.Unlock()

	if e.logger.syslogWriter != nil {
		switch e.Level {
		case "info":
			_ = e.logger.syslogWriter.Info(formattedMsg)
		case "warn":
			_ = e.logger.syslogWriter.Warning(formattedMsg)
		case "error":
			_ = e.logger.syslogWriter.Err(formattedMsg)
		default:
			_ = e.logger.syslogWriter.Info(formattedMsg)
		}
	} else {
		fallbackLogger := zlog.With().
			Timestamp().
			CallerWithSkipFrameCount(3).
			Fields(e.Fields).
			Logger()

		switch e.Level {
		case "info":
			fallbackLogger.Info().Err(e.Err).Msg(e.Message)
		case "warn":
			fallbackLogger.Warn().Err(e.Err).Msg(e.Message)
		case "error":
			fallbackLogger.Error().Err(e.Err).Msg(e.Message)
		default:
			fallbackLogger.Info().Err(e.Err).Msg(e.Message)
		}
	}
}

// formatLogAsJSON formats the log entry as a JSON string.
func (e *LogEntry) formatLogAsJSON() string {
	var buf bytes.Buffer

	logger := zerolog.New(&buf).With().
		Timestamp().
		CallerWithSkipFrameCount(3).
		Fields(e.Fields).
		Logger()

	event := logger.Log()
	if e.Err != nil {
		event = event.Err(e.Err)
	}
	event.Msg(e.Message)
	return buf.String()
}

// ParseLogEntry parses a JSON payload (sent from Windows) into a LogEntry.
func parseLogEntry(body io.ReadCloser) (*LogEntry, error) {
	var entry LogEntry
	err := json.NewDecoder(body).Decode(&entry)
	if err != nil {
		return nil, err
	}
	entry.Err = errors.New(entry.ErrString)
	return &entry, nil
}

// ParseAndLogWindowsEntry parses a Windows LogEntry JSON payload and logs it
// using the Linux logger (syslog if available or fallback).
func ParseAndLogWindowsEntry(body io.ReadCloser) error {
	entry, err := parseLogEntry(body)
	if err != nil {
		return err
	}
	L.mu.Lock()
	defer L.mu.Unlock()

	if L.syslogWriter != nil {
		switch entry.Level {
		case "info":
			_ = L.syslogWriter.Info(entry.Message)
		case "warn":
			_ = L.syslogWriter.Warning(entry.Message)
		case "error":
			_ = L.syslogWriter.Err(entry.Message)
		default:
			_ = L.syslogWriter.Info(entry.Message)
		}
	} else {
		fallbackLogger := zlog.With().
			CallerWithSkipFrameCount(3).
			Fields(entry.Fields).
			Logger()

		switch entry.Level {
		case "info":
			fallbackLogger.Info().Msg(entry.Message)
		case "warn":
			fallbackLogger.Warn().Msg(entry.Message)
		case "error":
			fallbackLogger.Error().Msg(entry.Message)
		default:
			fallbackLogger.Info().Msg(entry.Message)
		}
	}
	return nil
}
