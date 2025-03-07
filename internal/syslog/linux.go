//go:build linux

package syslog

import (
	"bytes"
	"log/syslog"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Logger struct for Linux syslog integration
type Logger struct {
	syslogWriter *syslog.Writer
	mu           sync.Mutex // Protects syslogWriter
}

// LogEntry represents a single log entry with additional context
type LogEntry struct {
	level   string
	message string
	err     error
	fields  map[string]interface{}
	logger  *Logger
}

// Global logger instance
var L *Logger

func init() {
	// Attempt to initialize the syslog writer
	syslogWriter, err := syslog.New(syslog.LOG_ERR|syslog.LOG_LOCAL7, "pbs-plus")
	if err != nil {
		// If syslog initialization fails, fallback to zerolog with stdout
		log.Logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).
			With().
			Timestamp().
			Caller(). // Automatically include caller information (file and line number)
			Logger()
		log.Warn().Err(err).Msg("Failed to initialize syslog, falling back to stdout")
		L = &Logger{syslogWriter: nil}
	} else {
		// If syslog is successfully initialized, use it
		L = &Logger{syslogWriter: syslogWriter}
	}
}

// Error starts a new log entry for an error
func (l *Logger) Error(err error) *LogEntry {
	return &LogEntry{
		level:  "error",
		err:    err,
		fields: make(map[string]interface{}),
		logger: l,
	}
}

// Warn starts a new log entry for a warning
func (l *Logger) Warn() *LogEntry {
	return &LogEntry{
		level:  "warn",
		fields: make(map[string]interface{}),
		logger: l,
	}
}

// Info starts a new log entry for informational messages
func (l *Logger) Info() *LogEntry {
	return &LogEntry{
		level:  "info",
		fields: make(map[string]interface{}),
		logger: l,
	}
}

// WithMessage adds a message to the log entry
func (e *LogEntry) WithMessage(msg string) *LogEntry {
	e.message = msg
	return e
}

// WithField adds a single key-value pair to the log entry
func (e *LogEntry) WithField(key string, value interface{}) *LogEntry {
	e.fields[key] = value
	return e
}

// WithFields adds multiple key-value pairs to the log entry
func (e *LogEntry) WithFields(fields map[string]interface{}) *LogEntry {
	for k, v := range fields {
		e.fields[k] = v
	}
	return e
}

// Send finalizes the log entry and sends it to the appropriate destination
func (e *LogEntry) Write() {
	// Format the log entry as JSON
	jsonMsg := e.formatLogAsJSON()

	// Send to syslog if available
	e.logger.mu.Lock()
	defer e.logger.mu.Unlock()

	if e.logger.syslogWriter != nil {
		switch e.level {
		case "info":
			_ = e.logger.syslogWriter.Info(jsonMsg)
		case "warn":
			_ = e.logger.syslogWriter.Warning(jsonMsg)
		case "error":
			_ = e.logger.syslogWriter.Err(jsonMsg)
		default:
			_ = e.logger.syslogWriter.Info(jsonMsg)
		}
	} else {
		// Fallback to stdout using zerolog
		event := log.With().CallerWithSkipFrameCount(3).Fields(e.fields) // Skip 3 frames to get the correct caller
		if e.err != nil {
			event = event.Err(e.err)
		}
		switch e.level {
		case "info":
			log.Info().Msg(e.message)
		case "warn":
			log.Warn().Msg(e.message)
		case "error":
			log.Error().Msg(e.message)
		default:
			log.Info().Msg(e.message)
		}
	}
}

// formatLogAsJSON formats the log entry as a JSON string
func (e *LogEntry) formatLogAsJSON() string {
	var buf bytes.Buffer

	// Create a zerolog logger that writes to the buffer
	logger := zerolog.New(&buf).With().
		Timestamp().
		Fields(e.fields).
		Logger()

	// Add the error if present
	event := logger.Log()
	if e.err != nil {
		event = event.Err(e.err)
	}

	// Add the message
	event.Msg(e.message)

	// Return the serialized JSON as a string
	return buf.String()
}
