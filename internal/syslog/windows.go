//go:build windows

package syslog

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/kardianos/service"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Logger struct for Windows Event Log integration
type Logger struct {
	LogWriter service.Logger
	mu        sync.Mutex // Protects LogWriter
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
	// Initialize the logger with zerolog output to stdout by default
	log.Logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).
		With().
		Timestamp().
		Caller(). // Automatically include caller information (file and line number)
		Logger()

	// Initialize the global logger instance
	L = &Logger{}
}

// SetServiceLogger sets the service logger for Windows Event Log integration
func (l *Logger) SetServiceLogger(s service.Service) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	logger, err := s.Logger(nil)
	if err != nil {
		return fmt.Errorf("failed to set service logger: %w", err)
	}

	l.LogWriter = logger
	log.Info().Msg("Service logger successfully added for Windows Event Log")
	return nil
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

	// Send to Windows Event Log if available
	e.logger.mu.Lock()
	defer e.logger.mu.Unlock()

	if e.logger.LogWriter != nil {
		switch e.level {
		case "info":
			_ = e.logger.LogWriter.Info(jsonMsg)
		case "warn":
			_ = e.logger.LogWriter.Warning(jsonMsg)
		case "error":
			_ = e.logger.LogWriter.Error(jsonMsg)
		default:
			_ = e.logger.LogWriter.Info(jsonMsg)
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
