//go:build windows

package syslog

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/kardianos/service"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent"
)

// Logger struct for Windows Event Log integration.
type Logger struct {
	LogWriter service.Logger
	mu        sync.Mutex // Protects LogWriter
}

// LogEntry represents a single log entry with additional context.
// It now includes Hostname so that logs can be identified by host.
type LogEntry struct {
	Level    string `json:"level"`
	Message  string `json:"message"`
	Hostname string `json:"hostname,omitempty"`
	// Err is not automatically marshaled; see MarshalJSON.
	Err       error  `json:"-"`
	ErrString string `json:"error,omitempty"`
	// Fields contains extra log data.
	Fields map[string]interface{} `json:"fields,omitempty"`
	// logger is omitted from JSON.
	logger *Logger `json:"-"`
}

// Global logger instance.
var L *Logger

// Worker pool variables.
var (
	logQueue   chan LogEntry
	workerOnce sync.Once
	workerWg   sync.WaitGroup
)

func init() {
	// Initialize zerolog with output to stdout.
	log.Logger = zerolog.New(
		zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: time.RFC3339,
		},
	).With().
		CallerWithSkipFrameCount(3).
		Timestamp().
		Caller().
		Logger()

	// Initialize the global logger instance.
	L = &Logger{}

	// Initialize the worker pool.
	initializeWorkerPool()
	// Launch a goroutine that gracefully shuts down the worker pool on SIGINT/SIGTERM.
	go stopWorkerPool()
}

// SetServiceLogger configures the service logger for Windows Event Log integration.
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

// Write finalizes the log entry and dispatches it both locally and remotely.
func (e *LogEntry) Write() {
	// Ensure the hostname is set if it's not provided.
	if e.Hostname == "" {
		if host, err := os.Hostname(); err == nil {
			e.Hostname = host
		}
	}

	// Format the complete log entry as JSON (including hostname).
	formattedMsg := e.formatLogAsJSON()

	// Enqueue the log for remote processing.
	e.enqueueLog()

	e.logger.mu.Lock()
	defer e.logger.mu.Unlock()

	if e.logger.LogWriter != nil {
		switch e.Level {
		case "info":
			_ = e.logger.LogWriter.Info(formattedMsg)
		case "warn":
			_ = e.logger.LogWriter.Warning(formattedMsg)
		case "error":
			_ = e.logger.LogWriter.Error(formattedMsg)
		default:
			_ = e.logger.LogWriter.Info(formattedMsg)
		}
	} else {
		fallbackLogger := log.With().
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

// initializeWorkerPool sets up the singleton worker pool.
func initializeWorkerPool() {
	workerOnce.Do(func() {
		logQueue = make(chan LogEntry, 100) // Buffered channel for log messages.
		startWorkerPool(5)                  // Start 5 workers.
	})
}

// startWorkerPool starts the specified number of worker goroutines.
func startWorkerPool(workerCount int) {
	workerWg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go worker()
	}
}

// worker processes log messages from the logQueue.
func worker() {
	defer workerWg.Done()
	for logMsg := range logQueue {
		sendLogToServer(logMsg)
	}
}

// sendLogToServer marshals and sends the LogEntry as JSON to the remote server.
func sendLogToServer(entry LogEntry) {
	entry.ErrString = entry.Err.Error()

	reqBody, err := json.Marshal(entry)
	if err != nil {
		log.Error().Err(err).Msg("Failed to marshal log entry")
		return
	}

	body, err := agent.ProxmoxHTTPRequest(
		http.MethodPost,
		"/api2/json/d2d/agent-log",
		bytes.NewBuffer(reqBody),
		nil,
	)
	if err != nil {
		log.Error().Err(err).Msg("Failed to send log to remote server")
		return
	}
	defer body.Close()
	_, _ = io.Copy(io.Discard, body)
}

// stopWorkerPool gracefully shuts down the worker pool using a context that cancels on SIGINT or SIGTERM.
func stopWorkerPool() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	<-ctx.Done()

	close(logQueue)
	workerWg.Wait()
}

// enqueueLog adds a log message to the logQueue for processing.
func (e *LogEntry) enqueueLog() {
	select {
	case logQueue <- *e:
		// Successfully enqueued.
	default:
		log.Warn().Msg("Log queue is full, dropping log message")
	}
}

// formatLogAsJSON formats the log entry as a JSON string, ensuring
// that the hostname is included.
func (e *LogEntry) formatLogAsJSON() string {
	// Ensure a hostname is present.
	if e.Hostname == "" {
		if host, err := os.Hostname(); err == nil {
			e.Hostname = host
		}
	}

	// Merge the hostname into the fields.
	fields := make(map[string]interface{}, len(e.Fields)+1)
	for k, v := range e.Fields {
		fields[k] = v
	}
	fields["hostname"] = e.Hostname

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
