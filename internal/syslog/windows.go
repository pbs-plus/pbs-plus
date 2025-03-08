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
type LogEntry struct {
	level   string
	message string
	err     error
	fields  map[string]interface{}
	logger  *Logger
}

type LogRemoteRequest struct {
	Hostname string `json:"hostname"`
	Message  string `json:"message"`
	Level    string `json:"level"`
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
	// Start a goroutine that automatically shuts down the worker pool when a SIGINT or SIGTERM is received.
	go stopWorkerPool()
}

// SetServiceLogger sets the service logger for Windows Event Log integration.
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
		level:  "error",
		err:    err,
		fields: make(map[string]interface{}),
		logger: l,
	}
}

// Warn starts a new log entry for a warning.
func (l *Logger) Warn() *LogEntry {
	return &LogEntry{
		level:  "warn",
		fields: make(map[string]interface{}),
		logger: l,
	}
}

// Info starts a new log entry for informational messages.
func (l *Logger) Info() *LogEntry {
	return &LogEntry{
		level:  "info",
		fields: make(map[string]interface{}),
		logger: l,
	}
}

// WithMessage adds a message to the log entry.
func (e *LogEntry) WithMessage(msg string) *LogEntry {
	e.message = msg
	return e
}

// WithJSON attempts to unmarshal the provided string into JSON fields.
// If successful, it merges the parsed fields; otherwise, it sets the raw message.
func (e *LogEntry) WithJSON(msg string) *LogEntry {
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(msg), &parsed); err == nil {
		for k, v := range parsed {
			e.fields[k] = v
		}
	} else {
		e.message = msg
	}
	return e
}

// WithField adds a single key-value pair to the log entry.
func (e *LogEntry) WithField(key string, value interface{}) *LogEntry {
	e.fields[key] = value
	return e
}

// WithFields adds multiple key-value pairs to the log entry.
func (e *LogEntry) WithFields(fields map[string]interface{}) *LogEntry {
	for k, v := range fields {
		e.fields[k] = v
	}
	return e
}

// Write finalizes the log entry and sends it to the appropriate destination.
func (e *LogEntry) Write() {
	// Format the log entry as JSON.
	jsonMsg := e.formatLogAsJSON()

	// Enqueue the log for remote processing.
	e.enqueueLog()

	// Send to Windows Event Log if available.
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
		// Fallback to stdout using zerolog.
		fallbackLogger := log.With().
			CallerWithSkipFrameCount(3).
			Fields(e.fields).
			Logger()

		switch e.level {
		case "info":
			fallbackLogger.Info().Err(e.err).Msg(e.message)
		case "warn":
			fallbackLogger.Warn().Err(e.err).Msg(e.message)
		case "error":
			fallbackLogger.Error().Err(e.err).Msg(e.message)
		default:
			fallbackLogger.Info().Err(e.err).Msg(e.message)
		}
	}
}

// formatLogAsJSON formats the log entry as a JSON string.
func (e *LogEntry) formatLogAsJSON() string {
	var buf bytes.Buffer

	logger := zerolog.New(&buf).With().
		Timestamp().
		Fields(e.fields).
		Logger()

	event := logger.Log()
	if e.err != nil {
		event = event.Err(e.err)
	}

	event.Msg(e.message)
	return buf.String()
}

// initializeWorkerPool sets up the worker pool for sending logs to the server.
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

// sendLogToServer sends a log message to the remote server.
func sendLogToServer(entry LogEntry) {
	hostname, _ := os.Hostname()
	reqBody, err := json.Marshal(&LogRemoteRequest{
		Hostname: hostname,
		Message:  entry.formatLogAsJSON(),
		Level:    entry.level,
	})
	if err != nil {
		log.Error().Err(err).Msg("Failed to marshal log request")
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

// stopWorkerPool gracefully shuts down the worker pool by creating a context
// that gets canceled when a SIGINT or SIGTERM signal is received. Once the signal
// is captured, it closes the logQueue so that workers finish processing the remaining
// messages before exiting.
func stopWorkerPool() {
	// Create a context that is canceled upon receiving SIGINT or SIGTERM.
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	<-ctx.Done()

	// Close the log queue to signal workers that no new logs will arrive.
	close(logQueue)
	// Wait until all workers have finished processing.
	workerWg.Wait()
}

// enqueueLog adds a log message to the logQueue for processing.
func (e *LogEntry) enqueueLog() {
	select {
	case logQueue <- *e:
		// Successfully enqueued.
	default:
		// If the queue is full, drop the log and log a warning.
		log.Warn().Msg("Log queue is full, dropping log message")
	}
}
