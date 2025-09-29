//go:build windows

package syslog

import (
	"github.com/kardianos/service"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func init() {
	// Configure zerolog to output via our EventLogWriter wrapped in a ConsoleWriter.
	zlogger := zerolog.New(zerolog.NewConsoleWriter()).With().
		CallerWithSkipFrameCount(3).
		Timestamp().
		Logger()

	L = &Logger{zlog: &zlogger}
}

// SetServiceLogger configures the service logger for Windows Event Log integration.
func (l *Logger) SetServiceLogger(s service.Logger) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	zlogger := zerolog.New(zerolog.NewConsoleWriter(func(w *zerolog.ConsoleWriter) {
		w.Out = &LogWriter{logger: s}
		w.NoColor = true
	})).With().
		CallerWithSkipFrameCount(3).
		Timestamp().
		Logger()

	l.zlog = &zlogger

	l.zlog.Info().Msg("Service logger successfully added for Windows Event Log")
	return nil
}

// Write finalizes the LogEntry and writes it using the global zerolog logger.
// (Here, the global logger sends the pre-formatted output through the
// ConsoleWriter and then our SyslogWriter.)
func (e *LogEntry) Write() {
	e.logger.mu.RLock()
	defer e.logger.mu.RUnlock()

	e.enqueueLog()

	if e.JobID != "" {
		e.Fields["jobId"] = e.JobID
	}

	// Produce a full JSON log entry.
	switch e.Level {
	case "info":
		e.logger.zlog.Info().Fields(e.Fields).Msg(e.Message)
	case "warn":
		e.logger.zlog.Warn().Fields(e.Fields).Msg(e.Message)
	case "error":
		e.logger.zlog.Error().Err(e.Err).Fields(e.Fields).Msg(e.Message)
	default:
		e.logger.zlog.Info().Fields(e.Fields).Msg(e.Message)
	}
}

// enqueueLog adds a log message to the logQueue for processing.
func (e *LogEntry) enqueueLog() {
	select {
	case logQueue <- *e:
		// Log enqueued successfully.
	default:
		log.Warn().Msg("Log queue is full, dropping log message")
	}
}
