//go:build linux

package syslog

import (
	"log/syslog"

	"github.com/rs/zerolog"
)

func init() {
	sysWriter, _ := syslog.New(syslog.LOG_ERR|syslog.LOG_LOCAL7, "pbs-plus")
	logger := zerolog.New(zerolog.NewConsoleWriter(func(w *zerolog.ConsoleWriter) {
		w.Out = &LogWriter{logger: sysWriter}
		w.NoColor = true
	})).With().Timestamp().CallerWithSkipFrameCount(4).Logger()

	L = &Logger{zlog: &logger}
}

// Write finalizes the LogEntry and writes it using the global zerolog logger.
// (Here, the global logger sends the pre-formatted output through the
// ConsoleWriter and then our SyslogWriter.)
func (e *LogEntry) Write() {
	e.logger.mu.RLock()
	defer e.logger.mu.RUnlock()

	// Produce a full JSON log entry.
	switch e.Level {
	case "info":
		e.logger.zlog.Info().Msg(e.Message)
	case "warn":
		e.logger.zlog.Warn().Msg(e.Message)
	case "error":
		e.logger.zlog.Error().Msg(e.Message)
	default:
		e.logger.zlog.Info().Msg(e.Message)
	}
}
