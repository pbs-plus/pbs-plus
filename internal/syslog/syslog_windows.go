//go:build windows

package syslog

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/kardianos/service"
	"github.com/pbs-plus/pbs-plus/internal/utils"
	"github.com/rs/zerolog"
)

// SetServiceLogger configures the service logger for Windows Event Log integration.
func (l *Logger) SetServiceLogger(s service.Logger) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	zlogger := zerolog.New(zerolog.NewConsoleWriter(func(w *zerolog.ConsoleWriter) {
		w.Out = &LogWriter{logger: s}
		w.NoColor = true
		w.FormatCaller = func(i interface{}) string {
			var c string
			if cc, ok := i.(string); ok {
				c = cc
			}
			if c == "" {
				return ""
			}

			parts := strings.Split(c, "/")
			if len(parts) >= 2 {
				return fmt.Sprintf("%s/%s", parts[len(parts)-2], parts[len(parts)-1])
			}
			return filepath.Base(c)
		}
	})).With().
		CallerWithSkipFrameCount(3).
		Timestamp().
		Logger()

	l.zlog = &zlogger
	l.hostname, _ = utils.GetAgentHostname()

	l.zlog.Info().Msg("Service logger successfully added for Windows Event Log")
	return nil
}

// Write finalizes the LogEntry and writes it using the global zerolog logger.
// (Here, the global logger sends the pre-formatted output through the
// ConsoleWriter and then our SyslogWriter.)
func (e *LogEntry) Write() {
	e.logger.mu.RLock()
	defer e.logger.mu.RUnlock()

	if e.logger.disabled {
		return
	}

	if e.JobID != "" {
		e.Fields["jobId"] = e.JobID
	}

	if _, ok := e.Fields["hostname"]; !ok {
		e.Fields["hostname"] = e.logger.hostname
	}

	// Produce a full JSON log entry.
	switch e.Level {
	case "info":
		e.logger.zlog.Info().Fields(e.Fields).Msg(e.Message)
	case "debug":
		e.logger.zlog.Debug().Fields(e.Fields).Msg(e.Message)
	case "warn":
		e.logger.zlog.Warn().Fields(e.Fields).Msg(e.Message)
	case "error":
		e.logger.zlog.Error().Err(e.Err).Fields(e.Fields).Msg(e.Message)
	default:
		e.logger.zlog.Info().Fields(e.Fields).Msg(e.Message)
	}
}
