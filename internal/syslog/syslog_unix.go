//go:build unix

package syslog

import (
	"fmt"
	"log/syslog"
	"os"
	"path/filepath"
	"strings"

	"github.com/rs/zerolog"
)

func init() {
	sysWriter, _ := syslog.New(syslog.LOG_ERR|syslog.LOG_LOCAL7, "pbs-plus")
	logger := zerolog.New(zerolog.NewConsoleWriter(func(w *zerolog.ConsoleWriter) {
		w.Out = &LogWriter{logger: sysWriter}
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
	})).With().Timestamp().CallerWithSkipFrameCount(3).Logger()

	hostname, _ := os.Hostname()
	L = &Logger{zlog: &logger, hostname: hostname}
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

	if _, ok := e.Fields["hostname"]; !ok {
		e.Fields["hostname"] = e.logger.hostname
	}

	if e.JobID != "" {
		backupLogger := GetExistingBackupLogger(e.JobID)
		if backupLogger != nil {
			var sb strings.Builder

			sb.WriteString("pbs-plus: ")

			if e.Level == "error" {
				sb.WriteString("warning: [non-fatal " + e.Level + "]")
			} else {
				sb.WriteString("[" + e.Level + "]")
			}

			if e.Err != nil {
				sb.WriteString(" " + e.Err.Error())
			}

			if e.Message != "" {
				sb.WriteString(": " + e.Message)
			}

			if e.Fields != nil {
				sb.WriteString(fmt.Sprintf(" (debug values: %v)", e.Fields))
			}

			backupLogger.Write([]byte(sb.String()))

			sb.Reset()
		}
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
