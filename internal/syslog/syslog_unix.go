//go:build unix

package syslog

import (
	"fmt"
	"log/syslog"
	"path/filepath"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/utils"
	"github.com/rs/zerolog"
)

func (l *Logger) SetServiceLogger() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.hostname, _ = utils.GetAgentHostname()

	sysWriter, err := syslog.New(syslog.LOG_ERR|syslog.LOG_LOCAL7, "pbs-plus")
	if err != nil {
		return err
	}

	zlogger := zerolog.New(zerolog.NewConsoleWriter(func(w *zerolog.ConsoleWriter) {
		w.Out = &LogWriter{logger: sysWriter}
		w.NoColor = true
		w.FormatCaller = func(i any) string {
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

	l.zlog.Info().Msg("Service logger successfully added for syslog")
	return nil
}

func (e *LogEntry) Write() {
	e.logger.mu.RLock()
	defer e.logger.mu.RUnlock()

	if e.logger.disabled {
		return
	}

	if _, ok := e.Fields["hostname"]; !ok {
		e.Fields["hostname"] = e.logger.hostname
	}

	if e.logger.Server {
		e.serverWrite()
	} else {
		if e.JobID != "" {
			e.Fields["jobId"] = e.JobID
		}
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

func (e *LogEntry) serverWrite() {
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
}
