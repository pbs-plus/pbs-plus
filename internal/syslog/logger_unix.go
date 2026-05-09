//go:build unix

package syslog

import (
	"fmt"
	"log/syslog"
	"os"
	"strings"

	"log/slog"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/conf"
)

func (l *Logger) SetServiceLogger() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.hostname, _ = types.GetAgentHostname()

	tag := "pbs-plus-agent"
	if conf.IsServer {
		tag = "pbs-plus"
	}

	sysWriter, err := syslog.New(syslog.LOG_ERR|syslog.LOG_LOCAL7, tag)
	if err != nil {
		return err
	}

	// Build a slog.Logger that writes JSON to the syslog writer.
	handler := slog.NewJSONHandler(&LogWriter{logger: sysWriter}, &slog.HandlerOptions{
		Level:     slog.LevelInfo,
		AddSource: true,
	})
	if conf.Env.Debug {
		handler = slog.NewJSONHandler(&LogWriter{logger: sysWriter}, &slog.HandlerOptions{
			Level:     slog.LevelDebug,
			AddSource: true,
		})
	}

	zlogger := slog.New(handler)
	l.zlog = zlogger
	l.zlog.Info("service logger successfully added for syslog")
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
		e.writeSlog()
	}
}

func (e *LogEntry) serverWrite() {
	if e.Level == "debug" && os.Getenv("DEBUG") != "true" {
		return
	}

	if e.JobID != "" {
		backupLogger := GetExistingJobLogger(e.JobID)
		if backupLogger != nil {
			var sb strings.Builder

			sb.WriteString("pbs-plus: ")

			if e.Level == "error" {
				sb.WriteString("WARNING: [non-fatal " + e.Level + "]")
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

	e.writeSlog()
}
