//go:build windows

package syslog

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/utils"
	"github.com/rs/zerolog"
	"golang.org/x/sys/windows/svc/eventlog"
	"gopkg.in/natefinch/lumberjack.v2"
)

func (l *Logger) SetServiceLogger() error {
	sourceName := "PBSPlusAgentLogs"

	l.mu.Lock()
	defer l.mu.Unlock()

	_ = eventlog.InstallAsEventCreate(
		sourceName,
		eventlog.Info|eventlog.Warning|eventlog.Error,
	)

	evl, err := eventlog.Open(sourceName)
	if err == nil {
		zlogger := zerolog.New(zerolog.NewConsoleWriter(func(w *zerolog.ConsoleWriter) {
			w.Out = &LogWriter{logger: evl}
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
		l.hostname, _ = utils.GetAgentHostname()
		l.zlog.Info().Msg("Service logger successfully added for Windows Event Log")
		return nil
	}

	logPath := `C:\ProgramData\PBS Plus Agent\agent.log`
	_ = os.MkdirAll(filepath.Dir(logPath), os.ModeDir)

	rotator := &lumberjack.Logger{
		Filename:   logPath,
		MaxSize:    25, // MB
		MaxBackups: 5,
		MaxAge:     30,   // days
		Compress:   true, // gzip
	}

	zlogger := zerolog.New(zerolog.NewConsoleWriter(func(w *zerolog.ConsoleWriter) {
		w.Out = rotator
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
	l.hostname, _ = utils.GetAgentHostname()

	l.zlog.Warn().Err(err).Msg("Windows Event Log unavailable; falling back to file logging")
	return nil
}

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
