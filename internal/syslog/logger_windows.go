//go:build windows

package syslog

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"log/slog"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"golang.org/x/sys/windows/svc/eventlog"
	"gopkg.in/natefinch/lumberjack.v2"
)

func (l *Logger) SetServiceLogger() error {
	sourceName := "PBSPlusAgent"

	l.mu.Lock()
	defer l.mu.Unlock()

	if os.Getenv("PBS_PLUS_STDOUT_ONLY") == "true" {
		return nil
	}

	_ = eventlog.InstallAsEventCreate(
		sourceName,
		eventlog.Info|eventlog.Warning|eventlog.Error,
	)

	evl, err := eventlog.Open(sourceName)
	if err == nil {
		handler := slog.NewJSONHandler(&LogWriter{logger: evl}, &slog.HandlerOptions{
			Level:     slog.LevelInfo,
			AddSource: true,
		})
		zlogger := slog.New(handler)
		l.zlog = zlogger
		l.hostname, _ = types.GetAgentHostname()
		l.zlog.Info("service logger successfully added for Windows Event Log")
		return nil
	}

	logPath := `C:\ProgramData\PBS Plus Agent\agent.log`
	_ = os.MkdirAll(filepath.Dir(logPath), os.ModeDir)

	rotator := &lumberjack.Logger{
		Filename:   logPath,
		MaxSize:    25,
		MaxBackups: 5,
		MaxAge:     30,
		Compress:   true,
	}

	handler := slog.NewJSONHandler(rotator, &slog.HandlerOptions{
		Level:     slog.LevelInfo,
		AddSource: true,
	})
	zlogger := slog.New(handler)
	l.zlog = zlogger
	l.hostname, _ = types.GetAgentHostname()
	l.zlog.Warn(fmt.Sprintf("windows event log unavailable; falling back to file logging: %v", err))
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

	e.writeSlog()
}
