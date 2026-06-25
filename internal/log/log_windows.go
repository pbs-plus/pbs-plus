//go:build windows

package log

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"log/slog"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/host"

	"golang.org/x/sys/windows/svc/eventlog"
	"gopkg.in/natefinch/lumberjack.v2"
)

type LogWriter struct {
	logger *eventlog.Log
}

func (ew *LogWriter) Write(p []byte) (n int, err error) {
	message := string(p)
	if ew.logger != nil {
		if strings.Contains(message, "ERR") {
			err = ew.logger.Error(2, message)
		} else {
			err = ew.logger.Info(1, message)
		}
	} else {
		return os.Stdout.Write(p)
	}
	return len(p), err
}

func setServiceLogger(l *Logger) error {
	sourceName := "PBSPlusAgent"

	l.mu.Lock()
	defer l.mu.Unlock()

	if conf.Env.StdoutOnly {
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
			AddSource: false,
		})
		zlogger := slog.New(handler)
		l.zlog = zlogger
		l.hostname, _ = host.AgentHostname()
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
		AddSource: false,
	})
	zlogger := slog.New(handler)
	l.zlog = zlogger
	l.hostname, _ = host.AgentHostname()
	l.zlog.Warn(fmt.Sprintf("windows event log unavailable; falling back to file logging: %v", err))
	return nil
}

func (l *Logger) writePlatform(e *entry, attrs []any) {
	if e.jobID != "" {
		e.fields["jobID"] = e.jobID
	}

	if _, ok := e.fields["hostname"]; !ok {
		e.fields["hostname"] = l.hostname
	}

	l.zlog.Log(ctxNone, levelFromEntry(e), e.message, attrs...)
}
