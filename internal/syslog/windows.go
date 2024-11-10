//go:build windows

package syslog

import (
	"fmt"
	"log"

	"github.com/kardianos/service"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

type Logger struct {
	LogWriter service.Logger
}

func InitializeLogger(s service.Service) (*Logger, error) {
	logger, err := s.Logger(nil)
	if err != nil {
		return nil, fmt.Errorf("InitializeLogger: %w", err)
	}

	return &Logger{
		LogWriter: logger,
	}, nil
}

func (l *Logger) Infof(format string, v ...any) {
	log.Printf(format, v...)
	_ = l.LogWriter.Info(fmt.Sprintf(format, v...))
}

func (l *Logger) Info(v ...any) {
	log.Print(v...)
	_ = l.LogWriter.Info(fmt.Sprint(v...))
}

func (l *Logger) Errorf(format string, v ...any) {
	log.Printf(format, v...)
	_ = l.LogWriter.Error(fmt.Sprintf(format, v...))
	if service.Interactive() {
		utils.ShowMessageBox("Error", fmt.Sprintf(format, v...))
	}
}

func (l *Logger) Error(v ...any) {
	log.Print(v...)
	_ = l.LogWriter.Error(fmt.Sprint(v...))
	if service.Interactive() {
		utils.ShowMessageBox("Error", fmt.Sprint(v...))
	}
}

func (l *Logger) Warnf(format string, v ...any) {
	log.Printf(format, v...)
	_ = l.LogWriter.Warning(fmt.Sprintf(format, v...))
}

func (l *Logger) Warn(v ...any) {
	log.Print(v...)
	_ = l.LogWriter.Warning(fmt.Sprint(v...))
}
