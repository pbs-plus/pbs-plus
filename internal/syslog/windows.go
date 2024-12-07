//go:build windows

package syslog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/kardianos/service"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

type Logger struct {
	LogWriter service.Logger
}

type LogRemoteRequest struct {
	Hostname string `json:"hostname"`
	Message  string `json:"message"`
	Level    string `json:"level"`
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

func (l *Logger) logToServer(level string, msg string) {
	hostname, _ := os.Hostname()
	reqBody, err := json.Marshal(&LogRemoteRequest{
		Hostname: hostname,
		Message:  msg,
		Level:    level,
	})
	if err != nil {
		return
	}

	_ = agent.ProxmoxHTTPRequest(
		http.MethodPost,
		"/api2/json/d2d/agent-log",
		bytes.NewBuffer(reqBody),
		nil,
	)
}

func (l *Logger) Infof(format string, v ...any) {
	log.Printf(format, v...)
	_ = l.LogWriter.Info(fmt.Sprintf(format, v...))
	l.logToServer("info", fmt.Sprintf(format, v...))
}

func (l *Logger) Info(v ...any) {
	log.Print(v...)
	_ = l.LogWriter.Info(fmt.Sprint(v...))
	l.logToServer("info", fmt.Sprint(v...))
}

func (l *Logger) Errorf(format string, v ...any) {
	log.Printf(format, v...)
	_ = l.LogWriter.Error(fmt.Sprintf(format, v...))
	if service.Interactive() {
		utils.ShowMessageBox("Error", fmt.Sprintf(format, v...))
	}
	l.logToServer("error", fmt.Sprintf(format, v...))
}

func (l *Logger) Error(v ...any) {
	log.Print(v...)
	_ = l.LogWriter.Error(fmt.Sprint(v...))
	if service.Interactive() {
		utils.ShowMessageBox("Error", fmt.Sprint(v...))
	}
	l.logToServer("error", fmt.Sprint(v...))
}

func (l *Logger) Warnf(format string, v ...any) {
	log.Printf(format, v...)
	_ = l.LogWriter.Warning(fmt.Sprintf(format, v...))
	l.logToServer("warn", fmt.Sprintf(format, v...))
}

func (l *Logger) Warn(v ...any) {
	log.Print(v...)
	_ = l.LogWriter.Warning(fmt.Sprint(v...))
	l.logToServer("warn", fmt.Sprint(v...))
}
