//go:build linux

package esxi

import (
	"bytes"
	"fmt"
	"io"
	"time"
)

// NewLogger creates a new logger instance
func NewLogger(level string, output io.Writer) *Logger {
	return &Logger{
		level:    level,
		output:   output,
		emailLog: &bytes.Buffer{},
	}
}

// Log writes a log message with the specified level
func (l *Logger) Log(level, message string) {
	if l.shouldLog(level) {
		timestamp := time.Now().Format("2006-01-02 15:04:05")
		logLine := fmt.Sprintf("%s -- %s: %s\n", timestamp, level, message)

		if l.output != nil {
			l.output.Write([]byte(logLine))
		}

		if l.logFile != nil {
			l.logFile.WriteString(logLine)
		}

		if l.emailLog != nil {
			l.emailLog.WriteString(logLine)
		}
	}
}

func (l *Logger) shouldLog(level string) bool {
	switch l.level {
	case "debug":
		return true
	case "info":
		return level == "info" || level == "dryrun"
	case "dryrun":
		return level == "dryrun" || level == "info"
	default:
		return level == "info"
	}
}

// Info logs an info message
func (l *Logger) Info(message string) {
	l.Log("info", message)
}

// Debug logs a debug message
func (l *Logger) Debug(message string) {
	l.Log("debug", message)
}

// DryRun logs a dry run message
func (l *Logger) DryRun(message string) {
	l.Log("dryrun", message)
}
