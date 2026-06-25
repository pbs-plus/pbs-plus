//go:build windows

package log

import (
	"io"
	"time"
)

func (l *Logger) ensureJobLogger()           {}
func (l *Logger) closeJobLogger()            {}
func (l *Logger) JobStdoutWriter() io.Writer { return nil }
func (l *Logger) JobLogPath() string         { return "" }
func (l *Logger) JobStartTime() time.Time    { return time.Now() }
func (l *Logger) FlushJobLog() error         { return nil }
