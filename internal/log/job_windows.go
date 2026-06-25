//go:build windows

package log

import "io"

func (l *Logger) ensureJobLogger()           {}
func (l *Logger) closeJobLogger()            {}
func (l *Logger) JobStdoutWriter() io.Writer { return nil }
