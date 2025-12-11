//go:build windows

package syslog

import (
	"os"
	"strings"

	"golang.org/x/sys/windows/svc/eventlog"
)

// EventLogWriter is a custom io.Writer that sends formatted log
// output to the Windows Event Log.
type LogWriter struct {
	logger *eventlog.Log
}

// Write implements io.Writer. It converts the provided bytes into a string
// and sends it via LogWriter.
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
