//go:build unix

package syslog

import (
	"log/syslog"
	"os"
	"strings"
)

// SyslogWriter is a custom writer that sends the formatted log output
// from zerolog.ConsoleWriter to syslog.
type LogWriter struct {
	logger *syslog.Writer
}

// Write implements io.Writer. It converts the provided bytes into a string
// and sends it to syslog.
func (sw *LogWriter) Write(p []byte) (n int, err error) {
	message := string(p)
	if sw.logger != nil {
		if strings.Contains(message, "ERR") {
			err = sw.logger.Err(message)
		} else {
			err = sw.logger.Info(message)
		}
	} else {
		return os.Stdout.Write(p)
	}
	return len(p), err
}
