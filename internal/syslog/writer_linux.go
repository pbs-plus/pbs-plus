//go:build linux

package syslog

import (
	"log/syslog"
	"strings"

	"github.com/rs/zerolog/log"
)

// SyslogWriter is a custom writer that sends the formatted log output
// from zerolog.ConsoleWriter to syslog.
type LogWriter struct {
	logger *syslog.Writer
}

// Write implements io.Writer. It converts the provided bytes into a string
// and sends it to syslog. (In this simple example, we always use the Info level.
// You can extend this if needed.)
func (sw *LogWriter) Write(p []byte) (n int, err error) {
	message := string(p)
	if sw.logger != nil {
		if strings.Contains(message, "ERR") {
			err = sw.logger.Err(message)
		} else {
			err = sw.logger.Info(message)
		}
	} else {
		log.Print(message)
	}
	return len(p), err
}
