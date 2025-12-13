//go:build unix

package syslog

import (
	"log/syslog"
	"os"
	"strings"
)

type LogWriter struct {
	logger *syslog.Writer
}

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
