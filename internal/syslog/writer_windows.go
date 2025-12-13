//go:build windows

package syslog

import (
	"os"
	"strings"

	"golang.org/x/sys/windows/svc/eventlog"
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
