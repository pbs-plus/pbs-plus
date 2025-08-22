//go:build windows

package main

import (
	"fmt"
	"os"

	"github.com/kardianos/service"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

func main() {
	svcConfig := &service.Config{
		Name:        "PBSPlusUpdater",
		DisplayName: "PBS Plus Updater Service",
		Description: "Handles automatic updates for PBS Plus Agent",
	}

	updater := &UpdaterService{}
	s, err := service.New(updater, svcConfig)
	if err != nil {
		syslog.L.Error(err).WithMessage("Failed to initialize service").Write()
		return
	}

	if err := createMutex(); err != nil {
		syslog.L.Error(err).WithMessage("mutex creation failed").Write()
		os.Exit(1)
	}
	defer releaseMutex()

	// Allow control commands like install, start, stop, etc.
	if len(os.Args) > 1 {
		if err := service.Control(s, os.Args[1]); err != nil {
			syslog.L.Error(err).
				WithMessage(fmt.Sprintf("Failed to execute command %s", os.Args[1])).
				Write()
		}
		return
	}

	if err := s.Run(); err != nil {
		syslog.L.Error(err).WithMessage("failed to run service").Write()
	}
}
