//go:build windows
// +build windows

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/kardianos/service"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/syslog"
)

func main() {
	svcConfig := &service.Config{
		Name:        "ProxmoxBackupAgent",
		DisplayName: "Proxmox Backup Agent",
		Description: "Agent for orchestrating backups with Proxmox Backup Server",
		UserName:    "",
	}

	prg := &agentService{}
	s, err := service.New(prg, svcConfig)
	if err != nil {
		fmt.Println("Failed to initialize service:", err)
		return
	}
	prg.svc = s

	logger, err := syslog.InitializeLogger(s)
	if err != nil {
		fmt.Println("Failed to initialize logger:", err)
		return
	}

	tray := &agentTray{svc: s, ctx: context.Background()}

	if len(os.Args) > 1 {
		cmd := os.Args[1]
		switch cmd {
		case "install":
			err = s.Install()
			if err != nil {
				logger.Errorf("Failed to install service: %s", err)
			} else {
				logger.Info("Service installed")
			}
			return
		case "uninstall":
			err = s.Uninstall()
			if err != nil {
				logger.Errorf("Failed to uninstall service: %s", err)
			} else {
				logger.Info("Service uninstalled")
			}
			return
		case "start":
			err = s.Start()
			if err != nil {
				logger.Errorf("Failed to start service: %s", err)
			} else {
				logger.Info("Service started")
			}
			return
		case "stop":
			err = s.Stop()
			if err != nil {
				logger.Errorf("Failed to stop service: %s", err)
			} else {
				logger.Info("Service stopped")
			}
			return
		default:
			logger.Errorf("Unknown command: %s", cmd)
			logger.Info("Available commands: install, uninstall, start, stop")
			return
		}
	}

	if !service.Interactive() {
		err = s.Run()
		if err != nil {
			logger.Errorf("Error running service: %s", err)
		}
	} else {
		if !isAdmin() {
			err = runAsAdmin()
			if err != nil {
				logger.Errorf("Error running as admin: %s", err)
			}
			return
		}

		err = tray.foregroundTray()
		if err != nil {
			logger.Errorf("Error running tray: %s", err)
		}
	}
}
