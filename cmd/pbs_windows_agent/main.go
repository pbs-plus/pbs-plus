//go:build windows
// +build windows

package main

import (
	"fmt"
	"os"

	"github.com/kardianos/service"
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

	tray := &agentTray{svc: s}

	if len(os.Args) > 1 {
		cmd := os.Args[1]
		switch cmd {
		case "install":
			err = s.Install()
			if err != nil {
				fmt.Println("Failed to install service:", err)
			} else {
				fmt.Println("Service installed")
			}
			return
		case "uninstall":
			err = s.Uninstall()
			if err != nil {
				fmt.Println("Failed to uninstall service:", err)
			} else {
				fmt.Println("Service uninstalled")
			}
			return
		case "start":
			err = s.Start()
			if err != nil {
				fmt.Println("Failed to start service:", err)
			} else {
				fmt.Println("Service started")
			}
			return
		case "stop":
			err = s.Stop()
			if err != nil {
				fmt.Println("Failed to stop service:", err)
			} else {
				fmt.Println("Service stopped")
			}
			return
		default:
			fmt.Println("Unknown command:", cmd)
			fmt.Println("Available commands: install, uninstall, start, stop")
			return
		}
	}

	if isWindowsService() {
		err = s.Run()
		if err != nil {
			fmt.Println("Error running service:", err)
		}
	} else {
		if !isAdmin() {
			runAsAdmin()
			return
		}

		err = tray.foregroundTray()
	}
}
