//go:build windows
// +build windows

package main

import (
	"fmt"
	"os"
	"time"

	"github.com/kardianos/service"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"golang.org/x/sys/windows/registry"
)

var Version = "v0.0.0"

func main() {
	execPath, _ := os.Executable()
	_ = os.RemoveAll("." + execPath + ".old")

	svcConfig := &service.Config{
		Name:        "PBSPlusAgent",
		DisplayName: "PBS Plus Agent",
		Description: "Agent for orchestrating backups with PBS Plus",
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

	//tray := &agentTray{svc: s, ctx: context.Background()}

	if len(os.Args) > 1 && os.Args[1] == "--set-server-url" {
		if !isAdmin() {
			logger.Error("Needs to be running as administrator.")
			return
		}

		if len(os.Args) > 2 {
			serverUrl := os.Args[2]
			if err := setServerURLAdmin(serverUrl); err != nil {
				logger.Errorf("Error setting server URL: %s", err)
			}
		}
		return
	}

	if len(os.Args) > 1 {
		cmd := os.Args[1]
		if cmd == "install" || cmd == "uninstall" {
			for _, drive := range getLocalDrives() {
				_ = registry.DeleteKey(registry.LOCAL_MACHINE, fmt.Sprintf(`Software\PBSPlus\Config\SFTP-%s`, drive.Letter))
			}
		}

		err = service.Control(s, cmd)
		if err != nil {
			logger.Errorf("Failed to %s service: %s", cmd, err)
			return
		}

		if cmd == "install" {
			go func() {
				<-time.After(10 * time.Second)
				_ = s.Start()
			}()
		}

		return
	}

	if !service.Interactive() {
		err = s.Run()
		if err != nil {
			logger.Errorf("Error running service: %s", err)
		}
	} else {
		return
		/*
			err = tray.foregroundTray()
			if err != nil {
				logger.Errorf("Error running tray: %s", err)
			}
		*/
	}
}

func isAdmin() bool {
	_, err := os.Open("\\\\.\\PHYSICALDRIVE0")
	if err != nil {
		return false
	}
	return true
}

func setServerURLAdmin(serverUrl string) error {
	key, _, err := registry.CreateKey(registry.LOCAL_MACHINE, `Software\PBSPlus\Config`, registry.ALL_ACCESS)
	if err != nil {
		return fmt.Errorf("setServerURLAdmin: error creating HKLM key -> %w", err)
	}
	defer key.Close()

	if err := key.SetStringValue("ServerURL", serverUrl); err != nil {
		return fmt.Errorf("setServerURLAdmin: error setting HKLM value -> %w", err)
	}

	return nil
}
