//go:build windows
// +build windows

package main

import (
	"context"
	"fmt"
	"os"
	"os/user"
	"path/filepath"

	"github.com/go-ole/go-ole"
	"github.com/go-ole/go-ole/oleutil"
	"github.com/kardianos/service"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
)

func main() {
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

	tray := &agentTray{svc: s, ctx: context.Background()}

	if len(os.Args) > 1 && os.Args[1] == "--set-server-url" {
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
		switch cmd {
		case "install":
			err = s.Install()
			if err != nil {
				logger.Errorf("Failed to install service: %s", err)
			} else {
				logger.Info("Service installed")
				err = createStartupShortcut()
				if err != nil {
					logger.Errorf("Failed to create startup shortcut: %s", err)
				} else {
					logger.Info("Startup shortcut created")
				}
			}
			return
		case "uninstall":
			err = s.Uninstall()
			if err != nil {
				logger.Errorf("Failed to uninstall service: %s", err)
			} else {
				logger.Info("Service uninstalled")
				err = removeStartupShortcut()
				if err != nil {
					logger.Errorf("Failed to remove startup shortcut: %s", err)
				} else {
					logger.Info("Startup shortcut removed")
				}
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
		err = tray.foregroundTray()
		if err != nil {
			logger.Errorf("Error running tray: %s", err)
		}
	}
}

// createStartupShortcut creates a shortcut in the Windows Startup folder for the application.
func createStartupShortcut() error {
	usr, err := user.Current()
	if err != nil {
		return fmt.Errorf("failed to get current user: %w", err)
	}

	startupDir := filepath.Join(usr.HomeDir, "AppData", "Roaming", "Microsoft", "Windows", "Start Menu", "Programs", "Startup")
	shortcutPath := filepath.Join(startupDir, "PBSPlusAgent.lnk")

	// Get the executable path
	executable, err := os.Executable()
	if err != nil {
		return fmt.Errorf("failed to get executable path: %w", err)
	}

	// Initialize COM library
	ole.CoInitialize(0)
	defer ole.CoUninitialize()

	// Create the WScript.Shell COM object
	shell, err := oleutil.CreateObject("WScript.Shell")
	if err != nil {
		return fmt.Errorf("failed to create WScript.Shell object: %w", err)
	}
	defer shell.Release()

	// Cast the object to IDispatch
	shellDispatch, err := shell.QueryInterface(ole.IID_IDispatch)
	if err != nil {
		return fmt.Errorf("failed to query IDispatch interface: %w", err)
	}
	defer shellDispatch.Release()

	// Create the shortcut using the COM object
	link, err := oleutil.CallMethod(shellDispatch, "CreateShortcut", shortcutPath)
	if err != nil {
		return fmt.Errorf("failed to create shortcut: %w", err)
	}
	defer link.Clear()

	// Set shortcut properties
	oleutil.PutProperty(link.ToIDispatch(), "TargetPath", executable)
	oleutil.PutProperty(link.ToIDispatch(), "WorkingDirectory", filepath.Dir(executable))
	oleutil.PutProperty(link.ToIDispatch(), "WindowStyle", 1) // Normal window
	oleutil.PutProperty(link.ToIDispatch(), "Description", "PBS Plus Agent")

	// Save the shortcut
	_, err = oleutil.CallMethod(link.ToIDispatch(), "Save")
	if err != nil {
		return fmt.Errorf("failed to save shortcut: %w", err)
	}

	return nil
}

// removeStartupShortcut removes the shortcut from the Windows Startup folder.
func removeStartupShortcut() error {
	usr, err := user.Current()
	if err != nil {
		return fmt.Errorf("failed to get current user: %w", err)
	}

	startupDir := filepath.Join(usr.HomeDir, "AppData", "Roaming", "Microsoft", "Windows", "Start Menu", "Programs", "Startup")
	shortcutPath := filepath.Join(startupDir, "PBSPlusAgent.lnk")

	err = os.Remove(shortcutPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove startup shortcut: %w", err)
	}
	return nil
}
