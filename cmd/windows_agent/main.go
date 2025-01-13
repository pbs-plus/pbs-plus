//go:build windows
// +build windows

package main

import (
	"fmt"
	"os"
	"path"
	"runtime/debug"
	"time"

	"github.com/kardianos/service"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"golang.org/x/sys/windows/registry"
	"golang.org/x/sys/windows/svc/eventlog"
)

var Version = "v0.0.0"
var eventLogger *eventlog.Log

const (
	eventSourceKey = "PBSPlusAgent"
)

func initEventLog() error {
	// Register event source
	err := eventlog.InstallAsEventCreate(eventSourceKey, eventlog.Error|eventlog.Warning|eventlog.Info)
	if err != nil {
		return fmt.Errorf("failed to install event source: %v", err)
	}

	// Open event log
	logger, err := eventlog.Open(eventSourceKey)
	if err != nil {
		return fmt.Errorf("failed to open event log: %v", err)
	}
	eventLogger = logger
	return nil
}

func logPanic(err interface{}) {
	if eventLogger != nil {
		stack := debug.Stack()
		errorMsg := fmt.Sprintf("PBS Plus Agent Panic: %v\nStack Trace:\n%s", err, stack)
		_ = eventLogger.Error(1, errorMsg)
	}
}

func setupPanicHandler() {
	if r := recover(); r != nil {
		logPanic(r)
		os.Exit(1)
	}
}

func main() {
	// Initialize Windows Event Log
	if err := initEventLog(); err != nil {
		fmt.Printf("Failed to initialize event log: %v\n", err)
	}
	defer func() {
		if eventLogger != nil {
			eventLogger.Close()
		}
	}()

	// Set up panic handler
	defer setupPanicHandler()

	execPath, _ := os.Executable()
	fileName := path.Base(execPath)
	filePath := path.Dir(execPath)
	fullOldPath := path.Join(filePath, "."+fileName+".old")
	_ = os.RemoveAll(fullOldPath)

	svcConfig := &service.Config{
		Name:        "PBSPlusAgent",
		DisplayName: "PBS Plus Agent",
		Description: "Agent for orchestrating backups with PBS Plus",
		UserName:    "",
	}

	prg := &agentService{}
	s, err := service.New(prg, svcConfig)
	if err != nil {
		if eventLogger != nil {
			_ = eventLogger.Error(1, fmt.Sprintf("Failed to initialize service: %v", err))
		}
		fmt.Println("Failed to initialize service:", err)
		return
	}
	prg.svc = s

	logger, err := syslog.InitializeLogger(s)
	if err != nil {
		if eventLogger != nil {
			_ = eventLogger.Error(1, fmt.Sprintf("Failed to initialize logger: %v", err))
		}
		fmt.Println("Failed to initialize logger:", err)
		return
	}

	if len(os.Args) > 1 && os.Args[1] == "--set-server-url" {
		if !isAdmin() {
			logger.Error("Needs to be running as administrator.")
			if eventLogger != nil {
				_ = eventLogger.Error(1, "Failed to set server URL: Not running as administrator")
			}
			return
		}
		if len(os.Args) > 2 {
			serverUrl := os.Args[2]
			if err := setServerURLAdmin(serverUrl); err != nil {
				logger.Errorf("Error setting server URL: %s", err)
				if eventLogger != nil {
					_ = eventLogger.Error(1, fmt.Sprintf("Error setting server URL: %v", err))
				}
			}
		}
		return
	}

	if len(os.Args) > 1 {
		cmd := os.Args[1]
		if cmd == "install" || cmd == "uninstall" {
			for _, drive := range getLocalDrives() {
				_ = registry.DeleteKey(registry.LOCAL_MACHINE, fmt.Sprintf(`Software\PBSPlus\Config\SFTP-%s`, drive))
			}
		}
		err = service.Control(s, cmd)
		if err != nil {
			logger.Errorf("Failed to %s service: %s", cmd, err)
			if eventLogger != nil {
				_ = eventLogger.Error(1, fmt.Sprintf("Failed to %s service: %v", cmd, err))
			}
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
			if eventLogger != nil {
				_ = eventLogger.Error(1, fmt.Sprintf("Error running service: %v", err))
			}
		}
	} else {
		return
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
