//go:build windows

package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"runtime/debug"
	"time"

	_ "net/http/pprof"

	"github.com/kardianos/service"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"golang.org/x/sys/windows/registry"
)

var Version = "v0.0.0"

// watchdogService wraps the original service and adds resilience
type watchdogService struct {
	*agentService
	restartCount    int
	lastRestartTime time.Time
	maxRestarts     int
	restartWindow   time.Duration
}

func newWatchdogService(original *agentService) *watchdogService {
	return &watchdogService{
		agentService:  original,
		maxRestarts:   5,             // Max restarts in window
		restartWindow: time.Hour * 1, // Reset counter after 1 hour
	}
}

func (w *watchdogService) resetRestartCounter() {
	if time.Since(w.lastRestartTime) > w.restartWindow {
		w.restartCount = 0
	}
}

func (w *watchdogService) shouldRestart() bool {
	w.resetRestartCounter()
	return w.restartCount < w.maxRestarts
}

func (w *watchdogService) Start(s service.Service) error {
	go func() {
		for {
			err := w.runWithRecovery(s)
			if err != nil {
				syslog.L.Errorf("Service failed with error: %v - Attempting restart", err)

				w.restartCount++
				w.lastRestartTime = time.Now()

				if !w.shouldRestart() {
					syslog.L.Errorf("Too many restart attempts (%d) within window. Waiting for window reset.", w.restartCount)
					time.Sleep(w.restartWindow)
					w.restartCount = 0
				}

				time.Sleep(time.Second * 5) // Brief delay before restart
				continue
			}
			break // Clean exit
		}
	}()
	return nil
}

func (w *watchdogService) runWithRecovery(s service.Service) (err error) {
	defer func() {
		if r := recover(); r != nil {
			stack := string(debug.Stack())
			err = fmt.Errorf("service panicked: %v\nStack:\n%s", r, stack)
			syslog.L.Error(err)
		}
	}()

	return w.agentService.Start(s)
}

func (w *watchdogService) Stop(s service.Service) error {
	return w.agentService.Stop(s)
}

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	svcConfig := &service.Config{
		Name:        "PBSPlusAgent",
		DisplayName: "PBS Plus Agent",
		Description: "Agent for orchestrating backups with PBS Plus",
		UserName:    "",
	}

	prg := &agentService{}
	watchdog := newWatchdogService(prg)

	s, err := service.New(watchdog, svcConfig)
	if err != nil {
		fmt.Printf("Failed to initialize service: %v\n", err)
		return
	}
	prg.svc = s

	err = syslog.InitializeLogger(s)
	if err != nil {
		fmt.Printf("Failed to initialize logger: %v\n", err)
		return
	}

	// Handle special commands (install, uninstall, etc.)
	if len(os.Args) > 1 {
		if err := handleServiceCommands(s, os.Args[1]); err != nil {
			syslog.L.Errorf("Command handling failed: %v", err)
			return
		}
	}

	// Run the service
	err = s.Run()
	if err != nil {
		syslog.L.Errorf("Service run failed: %v", err)
		// Instead of exiting, restart the service
		if err := restartService(); err != nil {
			syslog.L.Errorf("Service restart failed: %v", err)
		}
	}
}

func restartService() error {
	cmd := exec.Command("sc", "start", "PBSPlusAgent")
	return cmd.Run()
}

func handleServiceCommands(s service.Service, cmd string) error {
	switch cmd {
	case "version":
		fmt.Print(Version)
		os.Stdout.Sync()
		os.Exit(0)
	case "install", "uninstall":
		// Clean up registry before install/uninstall
		_ = registry.DeleteKey(registry.LOCAL_MACHINE, `Software\PBSPlus\Auth`)
		err := service.Control(s, cmd)
		if err != nil {
			return fmt.Errorf("failed to %s service: %v", cmd, err)
		}
		if cmd == "install" {
			go func() {
				<-time.After(10 * time.Second)
				_ = s.Start()
			}()
		}
	// case "--set-server-url":
	// 	if !isAdmin() {
	// 		return fmt.Errorf("needs to be running as administrator")
	// 	}
	// 	if len(os.Args) > 2 {
	// 		serverUrl := os.Args[2]
	// 		if err := setServerURLAdmin(serverUrl); err != nil {
	// 			return fmt.Errorf("error setting server URL: %v", err)
	// 		}
	// 	}
	default:
		err := service.Control(s, cmd)
		if err != nil {
			return fmt.Errorf("failed to execute command %s: %v", cmd, err)
		}
	}
	return nil
}
