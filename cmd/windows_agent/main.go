//go:build windows

package main

import (
	"fmt"
	"os"
	"os/signal"
	"runtime/debug"
	"sync"
	"syscall"
	"time"

	"github.com/kardianos/service"
	"github.com/pbs-plus/pbs-plus/internal/agent/forks"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/registry"
)

var Version = "v0.0.0"
var (
	mutex  sync.Mutex
	handle windows.Handle
)

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
				syslog.L.Error(err).WithMessage("service failed and attempting to restart").Write()

				w.restartCount++
				w.lastRestartTime = time.Now()

				if !w.shouldRestart() {
					syslog.L.Error(nil).WithMessage("maximum restarts attempts reached and will resume in a few seconds").Write()
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
			syslog.L.Error(err).Write()
		}
	}()

	return w.agentService.Start(s)
}

func (w *watchdogService) Stop(s service.Service) error {
	return w.agentService.Stop(s)
}

func main() {
	defer func() {
		if r := recover(); r != nil {
			msg := fmt.Sprintf("Panic occurred: %v\nStack trace:\n%s", r, debug.Stack())

			logFile, err := os.OpenFile("panic.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
			if err == nil {
				defer logFile.Close()
				logFile.WriteString(msg)
			} else {
				fmt.Println("Error opening log file:", err)
			}

			os.Exit(1)
		}
	}()

	forks.CmdBackup()

	constants.Version = Version

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
		syslog.L.Error(err).WithMessage("failed to initialize service").Write()
		return
	}
	prg.svc = s

	err = prg.writeVersionToFile()
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to write version to file").Write()
		return
	}

	// Handle special commands (install, uninstall, etc.)
	if len(os.Args) > 1 {
		if err := handleServiceCommands(s, os.Args[1]); err != nil {
			syslog.L.Error(err).WithMessage("failed to handle command").Write()
			return
		}
		return
	}

	if service.Interactive() {
		setupConsoleSignals(func() {
			if prg.cancel != nil {
				prg.cancel()
			}
			prg.wg.Wait()
			releaseMutex()
		})
	}

	// Run the service
	if err := s.Run(); err != nil {
		syslog.L.Error(err).WithMessage("failed to run service").Write()
	}
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

func createMutex() error {
	mutex.Lock()
	defer mutex.Unlock()

	mutexName := "Global\\PBSPlusAgent"

	// Try to create/acquire the named mutex
	h, err := windows.CreateMutex(nil, false, windows.StringToUTF16Ptr(mutexName))
	switch err {
	case nil:
		handle = h
		return nil
	case windows.ERROR_ALREADY_EXISTS:
		windows.CloseHandle(h)
		return fmt.Errorf("another instance of pbs-plus-agent is already running")
	default:
		return fmt.Errorf("failed to create/open mutex: %w", err)
	}
}

func releaseMutex() {
	mutex.Lock()
	defer mutex.Unlock()
	if handle != 0 {
		windows.CloseHandle(handle)
	}
}

func setupConsoleSignals(onShutdown func()) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigs
		onShutdown()
		os.Exit(0)
	}()
}
