//go:build windows

package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/alexflint/go-filemutex"
	"github.com/kardianos/service"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/controllers"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"golang.org/x/sys/windows"
)

type UpdaterService struct {
	svc    service.Service
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

const (
	updateCheckInterval = 2 * time.Minute
)

var (
	mutex  sync.Mutex
	handle windows.Handle
)

func (u *UpdaterService) Start(s service.Service) error {
	u.svc = s
	u.ctx, u.cancel = context.WithCancel(context.Background())

	u.wg.Add(1)
	go func() {
		defer u.wg.Done()
		u.runUpdateCheck()
	}()

	return nil
}

func (u *UpdaterService) Stop(s service.Service) error {
	u.cancel()
	u.wg.Wait()
	return nil
}

func (u *UpdaterService) runUpdateCheck() {
	ticker := time.NewTicker(updateCheckInterval)
	defer ticker.Stop()

	checkAndUpdate := func() {
		hasActiveBackups, err := u.checkForActiveBackups()
		if err != nil {
			syslog.L.Errorf("Failed to check backup status: %v", err)
			return
		}
		if hasActiveBackups {
			syslog.L.Info("Skipping version check - backup in progress")
			return
		}

		newVersion, err := u.checkForNewVersion()
		if err != nil {
			syslog.L.Errorf("Version check failed: %v", err)
			return
		}

		if newVersion != "" {
			mainVersion, err := u.getMainServiceVersion()
			if err != nil {
				syslog.L.Errorf("Failed to get main version: %v", err)
				return
			}
			syslog.L.Infof("New version %s available, current version: %s", newVersion, mainVersion)

			// Double check before update
			hasActiveBackups, _ = u.checkForActiveBackups()
			if hasActiveBackups {
				syslog.L.Info("Postponing update - backup started during version check")
				return
			}

			if err := u.performUpdate(); err != nil {
				syslog.L.Errorf("Update failed: %v", err)
				return
			}

			syslog.L.Infof("Successfully updated to version %s", newVersion)
		}
	}

	// Initial check
	checkAndUpdate()

	for {
		select {
		case <-u.ctx.Done():
			return
		case <-ticker.C:
			checkAndUpdate()
		}
	}
}

func (u *UpdaterService) checkForActiveBackups() (bool, error) {
	store := controllers.GetNFSSessionStore()
	return store.HasSessions(), nil
}

func (u *UpdaterService) checkForNewVersion() (string, error) {
	var versionResp VersionResp
	_, err := agent.ProxmoxHTTPRequest(
		http.MethodGet,
		"/api2/json/plus/version",
		nil,
		&versionResp,
	)
	if err != nil {
		return "", err
	}

	mainVersion, err := u.getMainServiceVersion()
	if err != nil {
		return "", err
	}

	if versionResp.Version != mainVersion {
		return versionResp.Version, nil
	}
	return "", nil
}

func main() {
	svcConfig := &service.Config{
		Name:        "PBSPlusUpdater",
		DisplayName: "PBS Plus Updater Service",
		Description: "Handles automatic updates for PBS Plus Agent",
	}

	updater := &UpdaterService{}
	s, err := service.New(updater, svcConfig)
	if err != nil {
		fmt.Printf("Failed to initialize service: %v\n", err)
		return
	}

	err = syslog.InitializeLogger(s)
	if err != nil {
		fmt.Printf("Failed to initialize logger: %v\n", err)
		return
	}

	if err := createMutex(); err != nil {
		syslog.L.Errorf("Error: %v", err)
		os.Exit(1)
	}
	defer releaseMutex()

	if len(os.Args) > 1 {
		err = service.Control(s, os.Args[1])
		if err != nil {
			fmt.Printf("Failed to execute command %s: %v\n", os.Args[1], err)
			return
		}
		return
	}

	err = s.Run()
	if err != nil {
		syslog.L.Errorf("Service run failed: %v", err)
	}
}

func (p *UpdaterService) readVersionFromFile() (string, error) {
	ex, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("failed to get executable path: %w", err)
	}

	versionLockPath := filepath.Join(filepath.Dir(ex), "version.lock")
	mutex, err := filemutex.New(versionLockPath)
	if err != nil {
		return "", fmt.Errorf("failed to execute mutex: %w", err)
	}

	mutex.RLock()
	defer mutex.RUnlock()

	versionFile := filepath.Join(filepath.Dir(ex), "version.txt")
	data, err := os.ReadFile(versionFile)
	if err != nil {
		return "", fmt.Errorf("failed to read version file: %w", err)
	}

	version := strings.TrimSpace(string(data))
	if version == "" {
		syslog.L.Errorf("Version file is empty")
		return "", fmt.Errorf("version file is empty")
	}

	return version, nil
}

func createMutex() error {
	mutex.Lock()
	defer mutex.Unlock()

	// Create a unique mutex name based on the executable path
	execPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("failed to get executable path: %v", err)
	}
	mutexName := filepath.Base(execPath)

	// Try to create/acquire the named mutex
	h, err := windows.CreateMutex(nil, false, windows.StringToUTF16Ptr(mutexName))
	if err != nil {
		return fmt.Errorf("failed to create mutex: %v", err)
	}

	// Check if the mutex already exists
	if windows.GetLastError() == syscall.ERROR_ALREADY_EXISTS {
		windows.CloseHandle(h)
		return fmt.Errorf("another instance is already running")
	}

	handle = h
	return nil
}

func releaseMutex() {
	if handle != 0 {
		windows.CloseHandle(handle)
	}
}
