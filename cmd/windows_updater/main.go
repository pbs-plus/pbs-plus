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

	"github.com/gofrs/flock"
	"github.com/kardianos/service"
	"github.com/pbs-plus/pbs-plus/internal/agent"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"golang.org/x/sys/windows"
)

type UpdaterService struct {
	svc      service.Service
	hostname string
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

type VersionResp struct {
	Version string `json:"version"`
}

const updateCheckInterval = 2 * time.Minute

var (
	mutex  sync.Mutex
	handle windows.Handle
)

func (u *UpdaterService) Start(s service.Service) error {
	u.svc = s
	u.ctx, u.cancel = context.WithCancel(context.Background())

	hostname, err := os.Hostname()
	if err == nil {
		u.hostname = hostname
	}

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
		newVersion, err := u.checkForNewVersion()
		if err != nil {
			syslog.L.Error(err).WithMessage("failed to check version").WithField("hostname", u.hostname).Write()
			return
		}

		if newVersion != "" {
			mainVersion, err := u.getMainServiceVersion()
			if err != nil {
				syslog.L.Error(err).WithMessage("failed to get main version").WithField("hostname", u.hostname).Write()
				return
			}
			syslog.L.Info().WithMessage("new version available").
				WithFields(map[string]interface{}{"new": newVersion, "current": mainVersion}).
				WithField("hostname", u.hostname).
				Write()

			if err := u.performUpdate(); err != nil {
				syslog.L.Error(err).WithMessage("failed to update").WithField("hostname", u.hostname).Write()
				return
			}

			syslog.L.Info().WithMessage("updated to version").WithField("version", newVersion).WithField("hostname", u.hostname).Write()
		}

		// Perform cleanup after update check
		if err := u.cleanupOldUpdates(); err != nil {
			syslog.L.Error(err).WithMessage("failed to clean up old updates").WithField("hostname", u.hostname).Write()
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

	if err := createMutex(); err != nil {
		syslog.L.Error(err).Write()
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
		syslog.L.Error(err).WithMessage("failed to run service").Write()
	}
}

func (u *UpdaterService) readVersionFromFile() (string, error) {
	ex, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("failed to get executable path: %w", err)
	}

	versionLockPath := filepath.Join(filepath.Dir(ex), "version.lock")
	mutex := flock.New(versionLockPath)

	mutex.RLock()
	defer mutex.Close()

	versionFile := filepath.Join(filepath.Dir(ex), "version.txt")
	data, err := os.ReadFile(versionFile)
	if err != nil {
		return "", fmt.Errorf("failed to read version file: %w", err)
	}

	version := strings.TrimSpace(string(data))
	if version == "" {
		return "", fmt.Errorf("version file is empty")
	}

	return version, nil
}

func createMutex() error {
	mutex.Lock()
	defer mutex.Unlock()

	execPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("failed to get executable path: %v", err)
	}
	mutexName := filepath.Base(execPath)

	h, err := windows.CreateMutex(nil, false, windows.StringToUTF16Ptr(mutexName))
	if err != nil {
		return fmt.Errorf("failed to create mutex: %v", err)
	}

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
