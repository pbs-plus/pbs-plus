//go:build unix

package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gofrs/flock"
	"github.com/pbs-plus/pbs-plus/internal/agent"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

type UpdaterService struct {
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
	mutex    sync.Mutex
	lockFile *os.File
)

func (u *UpdaterService) Start() error {
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

func (u *UpdaterService) Stop() error {
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
	if err := createLockFile(); err != nil {
		syslog.L.Error(err).Write()
		os.Exit(1)
	}
	defer releaseLockFile()

	updater := &UpdaterService{}

	if err := updater.Start(); err != nil {
		fmt.Printf("Failed to start updater: %v\n", err)
		os.Exit(1)
	}

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	syslog.L.Info().WithMessage("received shutdown signal").Write()

	if err := updater.Stop(); err != nil {
		syslog.L.Error(err).WithMessage("failed to stop updater").Write()
	}
}

func (u *UpdaterService) readVersionFromFile() (string, error) {
	if err := os.MkdirAll("/etc/pbs-plus-agent", 0755); err != nil {
		return "", err
	}

	versionLockPath := filepath.Join("/etc/pbs-plus-agent", "version.lock")
	mutex := flock.New(versionLockPath)

	mutex.RLock()
	defer mutex.Close()

	versionFile := filepath.Join("/etc/pbs-plus-agent", "version.txt")
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

func createLockFile() error {
	mutex.Lock()
	defer mutex.Unlock()

	execPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("failed to get executable path: %v", err)
	}

	lockPath := filepath.Join("/var/lock", filepath.Base(execPath)+".lock")

	// Try to create lock file
	file, err := os.OpenFile(lockPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0644)
	if err != nil {
		if os.IsExist(err) {
			return fmt.Errorf("another instance is already running")
		}
		return fmt.Errorf("failed to create lock file: %v", err)
	}

	// Write PID to lock file
	if _, err := fmt.Fprintf(file, "%d\n", os.Getpid()); err != nil {
		file.Close()
		os.Remove(lockPath)
		return fmt.Errorf("failed to write PID to lock file: %v", err)
	}

	lockFile = file
	return nil
}

func releaseLockFile() {
	if lockFile != nil {
		lockFile.Close()
		os.Remove(lockFile.Name())
	}
}
