//go:build windows

package main

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gofrs/flock"
	"github.com/kardianos/service"
	"github.com/pbs-plus/pbs-plus/internal/agent"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
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

func (u *UpdaterService) Start(s service.Service) error {
	if err := createMutex(); err != nil {
		syslog.L.Error(err).WithMessage("mutex creation failed").Write()
		return err
	}

	u.svc = s
	u.ctx, u.cancel = context.WithCancel(context.Background())

	syslog.L.SetServiceLogger(s)

	if hostname, err := os.Hostname(); err == nil {
		u.hostname = hostname
	}

	u.wg.Add(1)
	go func() {
		defer u.wg.Done()
		u.runUpdateLoop()
	}()

	return nil
}

func (u *UpdaterService) Stop(service.Service) error {
	if u.cancel != nil {
		u.cancel()
	}
	u.wg.Wait()
	releaseMutex()
	return nil
}

func (u *UpdaterService) runUpdateLoop() {
	ticker := time.NewTicker(updateCheckInterval)
	defer ticker.Stop()

	u.checkAndUpdateOnce()

	for {
		select {
		case <-u.ctx.Done():
			return
		case <-ticker.C:
			u.checkAndUpdateOnce()
		}
	}
}

func (u *UpdaterService) checkAndUpdateOnce() {
	newVersion, err := u.checkForNewVersion()
	if err != nil {
		syslog.L.Error(err).
			WithMessage("failed to check version").
			WithField("hostname", u.hostname).
			Write()
		_ = u.cleanupOldUpdates()
		return
	}

	if newVersion != "" {
		mainVersion, err := u.getMainServiceVersion()
		if err != nil {
			syslog.L.Error(err).
				WithMessage("failed to get main version").
				WithField("hostname", u.hostname).
				Write()
			_ = u.cleanupOldUpdates()
			return
		}

		syslog.L.Info().
			WithMessage("new version available").
			WithFields(map[string]interface{}{
				"new":     newVersion,
				"current": mainVersion,
			}).
			WithField("hostname", u.hostname).
			Write()

		if err := u.performUpdate(); err != nil {
			syslog.L.Error(err).
				WithMessage("failed to update").
				WithField("hostname", u.hostname).
				Write()
		} else {
			syslog.L.Info().
				WithMessage("updated to version").
				WithField("version", newVersion).
				WithField("hostname", u.hostname).
				Write()
		}
	}

	if err := u.cleanupOldUpdates(); err != nil {
		syslog.L.Error(err).
			WithMessage("failed to clean up old updates").
			WithField("hostname", u.hostname).
			Write()
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

	if versionResp.Version != "" && versionResp.Version != mainVersion {
		return versionResp.Version, nil
	}
	return "", nil
}

// readVersionFromFile returns the installed agent version guarded by a shared lock.
func (u *UpdaterService) readVersionFromFile() (string, error) {
	ex, err := os.Executable()
	if err != nil {
		return "", err
	}

	lockPath := filepath.Join(filepath.Dir(ex), "version.lock")
	fileLock := flock.New(lockPath)

	if err := fileLock.RLock(); err != nil {
		return "", err
	}
	defer fileLock.Close()

	versionFile := filepath.Join(filepath.Dir(ex), "version.txt")
	data, err := os.ReadFile(versionFile)
	if err != nil {
		return "", err
	}

	version := strings.TrimSpace(string(data))
	if version == "" {
		return "", ErrEmptyVersion
	}
	return version, nil
}
