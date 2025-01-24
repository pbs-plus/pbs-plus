//go:build windows

package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/kardianos/service"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/controllers"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
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
