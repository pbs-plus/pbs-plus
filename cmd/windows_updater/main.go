//go:build windows

package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/kardianos/service"
	"github.com/pbs-plus/pbs-plus/internal/agent"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

var Version = "v0.0.0"

type updaterService struct {
	logger   service.Logger
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	hostname string
}

type VersionResp struct {
	Version string `json:"version"`
}

const (
	updateCheckInterval = 2 * time.Minute
	tempUpdateDir       = "updates"
	mainServiceName     = "PBSPlusAgent"
	mainBinaryName      = "pbs-plus-agent.exe"
	maxUpdateRetries    = 3
	updateRetryDelay    = 5 * time.Second
)

func (u *updaterService) Start(s service.Service) error {
	if logger, err := s.Logger(nil); err == nil {
		u.logger = logger
		syslog.L.SetServiceLogger(logger)
	}

	syslog.L.Info().WithMessage(fmt.Sprintf("Starting PBS Plus Updater %s", Version)).Write()

	if hostname, err := os.Hostname(); err == nil {
		u.hostname = hostname
		syslog.L.Info().WithMessage(fmt.Sprintf("Updater running on hostname: %s", hostname)).Write()
	} else {
		syslog.L.Warn().WithMessage(fmt.Sprintf("Failed to get hostname: %v", err)).Write()
	}

	u.ctx, u.cancel = context.WithCancel(context.Background())
	go u.run()

	return nil
}

func (u *updaterService) Stop(s service.Service) error {
	syslog.L.Info().WithMessage("Stopping PBS Plus Updater").Write()

	if u.cancel != nil {
		u.cancel()
	}

	done := make(chan struct{})
	go func() {
		u.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		syslog.L.Info().WithMessage("Updater stopped gracefully").Write()
	case <-time.After(30 * time.Second):
		syslog.L.Warn().WithMessage("Updater stop timeout reached").Write()
	}

	return nil
}

func (u *updaterService) run() {
	defer func() {
		if r := recover(); r != nil {
			syslog.L.Error(fmt.Errorf("panic: %v", r)).WithMessage("Updater service panicked").Write()
		}
	}()

	u.wg.Add(1)
	go func() {
		defer u.wg.Done()

		ticker := time.NewTicker(updateCheckInterval)
		defer ticker.Stop()

		syslog.L.Info().WithMessage(fmt.Sprintf("Starting update check routine with interval: %v", updateCheckInterval)).Write()

		u.checkAndUpdate()

		for {
			select {
			case <-u.ctx.Done():
				syslog.L.Info().WithMessage("Update check routine stopped").Write()
				return
			case <-ticker.C:
				u.checkAndUpdate()
			}
		}
	}()
}

func (u *updaterService) checkAndUpdate() {
	newVersion, err := u.checkForNewVersion()
	if err != nil {
		syslog.L.Warn().WithMessage(fmt.Sprintf("Failed to check version: %v", err)).Write()
		return
	}

	if newVersion == "" {
		return
	}

	mainVersion, err := u.getMainServiceVersion()
	if err != nil {
		syslog.L.Warn().WithMessage(fmt.Sprintf("Failed to get main version: %v", err)).Write()
		return
	}

	syslog.L.Info().WithMessage(fmt.Sprintf("New version available: %s (current: %s)", newVersion, mainVersion)).Write()

	if err := u.performUpdate(); err != nil {
		syslog.L.Error(err).WithMessage("Failed to update").Write()
		return
	}

	syslog.L.Info().WithMessage(fmt.Sprintf("Updated to version: %s", newVersion)).Write()

	if err := u.cleanupOldUpdates(); err != nil {
		syslog.L.Warn().WithMessage(fmt.Sprintf("Failed to clean up old updates: %v", err)).Write()
	}
}

func (u *updaterService) checkForNewVersion() (string, error) {
	var versionResp VersionResp

	resp, err := agent.ProxmoxHTTPRequest(http.MethodGet, "/api2/json/plus/version", nil, nil)
	if err != nil {
		return "", err
	}
	defer resp.Close()

	data, err := io.ReadAll(resp)
	if err != nil {
		return "", err
	}

	if err := json.Unmarshal(data, &versionResp); err != nil {
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

func (u *updaterService) getMainServiceVersion() (string, error) {
	ex, err := os.Executable()
	if err != nil {
		return "", err
	}

	versionFile := filepath.Join(filepath.Dir(ex), "version.txt")

	data, err := os.ReadFile(versionFile)
	if err != nil {
		return "", err
	}

	version := strings.TrimSpace(string(data))
	if version == "" {
		return "", fmt.Errorf("version file is empty")
	}

	return version, nil
}

func (u *updaterService) performUpdate() error {
	syslog.L.Info().WithMessage("Starting update process").Write()

	for retry := 0; retry < maxUpdateRetries; retry++ {
		if retry > 0 {
			syslog.L.Info().WithMessage(fmt.Sprintf("Retrying update, attempt: %d", retry+1)).Write()
			time.Sleep(updateRetryDelay)
		}

		if err := u.tryUpdate(); err == nil {
			syslog.L.Info().WithMessage("Update completed successfully").Write()
			return nil
		} else {
			syslog.L.Warn().WithMessage(fmt.Sprintf("Update attempt %d failed: %v", retry+1, err)).Write()
		}
	}
	return fmt.Errorf("all %d update attempts failed", maxUpdateRetries)
}

func (u *updaterService) tryUpdate() error {
	syslog.L.Info().WithMessage("Attempting update").Write()

	tempFile, err := u.downloadUpdate()
	if err != nil {
		return fmt.Errorf("download failed: %w", err)
	}
	defer os.Remove(tempFile)

	if err := u.verifyUpdate(tempFile); err != nil {
		return fmt.Errorf("verification failed: %w", err)
	}

	return u.applyUpdate(tempFile)
}

func (u *updaterService) downloadUpdate() (string, error) {
	syslog.L.Info().WithMessage("Downloading update binary").Write()

	tempDir, err := u.ensureTempDir()
	if err != nil {
		return "", err
	}

	tempFile := filepath.Join(tempDir, fmt.Sprintf("update-%s.tmp", time.Now().Format("20060102150405")))
	syslog.L.Info().WithMessage(fmt.Sprintf("Creating temp file: %s", tempFile)).Write()

	file, err := os.Create(tempFile)
	if err != nil {
		return "", err
	}
	defer file.Close()

	resp, err := agent.ProxmoxHTTPRequest(http.MethodGet, "/api2/json/plus/binary", nil, nil)
	if err != nil {
		os.Remove(tempFile)
		return "", err
	}
	defer resp.Close()

	if _, err := io.Copy(file, resp); err != nil {
		os.Remove(tempFile)
		return "", err
	}

	syslog.L.Info().WithMessage(fmt.Sprintf("Successfully downloaded update to: %s", tempFile)).Write()
	return tempFile, nil
}

func (u *updaterService) verifyUpdate(tempFile string) error {
	syslog.L.Info().WithMessage("Verifying update binary").Write()

	expectedMD5, err := u.downloadMD5()
	if err != nil {
		return err
	}

	syslog.L.Info().WithMessage(fmt.Sprintf("Expected MD5: %s", expectedMD5)).Write()

	actualMD5, err := u.calculateMD5(tempFile)
	if err != nil {
		return err
	}

	syslog.L.Info().WithMessage(fmt.Sprintf("Calculated MD5: %s", actualMD5)).Write()

	if !strings.EqualFold(actualMD5, expectedMD5) {
		return fmt.Errorf("MD5 mismatch: expected %s, got %s", expectedMD5, actualMD5)
	}

	syslog.L.Info().WithMessage("Binary verification successful").Write()
	return nil
}

func (u *updaterService) downloadMD5() (string, error) {
	syslog.L.Info().WithMessage("Downloading MD5 checksum").Write()

	resp, err := agent.ProxmoxHTTPRequest(http.MethodGet, "/api2/json/plus/binary/checksum", nil, nil)
	if err != nil {
		return "", err
	}
	defer resp.Close()

	md5Bytes, err := io.ReadAll(resp)
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(md5Bytes)), nil
}

func (u *updaterService) calculateMD5(filepath string) (string, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

func (u *updaterService) applyUpdate(tempFile string) error {
	syslog.L.Info().WithMessage("Applying update").Write()

	mainBinary := filepath.Join(filepath.Dir(tempFile), "..", mainBinaryName)
	backupPath := mainBinary + ".backup"

	syslog.L.Info().WithMessage(fmt.Sprintf("Main binary path: %s", mainBinary)).Write()
	syslog.L.Info().WithMessage(fmt.Sprintf("Backup path: %s", backupPath)).Write()

	if err := u.stopMainService(); err != nil {
		return fmt.Errorf("failed to stop service: %w", err)
	}

	syslog.L.Info().WithMessage("Creating backup of current binary").Write()
	if err := os.Rename(mainBinary, backupPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to create backup: %w", err)
	}

	syslog.L.Info().WithMessage("Replacing binary with new version").Write()
	if err := os.Rename(tempFile, mainBinary); err != nil {
		syslog.L.Error(err).WithMessage("Failed to replace binary, restoring backup").Write()
		os.Rename(backupPath, mainBinary)
		return fmt.Errorf("failed to replace binary: %w", err)
	}

	if err := u.startMainService(); err != nil {
		syslog.L.Error(err).WithMessage("Failed to start service, restoring backup").Write()
		os.Rename(backupPath, mainBinary)
		return fmt.Errorf("failed to start service: %w", err)
	}

	syslog.L.Info().WithMessage("Removing backup file").Write()
	os.Remove(backupPath)
	syslog.L.Info().WithMessage("Update applied successfully").Write()
	return nil
}

func (u *updaterService) stopMainService() error {
	syslog.L.Info().WithMessage(fmt.Sprintf("Stopping main service: %s", mainServiceName)).Write()

	cmd := exec.Command("sc", "stop", mainServiceName)
	if err := cmd.Run(); err != nil {
		return err
	}

	syslog.L.Info().WithMessage("Waiting for service to stop").Write()
	for i := 0; i < 20; i++ {
		cmd := exec.Command("sc", "query", mainServiceName)
		output, _ := cmd.CombinedOutput()
		if strings.Contains(string(output), "STOPPED") {
			syslog.L.Info().WithMessage(fmt.Sprintf("Service stopped after %d seconds", i*10)).Write()
			return nil
		}
		time.Sleep(10 * time.Second)
	}

	return fmt.Errorf("timeout waiting for service to stop")
}

func (u *updaterService) startMainService() error {
	syslog.L.Info().WithMessage(fmt.Sprintf("Starting main service: %s", mainServiceName)).Write()

	cmd := exec.Command("sc", "start", mainServiceName)
	err := cmd.Run()
	if err != nil {
		syslog.L.Error(err).WithMessage("Failed to start service").Write()
	} else {
		syslog.L.Info().WithMessage("Service started successfully").Write()
	}
	return err
}

func (u *updaterService) ensureTempDir() (string, error) {
	ex, err := os.Executable()
	if err != nil {
		return "", err
	}

	tempDir := filepath.Join(filepath.Dir(ex), tempUpdateDir)
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return "", err
	}

	return tempDir, nil
}

func (u *updaterService) cleanupOldUpdates() error {
	syslog.L.Info().WithMessage("Cleaning up old update files").Write()

	tempDir, err := u.ensureTempDir()
	if err != nil {
		return err
	}

	entries, err := os.ReadDir(tempDir)
	if err != nil {
		return err
	}

	cleanedFiles := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			path := filepath.Join(tempDir, entry.Name())
			info, err := entry.Info()
			if err == nil && time.Since(info.ModTime()) > 24*time.Hour {
				if err := os.Remove(path); err == nil {
					cleanedFiles++
				}
			}
		}
	}

	backups, _ := filepath.Glob(filepath.Join(tempDir, "*.backup"))
	cleanedBackups := 0
	for _, backup := range backups {
		info, _ := os.Stat(backup)
		if info != nil && time.Since(info.ModTime()) > 48*time.Hour {
			if err := os.Remove(backup); err == nil {
				cleanedBackups++
			}
		}
	}

	if cleanedFiles > 0 || cleanedBackups > 0 {
		syslog.L.Info().WithMessage(fmt.Sprintf("Cleaned up %d old files and %d old backups", cleanedFiles, cleanedBackups)).Write()
	}

	return nil
}

func main() {
	svcConfig := &service.Config{
		Name:        "PBSPlusUpdater",
		DisplayName: "PBS Plus Updater Service",
		Description: "Handles automatic updates for PBS Plus Agent",
		Option: service.KeyValue{
			"OnFailure":              "restart",
			"OnFailureDelayDuration": "5s",
			"OnFailureResetPeriod":   300,
		},
	}

	prg := &updaterService{}
	s, err := service.New(prg, svcConfig)
	if err != nil {
		log.Fatal("Failed to create service: ", err)
	}

	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "install":
			err := service.Control(s, "install")
			if err != nil {
				log.Fatal("Failed to install service: ", err)
			}
			fmt.Println("Updater service installed successfully")
		case "uninstall":
			err := service.Control(s, "uninstall")
			if err != nil {
				log.Fatal("Failed to uninstall service: ", err)
			}
			fmt.Println("Updater service uninstalled successfully")
		default:
			err := service.Control(s, os.Args[1])
			if err != nil {
				log.Fatal("Failed to execute command: ", err)
			}
		}
		return
	}

	err = s.Run()
	if err != nil {
		log.Fatal("Service failed: ", err)
	}
}
