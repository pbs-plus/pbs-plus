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
		syslog.L.SetServiceLogger(s)
	}

	u.logger.Info("Starting PBS Plus Updater v", Version)

	if hostname, err := os.Hostname(); err == nil {
		u.hostname = hostname
	}

	u.ctx, u.cancel = context.WithCancel(context.Background())
	go u.run()

	return nil
}

func (u *updaterService) Stop(s service.Service) error {
	u.logger.Info("Stopping PBS Plus Updater")

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
		u.logger.Info("Updater stopped gracefully")
	case <-time.After(30 * time.Second):
		u.logger.Warning("Updater stop timeout reached")
	}

	return nil
}

func (u *updaterService) run() {
	defer func() {
		if r := recover(); r != nil {
			u.logger.Error("Updater service panicked: ", r)
		}
	}()

	u.wg.Add(1)
	go func() {
		defer u.wg.Done()

		ticker := time.NewTicker(updateCheckInterval)
		defer ticker.Stop()

		u.checkAndUpdate()

		for {
			select {
			case <-u.ctx.Done():
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
		u.logger.Warning("Failed to check version: ", err)
		return
	}

	if newVersion == "" {
		return
	}

	mainVersion, err := u.getMainServiceVersion()
	if err != nil {
		u.logger.Warning("Failed to get main version: ", err)
		return
	}

	u.logger.Info("New version available: ", newVersion, " (current: ", mainVersion, ")")

	if err := u.performUpdate(); err != nil {
		u.logger.Error("Failed to update: ", err)
		return
	}

	u.logger.Info("Updated to version: ", newVersion)

	if err := u.cleanupOldUpdates(); err != nil {
		u.logger.Warning("Failed to clean up old updates: ", err)
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
	for retry := 0; retry < maxUpdateRetries; retry++ {
		if retry > 0 {
			u.logger.Info("Retrying update, attempt: ", retry+1)
			time.Sleep(updateRetryDelay)
		}

		if err := u.tryUpdate(); err == nil {
			return nil
		} else {
			u.logger.Warning("Update attempt failed: ", err)
		}
	}
	return fmt.Errorf("all %d update attempts failed", maxUpdateRetries)
}

func (u *updaterService) tryUpdate() error {
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
	tempDir, err := u.ensureTempDir()
	if err != nil {
		return "", err
	}

	tempFile := filepath.Join(tempDir, fmt.Sprintf("update-%s.tmp", time.Now().Format("20060102150405")))
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

	return tempFile, nil
}

func (u *updaterService) verifyUpdate(tempFile string) error {
	expectedMD5, err := u.downloadMD5()
	if err != nil {
		return err
	}

	actualMD5, err := u.calculateMD5(tempFile)
	if err != nil {
		return err
	}

	if !strings.EqualFold(actualMD5, expectedMD5) {
		return fmt.Errorf("MD5 mismatch: expected %s, got %s", expectedMD5, actualMD5)
	}

	return nil
}

func (u *updaterService) downloadMD5() (string, error) {
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
	mainBinary := filepath.Join(filepath.Dir(tempFile), "..", mainBinaryName)
	backupPath := mainBinary + ".backup"

	if err := u.stopMainService(); err != nil {
		return fmt.Errorf("failed to stop service: %w", err)
	}

	if err := os.Rename(mainBinary, backupPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to create backup: %w", err)
	}

	if err := os.Rename(tempFile, mainBinary); err != nil {
		os.Rename(backupPath, mainBinary)
		return fmt.Errorf("failed to replace binary: %w", err)
	}

	if err := u.startMainService(); err != nil {
		os.Rename(backupPath, mainBinary)
		return fmt.Errorf("failed to start service: %w", err)
	}

	os.Remove(backupPath)
	return nil
}

func (u *updaterService) stopMainService() error {
	cmd := exec.Command("sc", "stop", mainServiceName)
	if err := cmd.Run(); err != nil {
		return err
	}

	for i := 0; i < 10; i++ {
		cmd := exec.Command("sc", "query", mainServiceName)
		output, _ := cmd.CombinedOutput()
		if strings.Contains(string(output), "STOPPED") {
			return nil
		}
		time.Sleep(2 * time.Second)
	}

	return fmt.Errorf("timeout waiting for service to stop")
}

func (u *updaterService) startMainService() error {
	cmd := exec.Command("sc", "start", mainServiceName)
	return cmd.Run()
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
	tempDir, err := u.ensureTempDir()
	if err != nil {
		return err
	}

	entries, err := os.ReadDir(tempDir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			path := filepath.Join(tempDir, entry.Name())
			info, err := entry.Info()
			if err == nil && time.Since(info.ModTime()) > 24*time.Hour {
				os.Remove(path)
			}
		}
	}

	backups, _ := filepath.Glob(filepath.Join(tempDir, "*.backup"))
	for _, backup := range backups {
		info, _ := os.Stat(backup)
		if info != nil && time.Since(info.ModTime()) > 48*time.Hour {
			os.Remove(backup)
		}
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
