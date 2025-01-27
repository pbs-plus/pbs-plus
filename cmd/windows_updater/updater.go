//go:build windows

package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
)

const (
	tempUpdateDir    = "updates"
	mainServiceName  = "PBSPlusAgent"
	mainBinaryName   = "pbs-plus-agent.exe"
	maxUpdateRetries = 3
	updateRetryDelay = 5 * time.Second
)

type VersionResp struct {
	Version string `json:"version"`
}

func (p *UpdaterService) getMainBinaryPath() (string, error) {
	ex, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("failed to get executable path: %w", err)
	}

	return filepath.Join(filepath.Dir(ex), mainBinaryName), nil
}

func (p *UpdaterService) getMainServiceVersion() (string, error) {
	mainBinary, err := p.getMainBinaryPath()
	if err != nil {
		return "", err
	}

	cmd := exec.Command(mainBinary, "version")
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to get main service version: %w", err)
	}

	return strings.TrimSpace(string(output)), nil
}

func (p *UpdaterService) ensureTempDir() (string, error) {
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

func (p *UpdaterService) downloadAndVerifyMD5() (string, error) {
	md5Resp, err := agent.ProxmoxHTTPRequest(
		http.MethodGet,
		"/api2/json/plus/binary/checksum",
		nil,
		nil,
	)
	if err != nil {
		return "", fmt.Errorf("failed to download MD5: %w", err)
	}
	defer md5Resp.Close()

	md5Bytes, err := io.ReadAll(md5Resp)
	if err != nil {
		return "", fmt.Errorf("failed to read MD5: %w", err)
	}

	return strings.TrimSpace(string(md5Bytes)), nil
}

func (p *UpdaterService) calculateMD5(filepath string) (string, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return "", fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("failed to calculate MD5: %w", err)
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

func (p *UpdaterService) downloadUpdate() (string, error) {
	syslog.L.Infof("Starting update download process")
	tempDir, err := p.ensureTempDir()
	if err != nil {
		syslog.L.Errorf("Failed to create temp directory: %v", err)
		return "", err
	}
	syslog.L.Infof("Created temporary directory at: %s", tempDir)

	tempFile := filepath.Join(tempDir, fmt.Sprintf("update-%s.tmp", time.Now().Format("20060102150405")))
	file, err := os.Create(tempFile)
	if err != nil {
		syslog.L.Errorf("Failed to create temporary file: %v", err)
		return "", fmt.Errorf("failed to create temporary file: %w", err)
	}
	syslog.L.Infof("Created temporary file for update at: %s", tempFile)

	dlResp, err := agent.ProxmoxHTTPRequest(http.MethodGet, "/api2/json/plus/binary", nil, nil)
	if err != nil {
		file.Close()
		os.Remove(tempFile)
		syslog.L.Errorf("Failed to download update: %v", err)
		return "", fmt.Errorf("failed to download update: %w", err)
	}
	syslog.L.Infof("Successfully initiated download from update server")

	defer func() {
		if dlResp != nil {
			_, _ = io.Copy(io.Discard, dlResp)
			dlResp.Close()
		}
	}()

	if _, err := io.Copy(file, dlResp); err != nil {
		file.Close()
		os.Remove(tempFile)
		syslog.L.Errorf("Failed to save update file: %v", err)
		return "", fmt.Errorf("failed to save update file: %w", err)
	}
	file.Close()
	syslog.L.Infof("Successfully downloaded and saved update file")

	return tempFile, nil
}

func (p *UpdaterService) verifyUpdate(tempFile string) error {
	syslog.L.Infof("Starting update verification process")

	expectedMD5, err := p.downloadAndVerifyMD5()
	if err != nil {
		syslog.L.Errorf("Failed to download expected MD5: %v", err)
		return fmt.Errorf("failed to get expected MD5: %w", err)
	}
	syslog.L.Infof("Downloaded expected MD5 checksum: %s", expectedMD5)

	actualMD5, err := p.calculateMD5(tempFile)
	if err != nil {
		syslog.L.Errorf("Failed to calculate actual MD5: %v", err)
		return fmt.Errorf("failed to calculate actual MD5: %w", err)
	}
	syslog.L.Infof("Calculated actual MD5 checksum: %s", actualMD5)

	if !strings.EqualFold(actualMD5, expectedMD5) {
		syslog.L.Errorf("MD5 mismatch: expected %s, got %s", expectedMD5, actualMD5)
		return fmt.Errorf("MD5 mismatch: expected %s, got %s", expectedMD5, actualMD5)
	}
	syslog.L.Infof("MD5 verification successful")

	return nil
}

func (p *UpdaterService) stopMainService() error {
	syslog.L.Infof("Attempting to stop main service: %s", mainServiceName)

	isStopped, err := p.isServiceStopped()
	if err == nil && isStopped {
		syslog.L.Infof("Service is already stopped")
		return nil
	}

	cmd := exec.Command("sc", "stop", mainServiceName)
	if err := cmd.Run(); err != nil {
		syslog.L.Errorf("Failed to stop service: %v", err)
		return err
	}
	syslog.L.Infof("Stop command sent to service")

	// Poll until stopped
	for i := 0; i < 10; i++ {
		cmd := exec.Command("sc", "query", mainServiceName)
		output, _ := cmd.CombinedOutput()
		if strings.Contains(string(output), "STOPPED") {
			syslog.L.Infof("Service successfully stopped")
			return nil
		}
		syslog.L.Infof("Waiting for service to stop... (attempt %d/10)", i+1)
		time.Sleep(2 * time.Second)
	}
	syslog.L.Errorf("Timeout waiting for service to stop")
	return fmt.Errorf("timeout waiting for service to stop")
}

func (p *UpdaterService) startMainService() error {
	syslog.L.Infof("Attempting to start main service: %s", mainServiceName)
	startCmd := exec.Command("sc", "start", mainServiceName)
	err := startCmd.Run()
	if err != nil {
		syslog.L.Errorf("Failed to start service: %v", err)
		return err
	}
	syslog.L.Infof("Service start command executed successfully")
	return nil
}

func (p *UpdaterService) applyUpdate(tempFile string) error {
	syslog.L.Infof("Starting update application process")

	mainBinary, err := p.getMainBinaryPath()
	if err != nil {
		syslog.L.Errorf("Failed to get main binary path: %v", err)
		return err
	}
	syslog.L.Infof("Main binary path: %s", mainBinary)

	backupPath := mainBinary + ".backup"
	syslog.L.Infof("Creating backup at: %s", backupPath)

	if err := p.stopMainService(); err != nil {
		syslog.L.Errorf("Failed to stop service before update: %v", err)
		return fmt.Errorf("failed to stop service: %w", err)
	}

	if err := os.Rename(mainBinary, backupPath); err != nil && !os.IsExist(err) {
		syslog.L.Errorf("Failed to create backup: %v", err)
	} else {
		syslog.L.Infof("Successfully created backup of current binary")
	}

	if err := os.Rename(tempFile, mainBinary); err != nil && !os.IsExist(err) {
		syslog.L.Errorf("Failed to copy update file: %v", err)
		if backupErr := os.Rename(backupPath, mainBinary); backupErr != nil {
			syslog.L.Errorf("Failed to restore backup after failed update: %v", backupErr)
		} else {
			syslog.L.Infof("Successfully restored backup after failed update")
		}
		return fmt.Errorf("failed to copy update file: %w", err)
	}
	syslog.L.Infof("Successfully replaced binary with update")

	if err := p.startMainService(); err != nil {
		syslog.L.Errorf("Failed to start service after update: %v", err)
		if backupErr := os.Rename(backupPath, mainBinary); backupErr != nil {
			syslog.L.Errorf("Failed to restore backup after failed service start: %v", backupErr)
		} else {
			syslog.L.Infof("Successfully restored backup after failed service start")
		}
		if startErr := p.startMainService(); startErr != nil {
			syslog.L.Errorf("Failed to start service after restore: %v", startErr)
		}
		return fmt.Errorf("failed to start service: %w", err)
	}
	syslog.L.Infof("Service successfully restarted with new binary")

	if err := os.Remove(backupPath); err != nil {
		syslog.L.Errorf("Failed to remove backup file: %v", err)
	} else {
		syslog.L.Infof("Successfully removed backup file")
	}

	if err := os.Remove(tempFile); err != nil {
		syslog.L.Errorf("Failed to remove temporary update file: %v", err)
	} else {
		syslog.L.Infof("Successfully removed temporary update file")
	}

	return nil
}

func (p *UpdaterService) performUpdate() error {
	syslog.L.Infof("Starting update process with maximum %d retries", maxUpdateRetries)
	var err error
	for retry := 0; retry < maxUpdateRetries; retry++ {
		if retry > 0 {
			syslog.L.Infof("Retrying update (attempt %d/%d)", retry+1, maxUpdateRetries)
			time.Sleep(updateRetryDelay)
		}
		if err = p.tryUpdate(); err == nil {
			syslog.L.Infof("Update completed successfully")
			return nil
		}
		syslog.L.Errorf("Update attempt %d failed: %v", retry+1, err)
	}
	syslog.L.Errorf("All update attempts failed after %d retries", maxUpdateRetries)
	return err
}

func (p *UpdaterService) tryUpdate() error {
	tempFile, err := p.downloadUpdate()
	if err != nil {
		return err
	}

	if err := p.verifyUpdate(tempFile); err != nil {
		os.Remove(tempFile)
		return err
	}

	if err := p.applyUpdate(tempFile); err != nil {
		os.Remove(tempFile)
		return err
	}

	return nil
}

func (p *UpdaterService) cleanupOldUpdates() error {
	syslog.L.Infof("Starting cleanup of old updates")
	tempDir, err := p.ensureTempDir()
	if err != nil {
		syslog.L.Errorf("Failed to access temp directory: %v", err)
		return err
	}

	entries, err := os.ReadDir(tempDir)
	if err != nil {
		syslog.L.Errorf("Failed to read temp directory: %v", err)
		return fmt.Errorf("failed to read temp directory: %w", err)
	}

	var deletedFiles int
	for _, entry := range entries {
		if !entry.IsDir() {
			path := filepath.Join(tempDir, entry.Name())
			info, err := entry.Info()
			if err != nil {
				syslog.L.Errorf("Failed to get file info for %s: %v", path, err)
				continue
			}

			if time.Since(info.ModTime()) > 24*time.Hour {
				if err := os.Remove(path); err != nil {
					syslog.L.Errorf("Failed to remove old file %s: %v", path, err)
				} else {
					deletedFiles++
					syslog.L.Infof("Removed old temporary file: %s", path)
				}
			}
		}
	}

	backupGlob := filepath.Join(tempDir, "*.backup")
	backups, _ := filepath.Glob(backupGlob)
	var deletedBackups int
	for _, backup := range backups {
		info, _ := os.Stat(backup)
		if time.Since(info.ModTime()) > 48*time.Hour {
			if err := os.Remove(backup); err != nil {
				syslog.L.Errorf("Failed to remove old backup %s: %v", backup, err)
			} else {
				deletedBackups++
				syslog.L.Infof("Removed old backup file: %s", backup)
			}
		}
	}

	syslog.L.Infof("Cleanup completed: removed %d temporary files and %d backup files", deletedFiles, deletedBackups)
	return nil
}

func (p *UpdaterService) isServiceStopped() (bool, error) {
	cmd := exec.Command("sc", "query", mainServiceName)
	output, err := cmd.Output()
	if err != nil {
		return false, err
	}
	return strings.Contains(string(output), "STOPPED"), nil
}
