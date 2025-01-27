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

	cmd := exec.Command(mainBinary, "--version")
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
	tempDir, err := p.ensureTempDir()
	if err != nil {
		return "", err
	}

	tempFile := filepath.Join(tempDir, fmt.Sprintf("update-%s.tmp", time.Now().Format("20060102150405")))
	file, err := os.Create(tempFile)
	if err != nil {
		return "", fmt.Errorf("failed to create temporary file: %w", err)
	}

	dlResp, err := agent.ProxmoxHTTPRequest(http.MethodGet, "/api2/json/plus/binary", nil, nil)
	if err != nil {
		file.Close()
		os.Remove(tempFile)
		return "", fmt.Errorf("failed to download update: %w", err)
	}
	defer func() {
		if dlResp != nil {
			_, _ = io.Copy(io.Discard, dlResp)
			dlResp.Close()
		}
	}()

	if _, err := io.Copy(file, dlResp); err != nil {
		file.Close()
		os.Remove(tempFile)
		return "", fmt.Errorf("failed to save update file: %w", err)
	}
	file.Close()

	return tempFile, nil
}

func (p *UpdaterService) verifyUpdate(tempFile string) error {
	expectedMD5, err := p.downloadAndVerifyMD5()
	if err != nil {
		return fmt.Errorf("failed to get expected MD5: %w", err)
	}

	actualMD5, err := p.calculateMD5(tempFile)
	if err != nil {
		return fmt.Errorf("failed to calculate actual MD5: %w", err)
	}

	if !strings.EqualFold(actualMD5, expectedMD5) {
		return fmt.Errorf("MD5 mismatch: expected %s, got %s", expectedMD5, actualMD5)
	}

	return nil
}

func (p *UpdaterService) stopMainService() error {
	isStopped, err := p.isServiceStopped()
	if err == nil && isStopped {
		return nil
	}

	cmd := exec.Command("sc", "stop", mainServiceName)
	if err := cmd.Run(); err != nil {
		return err
	}

	// Poll until stopped
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

func (p *UpdaterService) startMainService() error {
	startCmd := exec.Command("sc", "start", mainServiceName)
	return startCmd.Run()
}

func (p *UpdaterService) applyUpdate(tempFile string) error {
	mainBinary, err := p.getMainBinaryPath()
	if err != nil {
		return err
	}

	backupPath := mainBinary + ".backup"

	// Stop service before update
	if err := p.stopMainService(); err != nil {
		return fmt.Errorf("failed to stop service: %w", err)
	}

	// Create backup of current binary
	if err := os.Rename(mainBinary, backupPath); err != nil && !os.IsExist(err) {
		syslog.L.Errorf("Failed to create backup: %v", err)
	}

	// Do actual update
	if err := os.Rename(tempFile, mainBinary); err != nil && !os.IsExist(err) {
		if backupErr := os.Rename(backupPath, mainBinary); backupErr != nil {
			syslog.L.Errorf("Failed to restore backup after failed update: %v", backupErr)
		}
		return fmt.Errorf("failed to copy update file: %w", err)
	}

	// Start service after update
	if err := p.startMainService(); err != nil {
		syslog.L.Errorf("Failed to start service after update: %v", err)
		// Attempt to restore backup and restart service
		if backupErr := os.Rename(backupPath, mainBinary); backupErr != nil {
			syslog.L.Errorf("Failed to restore backup after failed service start: %v", backupErr)
		}
		if startErr := p.startMainService(); startErr != nil {
			syslog.L.Errorf("Failed to start service after restore: %v", startErr)
		}
		return fmt.Errorf("failed to start service: %w", err)
	}

	os.Remove(backupPath)
	os.Remove(tempFile)

	return nil
}

func (p *UpdaterService) performUpdate() error {
	var err error
	for retry := 0; retry < maxUpdateRetries; retry++ {
		if retry > 0 {
			time.Sleep(updateRetryDelay)
		}
		if err = p.tryUpdate(); err == nil {
			return nil
		}
	}
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
	tempDir, err := p.ensureTempDir()
	if err != nil {
		return err
	}

	entries, err := os.ReadDir(tempDir)
	if err != nil {
		return fmt.Errorf("failed to read temp directory: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			path := filepath.Join(tempDir, entry.Name())
			info, err := entry.Info()
			if err != nil {
				continue
			}

			if time.Since(info.ModTime()) > 24*time.Hour {
				_ = os.Remove(path)
			}
		}
	}

	backupGlob := filepath.Join(tempDir, "*.backup")
	backups, _ := filepath.Glob(backupGlob)
	for _, backup := range backups {
		info, _ := os.Stat(backup)
		if time.Since(info.ModTime()) > 48*time.Hour {
			os.Remove(backup)
		}
	}

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
