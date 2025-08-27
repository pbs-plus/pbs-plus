//go:build windows

package main

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/agent"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

const (
	tempUpdateDir    = "updates"
	mainServiceName  = "PBSPlusAgent"
	mainBinaryName   = "pbs-plus-agent.exe"
	maxUpdateRetries = 3
	updateRetryDelay = 5 * time.Second
)

var ErrEmptyVersion = errors.New("version file is empty")

func (u *UpdaterService) getMainServiceVersion() (string, error) {
	return u.readVersionFromFile()
}

func (u *UpdaterService) getMainBinaryPath() (string, error) {
	ex, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("failed to get executable path: %w", err)
	}
	return filepath.Join(filepath.Dir(ex), mainBinaryName), nil
}

func (u *UpdaterService) ensureTempDir() (string, error) {
	ex, err := os.Executable()
	if err != nil {
		return "", err
	}
	tempDir := filepath.Join(filepath.Dir(ex), tempUpdateDir)
	if err := os.MkdirAll(tempDir, 0o755); err != nil {
		return "", err
	}
	return tempDir, nil
}

func (u *UpdaterService) downloadAndVerifyMD5() (string, error) {
	resp, err := agent.ProxmoxHTTPRequest(
		http.MethodGet,
		"/api2/json/plus/binary/checksum",
		nil,
		nil,
	)
	if err != nil {
		return "", fmt.Errorf("failed to download MD5: %w", err)
	}
	defer resp.Close()

	md5Bytes, err := io.ReadAll(resp)
	if err != nil {
		return "", fmt.Errorf("failed to read MD5: %w", err)
	}
	return strings.TrimSpace(string(md5Bytes)), nil
}

func (u *UpdaterService) calculateMD5(path string) (string, error) {
	file, err := os.Open(path)
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

func (u *UpdaterService) downloadUpdate() (string, error) {
	tempDir, err := u.ensureTempDir()
	if err != nil {
		return "", err
	}

	tempFile := filepath.Join(
		tempDir,
		fmt.Sprintf("update-%s.tmp", time.Now().Format("20060102150405")),
	)

	file, err := os.Create(tempFile)
	if err != nil {
		return "", fmt.Errorf("failed to create temporary file: %w", err)
	}
	defer file.Close()

	resp, err := agent.ProxmoxHTTPRequest(
		http.MethodGet,
		"/api2/json/plus/binary",
		nil,
		nil,
	)
	if err != nil {
		_ = os.Remove(tempFile)
		return "", fmt.Errorf("failed to download update: %w", err)
	}
	defer resp.Close()

	if _, err := io.Copy(file, resp); err != nil {
		_ = os.Remove(tempFile)
		return "", fmt.Errorf("failed to save update file: %w", err)
	}

	return tempFile, nil
}

func (u *UpdaterService) verifyUpdate(tempFile string) error {
	expectedMD5, err := u.downloadAndVerifyMD5()
	if err != nil {
		return fmt.Errorf("failed to get expected MD5: %w", err)
	}

	actualMD5, err := u.calculateMD5(tempFile)
	if err != nil {
		return fmt.Errorf("failed to calculate actual MD5: %w", err)
	}

	if !strings.EqualFold(actualMD5, expectedMD5) {
		return fmt.Errorf("MD5 mismatch: expected %s, got %s", expectedMD5, actualMD5)
	}
	return nil
}

func (u *UpdaterService) performUpdate() error {
	var lastErr error
	for retry := 0; retry < maxUpdateRetries; retry++ {
		if retry > 0 {
			time.Sleep(updateRetryDelay)
		}
		if err := u.tryUpdateOnce(); err != nil {
			lastErr = err
			syslog.L.Error(err).
				WithMessage("update attempt failed").
				WithField("attempt", retry+1).
				Write()
			continue
		}
		return nil
	}
	if lastErr == nil {
		lastErr = errors.New("update failed (unknown error)")
	}
	return fmt.Errorf("all update attempts failed: %w", lastErr)
}

func (u *UpdaterService) tryUpdateOnce() error {
	tempFile, err := u.downloadUpdate()
	if err != nil {
		return err
	}
	defer os.Remove(tempFile)

	if err := u.verifyUpdate(tempFile); err != nil {
		return err
	}

	return u.applyUpdate(tempFile)
}

func (u *UpdaterService) applyUpdate(tempFile string) error {
	mainBinary, err := u.getMainBinaryPath()
	if err != nil {
		return err
	}

	backupPath := mainBinary + ".backup"

	if err := stopAndKillService(mainServiceName); err != nil {
		return fmt.Errorf("failed to stop service: %w", err)
	}

	// Backup existing binary if present.
	if _, err := os.Stat(mainBinary); err == nil {
		if err := os.Rename(mainBinary, backupPath); err != nil && !os.IsExist(err) {
			return fmt.Errorf("failed to create backup: %w", err)
		}
	}

	// Replace with new binary.
	if err := os.Rename(tempFile, mainBinary); err != nil {
		_ = os.Rename(backupPath, mainBinary)
		return fmt.Errorf("failed to replace binary: %w", err)
	}

	// If service binary path differs, uninstall the existing service.
	uninstalled, err := uninstallServiceIfDifferent(mainServiceName, mainBinary)
	if err != nil {
		_ = os.Rename(backupPath, mainBinary)
		return fmt.Errorf("check/uninstall existing service: %w", err)
	}

	installed, err := isServiceInstalled(mainServiceName)
	if err != nil {
		_ = os.Rename(backupPath, mainBinary)
		return fmt.Errorf("check service installed: %w", err)
	}

	if installed && !uninstalled {
		syslog.L.Info().WithMessage("Service already installed; attempting to start").Write()
		if err := scStart(mainServiceName); err != nil {
			syslog.L.Error(err).WithMessage("Start failed, reinstalling service").Write()
			if err := installService(mainBinary); err != nil {
				_ = os.Rename(backupPath, mainBinary)
				return fmt.Errorf("reinstall service: %w", err)
			}
			time.Sleep(2 * time.Second)
			if err := scStart(mainServiceName); err != nil {
				syslog.L.Error(err).
					WithMessage("Warning: service installed but failed to start automatically").
					Write()
			}
		}
	} else {
		syslog.L.Info().WithMessage("Installing PBS Plus Agent service").Write()
		if err := installService(mainBinary); err != nil {
			_ = os.Rename(backupPath, mainBinary)
			return fmt.Errorf("install service: %w", err)
		}
		time.Sleep(2 * time.Second)
		if err := scStart(mainServiceName); err != nil {
			syslog.L.Error(err).
				WithMessage("Warning: service installed but failed to start automatically").
				Write()
		}
	}

	_ = os.Remove(backupPath)
	_ = os.Remove(tempFile)
	return nil
}

func (u *UpdaterService) cleanupOldUpdates() error {
	tempDir, err := u.ensureTempDir()
	if err != nil {
		return err
	}

	entries, err := os.ReadDir(tempDir)
	if err != nil {
		return fmt.Errorf("failed to read temp directory: %w", err)
	}

	cutoff := time.Now().Add(-24 * time.Hour)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		path := filepath.Join(tempDir, entry.Name())
		info, err := entry.Info()
		if err == nil && info.ModTime().Before(cutoff) {
			_ = os.Remove(path)
		}
	}

	// Remove stale .backup files next to main binary (if any were left)
	mainBinary, err := u.getMainBinaryPath()
	if err == nil {
		backup := mainBinary + ".backup"
		if fi, err := os.Stat(backup); err == nil {
			if time.Since(fi.ModTime()) > 48*time.Hour {
				_ = os.Remove(backup)
			}
		}
	}

	return nil
}
