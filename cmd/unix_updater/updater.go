//go:build unix

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
	"runtime"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/agent"
)

const (
	tempUpdateDir    = "updates"
	mainServiceName  = "pbs-plus-agent"
	maxUpdateRetries = 3
	updateRetryDelay = 5 * time.Second
)

type PackageInfo struct {
	Format string
	Arch   string
	Ext    string
}

type ServiceManager struct {
	Type     string
	StopCmd  []string
	StartCmd []string
	CheckCmd []string
}

func (u *UpdaterService) getMainServiceVersion() (string, error) {
	version, err := u.readVersionFromFile()
	if err != nil {
		return "", fmt.Errorf("failed to read main service version: %w", err)
	}
	return version, nil
}

func (u *UpdaterService) detectPackageInfo() (*PackageInfo, error) {
	arch := runtime.GOARCH
	switch arch {
	case "amd64":
		arch = "amd64"
	case "arm64":
		arch = "arm64"
	case "arm":
		arch = "armhf"
	case "386":
		arch = "i386"
	default:
		return nil, fmt.Errorf("unsupported architecture: %s", arch)
	}

	// Detect package format based on available package managers
	if u.commandExists("dpkg") {
		return &PackageInfo{Format: "deb", Arch: arch, Ext: ".deb"}, nil
	}
	if u.commandExists("rpm") {
		return &PackageInfo{Format: "rpm", Arch: arch, Ext: ".rpm"}, nil
	}
	if u.commandExists("apk") {
		return &PackageInfo{Format: "apk", Arch: arch, Ext: ".apk"}, nil
	}
	if u.commandExists("opkg") {
		return &PackageInfo{Format: "ipk", Arch: arch, Ext: ".ipk"}, nil
	}

	// Fallback to binary if no package manager is detected
	return &PackageInfo{Format: "binary", Arch: arch, Ext: ""}, nil
}

func (u *UpdaterService) detectServiceManager() *ServiceManager {
	// Check for systemd (most common)
	if u.commandExists("systemctl") {
		return &ServiceManager{
			Type:     "systemd",
			StopCmd:  []string{"systemctl", "stop", mainServiceName},
			StartCmd: []string{"systemctl", "start", mainServiceName},
			CheckCmd: []string{"systemctl", "is-active", mainServiceName},
		}
	}

	// Check for OpenRC (Alpine, Gentoo)
	if u.commandExists("rc-service") {
		return &ServiceManager{
			Type:     "openrc",
			StopCmd:  []string{"rc-service", mainServiceName, "stop"},
			StartCmd: []string{"rc-service", mainServiceName, "start"},
			CheckCmd: []string{"rc-service", mainServiceName, "status"},
		}
	}

	// Check for SysV init (older systems)
	if u.commandExists("service") {
		return &ServiceManager{
			Type:     "sysv",
			StopCmd:  []string{"service", mainServiceName, "stop"},
			StartCmd: []string{"service", mainServiceName, "start"},
			CheckCmd: []string{"service", mainServiceName, "status"},
		}
	}

	// Check for runit (Void Linux)
	if u.commandExists("sv") {
		return &ServiceManager{
			Type:     "runit",
			StopCmd:  []string{"sv", "stop", mainServiceName},
			StartCmd: []string{"sv", "start", mainServiceName},
			CheckCmd: []string{"sv", "status", mainServiceName},
		}
	}

	// Check for s6 (some embedded systems)
	if u.commandExists("s6-svc") {
		servicePath := fmt.Sprintf("/var/service/%s", mainServiceName)
		return &ServiceManager{
			Type:     "s6",
			StopCmd:  []string{"s6-svc", "-d", servicePath},
			StartCmd: []string{"s6-svc", "-u", servicePath},
			CheckCmd: []string{"s6-svstat", servicePath},
		}
	}

	// Check for upstart (older Ubuntu)
	if u.commandExists("initctl") {
		return &ServiceManager{
			Type:     "upstart",
			StopCmd:  []string{"initctl", "stop", mainServiceName},
			StartCmd: []string{"initctl", "start", mainServiceName},
			CheckCmd: []string{"initctl", "status", mainServiceName},
		}
	}

	// Fallback - try to find and kill process directly
	return &ServiceManager{
		Type:     "manual",
		StopCmd:  []string{},
		StartCmd: []string{},
		CheckCmd: []string{},
	}
}

func (u *UpdaterService) commandExists(cmd string) bool {
	_, err := exec.LookPath(cmd)
	return err == nil
}

func (u *UpdaterService) getMainBinaryPath() (string, error) {
	ex, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("failed to get executable path: %w", err)
	}
	return filepath.Join(filepath.Dir(ex), "pbs-plus-agent"), nil
}

func (u *UpdaterService) ensureTempDir() (string, error) {
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

func (u *UpdaterService) downloadAndVerifyMD5(pkgInfo *PackageInfo) (string, error) {
	endpoint := fmt.Sprintf("/api2/json/plus/binary/checksum?format=%s&arch=%s",
		pkgInfo.Format, pkgInfo.Arch)

	resp, err := agent.ProxmoxHTTPRequest(http.MethodGet, endpoint, nil, nil)
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

func (u *UpdaterService) calculateMD5(filepath string) (string, error) {
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

func (u *UpdaterService) downloadUpdate(pkgInfo *PackageInfo) (string, error) {
	tempDir, err := u.ensureTempDir()
	if err != nil {
		return "", err
	}

	filename := fmt.Sprintf("update-%s%s",
		time.Now().Format("20060102150405"), pkgInfo.Ext)
	tempFile := filepath.Join(tempDir, filename)

	file, err := os.Create(tempFile)
	if err != nil {
		return "", fmt.Errorf("failed to create temporary file: %w", err)
	}
	defer file.Close()

	endpoint := fmt.Sprintf("/api2/json/plus/binary?format=%s&arch=%s",
		pkgInfo.Format, pkgInfo.Arch)

	resp, err := agent.ProxmoxHTTPRequest(http.MethodGet, endpoint, nil, nil)
	if err != nil {
		os.Remove(tempFile)
		return "", fmt.Errorf("failed to download update: %w", err)
	}
	defer resp.Close()

	if _, err := io.Copy(file, resp); err != nil {
		os.Remove(tempFile)
		return "", fmt.Errorf("failed to save update file: %w", err)
	}
	return tempFile, nil
}

func (u *UpdaterService) verifyUpdate(tempFile string, pkgInfo *PackageInfo) error {
	expectedMD5, err := u.downloadAndVerifyMD5(pkgInfo)
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

func (u *UpdaterService) stopMainServiceManual() error {
	// Find process by name
	cmd := exec.Command("pgrep", "-f", mainServiceName)
	output, err := cmd.Output()
	if err != nil {
		return nil // Process not running
	}

	pids := strings.Fields(strings.TrimSpace(string(output)))
	for _, pid := range pids {
		if pid != "" {
			killCmd := exec.Command("kill", "-TERM", pid)
			killCmd.Run()
		}
	}

	// Wait for processes to stop
	for i := 0; i < 10; i++ {
		cmd := exec.Command("pgrep", "-f", mainServiceName)
		if err := cmd.Run(); err != nil {
			return nil // No processes found
		}
		time.Sleep(1 * time.Second)
	}

	// Force kill if still running
	for _, pid := range pids {
		if pid != "" {
			killCmd := exec.Command("kill", "-KILL", pid)
			killCmd.Run()
		}
	}

	return nil
}

func (u *UpdaterService) startMainServiceManual() error {
	mainBinary, err := u.getMainBinaryPath()
	if err != nil {
		return err
	}

	// Start the service in background
	cmd := exec.Command(mainBinary)
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.Stdin = nil

	return cmd.Start()
}

func (u *UpdaterService) stopMainService() error {
	sm := u.detectServiceManager()

	if sm.Type == "manual" {
		return u.stopMainServiceManual()
	}

	cmd := exec.Command(sm.StopCmd[0], sm.StopCmd[1:]...)
	if err := cmd.Run(); err != nil {
		// If service command fails, try manual stop as fallback
		return u.stopMainServiceManual()
	}

	// Wait for service to stop
	for i := 0; i < 10; i++ {
		if sm.CheckCmd != nil && len(sm.CheckCmd) > 0 {
			cmd := exec.Command(sm.CheckCmd[0], sm.CheckCmd[1:]...)
			output, _ := cmd.CombinedOutput()
			outputStr := string(output)

			// Check for various "stopped" indicators
			if strings.Contains(outputStr, "inactive") ||
				strings.Contains(outputStr, "stopped") ||
				strings.Contains(outputStr, "down") ||
				strings.Contains(outputStr, "not running") {
				return nil
			}
		}
		time.Sleep(2 * time.Second)
	}

	// If we can't verify it stopped, try manual stop as final fallback
	return u.stopMainServiceManual()
}

func (u *UpdaterService) startMainService() error {
	sm := u.detectServiceManager()

	if sm.Type == "manual" {
		return u.startMainServiceManual()
	}

	cmd := exec.Command(sm.StartCmd[0], sm.StartCmd[1:]...)
	if err := cmd.Run(); err != nil {
		// If service command fails, try manual start as fallback
		return u.startMainServiceManual()
	}

	return nil
}

func (u *UpdaterService) installPackage(tempFile string, pkgInfo *PackageInfo) error {
	var cmd *exec.Cmd

	switch pkgInfo.Format {
	case "deb":
		cmd = exec.Command("dpkg", "-i", tempFile)
	case "rpm":
		cmd = exec.Command("rpm", "-U", tempFile)
	case "apk":
		cmd = exec.Command("apk", "add", "--allow-untrusted", tempFile)
	case "ipk":
		cmd = exec.Command("opkg", "install", tempFile)
	case "binary":
		return u.applyBinaryUpdate(tempFile)
	default:
		return fmt.Errorf("unsupported package format: %s", pkgInfo.Format)
	}

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to install package: %w", err)
	}
	return nil
}

func (u *UpdaterService) applyBinaryUpdate(tempFile string) error {
	mainBinary, err := u.getMainBinaryPath()
	if err != nil {
		return err
	}

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

	// Set executable permissions
	if err := os.Chmod(mainBinary, 0755); err != nil {
		os.Rename(backupPath, mainBinary)
		return fmt.Errorf("failed to set permissions: %w", err)
	}

	if err := u.startMainService(); err != nil {
		os.Rename(backupPath, mainBinary)
		return fmt.Errorf("failed to start service: %w", err)
	}

	os.Remove(backupPath)
	return nil
}

func (u *UpdaterService) applyUpdate(tempFile string, pkgInfo *PackageInfo) error {
	if pkgInfo.Format == "binary" {
		return u.applyBinaryUpdate(tempFile)
	}

	// For package formats, stop service, install package, start service
	if err := u.stopMainService(); err != nil {
		return fmt.Errorf("failed to stop service: %w", err)
	}

	if err := u.installPackage(tempFile, pkgInfo); err != nil {
		u.startMainService() // Try to restart even if install failed
		return fmt.Errorf("failed to install package: %w", err)
	}

	if err := u.startMainService(); err != nil {
		return fmt.Errorf("failed to start service: %w", err)
	}

	os.Remove(tempFile)
	return nil
}

func (u *UpdaterService) performUpdate() error {
	pkgInfo, err := u.detectPackageInfo()
	if err != nil {
		return fmt.Errorf("failed to detect package info: %w", err)
	}

	errors := []error{}

	for retry := 0; retry < maxUpdateRetries; retry++ {
		if retry > 0 {
			time.Sleep(updateRetryDelay)
		}
		if err := u.tryUpdate(pkgInfo); err == nil {
			return nil
		} else {
			errors = append(errors, err)
		}
	}
	return fmt.Errorf("all update attempts failed: %v", errors)
}

func (u *UpdaterService) tryUpdate(pkgInfo *PackageInfo) error {
	tempFile, err := u.downloadUpdate(pkgInfo)
	if err != nil {
		return err
	}
	defer os.Remove(tempFile)

	if err := u.verifyUpdate(tempFile, pkgInfo); err != nil {
		return err
	}

	return u.applyUpdate(tempFile, pkgInfo)
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

	for _, entry := range entries {
		if !entry.IsDir() {
			path := filepath.Join(tempDir, entry.Name())
			info, err := entry.Info()
			if err == nil && time.Since(info.ModTime()) > 24*time.Hour {
				os.Remove(path)
			}
		}
	}

	// Clean up backup files
	ex, err := os.Executable()
	if err != nil {
		return err
	}

	backups, _ := filepath.Glob(filepath.Join(filepath.Dir(ex), "*.backup"))
	for _, backup := range backups {
		info, _ := os.Stat(backup)
		if info != nil && time.Since(info.ModTime()) > 48*time.Hour {
			os.Remove(backup)
		}
	}
	return nil
}
