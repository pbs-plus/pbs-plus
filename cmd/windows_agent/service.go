//go:build windows
// +build windows

package main

import (
	"bytes"
	"context"
	"crypto/md5"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/kardianos/service"
	"github.com/minio/selfupdate"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/controllers"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/sftp"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/websockets"
	"golang.org/x/sys/windows/registry"
)

type PingData struct {
	Pong bool `json:"pong"`
}

type PingResp struct {
	Data PingData `json:"data"`
}

type VersionResp struct {
	Version string `json:"version"`
}

type AgentDrivesRequest struct {
	Hostname     string   `json:"hostname"`
	DriveLetters []string `json:"drive_letters"`
}

type agentService struct {
	svc    service.Service
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	logger *syslog.Logger
}

const (
	updateRetryDelay = 5 * time.Second
	maxUpdateRetries = 3
	tempUpdateDir    = "updates"
)

func (p *agentService) Start(s service.Service) error {
	var err error
	p.svc = s
	p.ctx, p.cancel = context.WithCancel(context.Background())

	p.logger, err = syslog.InitializeLogger(s)
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}

	p.wg.Add(2)
	go func() {
		defer p.wg.Done()
		p.versionCheck()
	}()
	go func() {
		defer p.wg.Done()
		p.run()
	}()

	return nil
}

func (p *agentService) Stop(s service.Service) error {
	p.cancel()
	p.wg.Wait()
	return nil
}

func (p *agentService) ensureTempDir() (string, error) {
	ex, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("failed to get executable path: %w", err)
	}

	tempDir := filepath.Join(filepath.Dir(ex), tempUpdateDir)
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create temp directory: %w", err)
	}

	return tempDir, nil
}

func (p *agentService) downloadAndVerifyMD5() (string, error) {
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

func (p *agentService) calculateMD5(filepath string) (string, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return "", fmt.Errorf("failed to open file for MD5 calculation: %w", err)
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("failed to calculate MD5: %w", err)
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

func (p *agentService) downloadUpdate() (string, io.ReadCloser, error) {
	tempDir, err := p.ensureTempDir()
	if err != nil {
		return "", nil, err
	}

	tempFile := filepath.Join(tempDir, fmt.Sprintf("update-%s.tmp", time.Now().Format("20060102150405")))
	file, err := os.Create(tempFile)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create temporary file: %w", err)
	}

	dlResp, err := agent.ProxmoxHTTPRequest(http.MethodGet, "/api2/json/plus/binary", nil, nil)
	if err != nil {
		file.Close()
		os.Remove(tempFile)
		return "", nil, fmt.Errorf("failed to download update: %w", err)
	}

	return tempFile, dlResp, nil
}

func (p *agentService) verifyAndApplyUpdate(tempFile string) error {
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

	ex, err := os.Executable()
	if err != nil {
		return fmt.Errorf("failed to get executable path: %w", err)
	}

	backupPath := ex + ".backup"
	if err := os.Link(ex, backupPath); err != nil && !os.IsExist(err) {
		p.logger.Errorf("Failed to create backup: %v", err)
	}

	updateFile, err := os.Open(tempFile)
	if err != nil {
		return fmt.Errorf("failed to open update file: %w", err)
	}
	defer updateFile.Close()

	if err := selfupdate.Apply(updateFile, selfupdate.Options{}); err != nil {
		if backupErr := os.Rename(backupPath, ex); backupErr != nil {
			p.logger.Errorf("Failed to restore backup after failed update: %v", backupErr)
		}
		return fmt.Errorf("failed to apply update: %w", err)
	}

	os.Remove(backupPath)
	os.Remove(tempFile)

	return nil
}

func (p *agentService) performUpdate() error {
	tempFile, dlResp, err := p.downloadUpdate()
	if err != nil {
		return err
	}
	defer func() {
		if dlResp != nil {
			_, _ = io.Copy(io.Discard, dlResp)
			dlResp.Close()
		}
	}()

	file, err := os.OpenFile(tempFile, os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open temp file: %w", err)
	}

	if _, err := io.Copy(file, dlResp); err != nil {
		file.Close()
		os.Remove(tempFile)
		return fmt.Errorf("failed to save update file: %w", err)
	}
	file.Close()

	if err := p.verifyAndApplyUpdate(tempFile); err != nil {
		os.Remove(tempFile)
		return err
	}

	ex, err := os.Executable()
	if err != nil {
		return fmt.Errorf("failed to get executable path: %w", err)
	}

	restartCmd := exec.Command(ex, "restart")
	return restartCmd.Start()
}

func (p *agentService) cleanupOldUpdates() error {
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
				p.logger.Errorf("Failed to get file info for %s: %v", path, err)
				continue
			}

			if time.Since(info.ModTime()) > 24*time.Hour {
				if err := os.Remove(path); err != nil {
					p.logger.Errorf("Failed to remove old update file %s: %v", path, err)
				}
			}
		}
	}

	return nil
}

func (p *agentService) versionCheck() {
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()

	if err := p.cleanupOldUpdates(); err != nil {
		p.logger.Errorf("Failed to clean up old updates: %v", err)
	}

	checkAndUpdate := func() {
		var versionResp VersionResp
		_, err := agent.ProxmoxHTTPRequest(http.MethodGet, "/api2/json/plus/version", nil, &versionResp)
		if err != nil {
			p.logger.Errorf("Version check failed: %v", err)
			return
		}

		if versionResp.Version != Version {
			p.logger.Infof("New version %s available, current version: %s", versionResp.Version, Version)

			for retry := 0; retry < maxUpdateRetries; retry++ {
				if err := p.performUpdate(); err != nil {
					p.logger.Errorf("Update attempt %d failed: %v", retry+1, err)
					time.Sleep(updateRetryDelay)
					continue
				}
				p.logger.Infof("Successfully updated to version %s", versionResp.Version)
				return
			}
			p.logger.Errorf("Failed to update after %d attempts", maxUpdateRetries)
		}
	}

	checkAndUpdate()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			checkAndUpdate()
		}
	}
}

func (p *agentService) run() {
	agent.SetStatus("Starting")
	if err := p.waitForServerURL(); err != nil {
		p.logger.Errorf("Failed waiting for server URL: %v", err)
		return
	}

	if err := p.initializeDrives(); err != nil {
		p.logger.Errorf("Failed to initialize drives: %v", err)
		return
	}

	infoChan := make(chan string, 100)
	errChan := make(chan string, 100)
	defer close(infoChan)
	defer close(errChan)

	go p.handleLogs(infoChan, errChan)

	if err := p.connectWebSocket(infoChan, errChan); err != nil {
		p.logger.Errorf("WebSocket connection failed: %v", err)
		return
	}

	<-p.ctx.Done()
}

func (p *agentService) waitForServerURL() error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		key, err := registry.OpenKey(registry.LOCAL_MACHINE, `Software\PBSPlus\Config`, registry.QUERY_VALUE)
		if err == nil {
			defer key.Close()
			if serverUrl, _, err := key.GetStringValue("ServerURL"); err == nil && serverUrl != "" {
				return nil
			}
		}

		select {
		case <-p.ctx.Done():
			return fmt.Errorf("context cancelled while waiting for server URL")
		case <-ticker.C:
			continue
		}
	}
}

func (p *agentService) initializeDrives() error {
	drives := getLocalDrives()
	driveLetters := make([]string, 0, len(drives))

	for _, drive := range drives {
		driveLetters = append(driveLetters, drive)
		if err := sftp.InitializeSFTPConfig(drive); err != nil {
			return fmt.Errorf("failed to initialize SFTP config for drive %s: %w", drive, err)
		}
	}

	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("failed to get hostname: %w", err)
	}

	reqBody, err := json.Marshal(&AgentDrivesRequest{
		Hostname:     hostname,
		DriveLetters: driveLetters,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal drive request: %w", err)
	}

	resp, err := agent.ProxmoxHTTPRequest(
		http.MethodPost,
		"/api2/json/d2d/target/agent",
		bytes.NewBuffer(reqBody),
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to update agent drives: %w", err)
	}
	defer resp.Close()
	_, _ = io.Copy(io.Discard, resp)

	return nil
}

func (p *agentService) handleLogs(infoChan, errChan chan string) {
	for {
		select {
		case <-p.ctx.Done():
			return
		case info := <-infoChan:
			p.logger.Info(info)
		case err := <-errChan:
			p.logger.Errorf("SFTP error: %s", err)
		}
	}
}

func (p *agentService) connectWebSocket(infoChan, errChan chan string) error {
	for {
		_, err := websockets.NewWSClient(func(c *websocket.Conn, m websockets.Message) {
			controllers.WSHandler(p.ctx, c, m, infoChan, errChan)
		})
		if err != nil {
			p.logger.Errorf("WS connection error: %s", err)
			select {
			case <-p.ctx.Done():
				return fmt.Errorf("context cancelled while connecting to WebSocket")
			case <-time.After(5 * time.Second):
				continue
			}
		}
		break
	}

	return nil
}
