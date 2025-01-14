//go:build windows
// +build windows

package main

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/kardianos/service"
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
