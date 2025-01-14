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
}

const (
	updateRetryDelay = 5 * time.Second
	maxUpdateRetries = 3
	tempUpdateDir    = "updates"
)

func (p *agentService) Start(s service.Service) error {
	p.svc = s
	p.ctx, p.cancel = context.WithCancel(context.Background())

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
		syslog.L.Errorf("Failed waiting for server URL: %v", err)
		return
	}

	if err := p.initializeDrives(); err != nil {
		syslog.L.Errorf("Failed to initialize drives: %v", err)
		return
	}

	if err := p.connectWebSocket(); err != nil {
		syslog.L.Errorf("WebSocket connection failed: %v", err)
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

func (p *agentService) connectWebSocket() error {
	for {
		_, err := websockets.NewWSClient(func(c *websocket.Conn, m websockets.Message) {
			controllers.WSHandler(p.ctx, c, m)
		})
		if err != nil {
			syslog.L.Errorf("WS connection error: %s", err)
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
