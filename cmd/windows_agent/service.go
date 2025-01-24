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

	"github.com/kardianos/service"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/controllers"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/registry"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
	"github.com/sonroyaalmerol/pbs-plus/internal/websockets"
)

type PingData struct {
	Pong bool `json:"pong"`
}

type PingResp struct {
	Data PingData `json:"data"`
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

func (p *agentService) Start(s service.Service) error {
	p.svc = s
	p.ctx, p.cancel = context.WithCancel(context.Background())

	p.wg.Add(1)
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

	if err := p.waitForBootstrap(); err != nil {
		syslog.L.Errorf("Failed waiting for bootstrap: %v", err)
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
		entry, err := registry.GetEntry(registry.CONFIG, "ServerURL", false)
		if err == nil && entry != nil {
			return nil
		}

		select {
		case <-p.ctx.Done():
			return fmt.Errorf("context cancelled while waiting for server URL")
		case <-ticker.C:
			continue
		}
	}
}

func (p *agentService) waitForBootstrap() error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		serverCA, _ := registry.GetEntry(registry.AUTH, "ServerCA", true)
		cert, _ := registry.GetEntry(registry.AUTH, "Cert", true)
		priv, _ := registry.GetEntry(registry.AUTH, "Priv", true)

		if serverCA != nil && cert != nil && priv != nil {
			return nil
		} else {
			err := agent.Bootstrap()
			syslog.L.Errorf("Bootstrap error: %v", err)
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
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("failed to get hostname: %w", err)
	}

	reqBody, err := json.Marshal(&AgentDrivesRequest{
		Hostname:     hostname,
		DriveLetters: utils.GetLocalDrives(),
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
		config, err := websockets.GetWindowsConfig()
		if err != nil {
			syslog.L.Errorf("WS client windows config error: %s", err)
			return err
		}

		tlsConfig, err := agent.GetTLSConfig()
		if err != nil {
			syslog.L.Errorf("WS client tls config error: %s", err)
			return err
		}

		client, err := websockets.NewWSClient(p.ctx, config, tlsConfig)
		if err != nil {
			syslog.L.Errorf("WS client init error: %s", err)
			select {
			case <-p.ctx.Done():
				return fmt.Errorf("context cancelled while connecting to WebSocket")
			case <-time.After(5 * time.Second):
				continue
			}
		}

		err = client.Connect()
		if err != nil {
			syslog.L.Errorf("WS client connect error: %s", err)
			select {
			case <-p.ctx.Done():
				return fmt.Errorf("context cancelled while connecting to WebSocket")
			case <-time.After(5 * time.Second):
				continue
			}
		}

		client.RegisterHandler("backup_start", controllers.BackupStartHandler(client))
		client.RegisterHandler("backup_close", controllers.BackupCloseHandler(client))

		client.Start()

		break
	}

	return nil
}
