//go:build windows
// +build windows

package main

import (
	"context"
	_ "embed"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/kardianos/service"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/sftp"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
	"golang.org/x/sys/windows/registry"
)

type PingData struct {
	Pong bool `json:"pong"`
}

type PingResp struct {
	Data PingData `json:"data"`
}

type agentService struct {
	svc    service.Service
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

func (p *agentService) Start(s service.Service) error {
	p.ctx, p.cancel = context.WithCancel(context.Background())

	go p.runLoop()

	return nil
}

func (p *agentService) startPing() {
	firstPing := true
	lastCheck := time.Now()
	for {
		select {
		case <-p.ctx.Done():
			utils.SetEnvironment("PBS_AGENT_STATUS", "Agent service is not running")
			return
		default:
			if time.Since(lastCheck) > time.Second*5 || firstPing {
				firstPing = false

				var pingResp PingResp
				pingErr := agent.ProxmoxHTTPRequest(http.MethodGet, "/api2/json/ping", nil, &pingResp)
				if pingErr != nil {
					utils.SetEnvironment("PBS_AGENT_STATUS", fmt.Sprintf("Error - (%s)", pingErr.Error()))
				} else if !pingResp.Data.Pong {
					utils.SetEnvironment("PBS_AGENT_STATUS", "Error - server did not return expected data")
				} else {
					utils.SetEnvironment("PBS_AGENT_STATUS", "Connected")
				}
				lastCheck = time.Now()
			}
		}
	}
}

func (p *agentService) runLoop() {
	logger, err := syslog.InitializeLogger(p.svc)
	if err != nil {
		utils.SetEnvironment("PBS_AGENT_STATUS", fmt.Sprintf("Failed to initialize logger -> %s", err.Error()))
		return
	}

	go p.startPing()

	for {
		p.run()
		wgDone := utils.WaitChan(&p.wg)

		select {
		case <-p.ctx.Done():
			snapshots.CloseAllSnapshots()
			return
		case <-wgDone:
			utils.SetEnvironment("PBS_AGENT_STATUS", "Unexpected shutdown - restarting SSH endpoints")
			logger.Error("SSH endpoints stopped unexpectedly. Restarting...")
			p.wg = sync.WaitGroup{}
			time.Sleep(5 * time.Second)
		}
	}
}

func (p *agentService) run() {
	utils.SetEnvironment("PBS_AGENT_STATUS", "Starting")
	logger, err := syslog.InitializeLogger(p.svc)
	if err != nil {
		utils.SetEnvironment("PBS_AGENT_STATUS", fmt.Sprintf("Failed to initialize logger -> %s", err.Error()))
		return
	}

	firstUrlCheck := true
	lastCheck := time.Now()
waitUrl:
	for {
		select {
		case <-p.ctx.Done():
			return
		default:
			if time.Since(lastCheck) > time.Second*5 || firstUrlCheck {
				firstUrlCheck = false
				key, err := registry.OpenKey(registry.LOCAL_MACHINE, `Software\PBSPlus\Config`, registry.QUERY_VALUE)
				if err == nil {
					defer key.Close()

					if serverUrl, _, err := key.GetStringValue("ServerURL"); err == nil && serverUrl != "" {
						break waitUrl
					}
				}
				lastCheck = time.Now()
			}
		}
	}

	drives := utils.GetLocalDrives()
	for _, driveLetter := range drives {
		rune := []rune(driveLetter)[0]
		sftpConfig, err := sftp.InitializeSFTPConfig(p.svc, driveLetter)
		if err != nil {
			logger.Error(fmt.Sprintf("Unable to initialize SFTP config: %s", err))
			continue
		}
		if err := sftpConfig.PopulateKeys(); err != nil {
			logger.Error(fmt.Sprintf("Unable to populate SFTP keys: %s", err))
			continue
		}

		port, err := utils.DriveLetterPort(rune)
		if err != nil {
			logger.Error(fmt.Sprintf("Unable to map letter to port: %s", err))
			continue
		}

		p.wg.Add(1)
		go func() {
			sftp.Serve(p.ctx, sftpConfig, "0.0.0.0", port, driveLetter)
			p.wg.Done()
		}()
	}
}

func (p *agentService) Stop(s service.Service) error {
	p.cancel()

	return nil
}
