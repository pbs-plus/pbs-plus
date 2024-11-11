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
	svc        service.Service
	ctx        context.Context
	pingCtx    context.Context
	cancel     context.CancelFunc
	pingCancel context.CancelFunc
	wg         sync.WaitGroup
}

func (p *agentService) Start(s service.Service) error {
	p.ctx, p.cancel = context.WithCancel(context.Background())
	p.pingCtx, p.pingCancel = context.WithCancel(context.Background())

	go p.startPing()
	go p.runLoop()

	return nil
}

func (p *agentService) startPing() {
	for {
		select {
		case <-p.pingCtx.Done():
			utils.SetEnvironment("PBS_AGENT_STATUS", "Agent service is not running")
			return
		default:
			var pingResp PingResp
			pingErr := agent.ProxmoxHTTPRequest(http.MethodGet, "/api2/json/ping", nil, &pingResp)
			if pingErr != nil {
				utils.SetEnvironment("PBS_AGENT_STATUS", fmt.Sprintf("Error - (%s)", pingErr.Error()))
			} else if !pingResp.Data.Pong {
				utils.SetEnvironment("PBS_AGENT_STATUS", "Error - server did not return expected data")
			} else {
				utils.SetEnvironment("PBS_AGENT_STATUS", "Connected")
			}
			time.Sleep(10 * time.Second)
		}
	}
}

func (p *agentService) runLoop() {
	logger, err := syslog.InitializeLogger(p.svc)
	if err != nil {
		utils.SetEnvironment("PBS_AGENT_STATUS", fmt.Sprintf("Failed to initialize logger -> %s", err.Error()))
		return
	}

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
		}

		time.Sleep(5 * time.Second)
	}
}

func (p *agentService) run() {
	utils.SetEnvironment("PBS_AGENT_STATUS", "Starting")
	logger, err := syslog.InitializeLogger(p.svc)
	if err != nil {
		utils.SetEnvironment("PBS_AGENT_STATUS", fmt.Sprintf("Failed to initialize logger -> %s", err.Error()))
		return
	}

waitUrl:
	for {
		select {
		case <-p.ctx.Done():
			return
		default:
			key, err := registry.OpenKey(registry.LOCAL_MACHINE, `Software\PBSPlus\Config`, registry.QUERY_VALUE)
			if err == nil {
				defer key.Close()

				if serverUrl, _, err := key.GetStringValue("ServerURL"); err == nil && serverUrl != "" {
					break waitUrl
				}
			}
			time.Sleep(time.Second)
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
		go sftp.Serve(p.ctx, &p.wg, sftpConfig, "0.0.0.0", port, driveLetter)
	}
}

func (p *agentService) Stop(s service.Service) error {
	p.cancel()
	p.pingCancel()

	p.wg.Wait()

	return nil
}
