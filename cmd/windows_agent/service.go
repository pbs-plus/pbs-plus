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
	exit       chan struct{}
	restart    chan struct{}
}

func (p *agentService) Start(s service.Service) error {
	p.exit = make(chan struct{})
	p.restart = make(chan struct{})
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
	defer func() {
		p.cancel()
		close(p.exit)
	}()

	for {
		select {
		case <-p.exit:
			return
		default:
			p.run()
		}

		select {
		case <-p.exit:
			return
		case <-p.restart:
			p.cancel()
			p.wg.Wait()
			p.ctx, p.cancel = context.WithCancel(context.Background())
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

	for {
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

		wgDone := utils.WaitChan(&p.wg)

		select {
		case <-p.ctx.Done():
			snapshots.CloseAllSnapshots()
			return
		case <-wgDone:
			utils.SetEnvironment("PBS_AGENT_STATUS", "Unexpected shutdown - restarting SFTP servers")
			logger.Error("SFTP servers stopped unexpectedly. Restarting...")
			// Reset the WaitGroup to prepare for new SFTP servers
			p.wg = sync.WaitGroup{}
		}
	}
}

func (p *agentService) Stop(s service.Service) error {
	close(p.exit)
	p.cancel()
	p.pingCancel()

	p.wg.Wait()
	return nil
}

func (p *agentService) Restart() {
	p.restart <- struct{}{}
}

