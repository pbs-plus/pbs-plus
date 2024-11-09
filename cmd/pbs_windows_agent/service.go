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
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/agent"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/agent/sftp"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/agent/snapshots"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/syslog"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/utils"
	"golang.org/x/sys/windows/registry"
)

type PingResp struct {
	Pong bool `json:"pong"`
}

type agentService struct {
	svc    service.Service
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func (p *agentService) Start(s service.Service) error {
	p.ctx, p.cancel = context.WithCancel(context.Background())
	go p.run()

	return nil
}

func (p *agentService) run() {
	utils.SetEnvironment("PBS_AGENT_STATUS", "Starting")
	logger, err := syslog.InitializeLogger(p.svc)
	if err != nil {
		utils.SetEnvironment("PBS_AGENT_STATUS", fmt.Sprintf("Failed to initialize logger -> %s", err.Error()))
		return
	}

	for {
		key, err := registry.OpenKey(registry.LOCAL_MACHINE, `Software\ProxmoxAgent\Config`, registry.QUERY_VALUE)
		if err == nil {
			defer key.Close()

			if serverUrl, _, err := key.GetStringValue("ServerURL"); err == nil && serverUrl != "" {
				break
			}
		}
		time.Sleep(time.Second)
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

	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				utils.SetEnvironment("PBS_AGENT_STATUS", "Agent service is not running")
				return
			default:
				var pingResp PingResp
				pingErr := agent.ProxmoxHTTPRequest(http.MethodGet, "/api2/json/ping", nil, &pingResp)
				if pingErr != nil {
					utils.SetEnvironment("PBS_AGENT_STATUS", fmt.Sprintf("Error - (%s)", pingErr.Error()))
					continue
				}
				if !pingResp.Pong {
					utils.SetEnvironment("PBS_AGENT_STATUS", "Error - server did not return expected data")
					continue
				}

				utils.SetEnvironment("PBS_AGENT_STATUS", "Connected")
			}
		}
	}(p.ctx)

	p.wg.Wait()
}

func (p *agentService) Stop(s service.Service) error {
	if p.cancel != nil {
		p.cancel()
	}

	utils.SetEnvironment("PBS_AGENT_STATUS", "Agent service is not running")
	snapshots.CloseAllSnapshots()
	return nil
}
