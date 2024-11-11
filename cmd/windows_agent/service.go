//go:build windows
// +build windows

package main

import (
	"context"
	_ "embed"
	"fmt"
	"net/http"
	"time"

	"github.com/kardianos/service"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent"
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
	ctx    context.Context
	cancel context.CancelFunc
}

func (p *agentService) Start(s service.Service) error {
	p.ctx, p.cancel = context.WithCancel(context.Background())

	go p.startPing()
	go p.run()

	return nil
}

func (p *agentService) startPing() {
	ping := func() {
		var pingResp PingResp
		pingErr := agent.ProxmoxHTTPRequest(http.MethodGet, "/api2/json/ping", nil, &pingResp)
		if pingErr != nil {
			utils.SetEnvironment("PBS_AGENT_STATUS", fmt.Sprintf("Error - (%s)", pingErr.Error()))
		} else if !pingResp.Data.Pong {
			utils.SetEnvironment("PBS_AGENT_STATUS", "Error - server did not return expected data")
		} else {
			utils.SetEnvironment("PBS_AGENT_STATUS", "Connected")
		}
	}

	ping()

	for {
		retryWait := utils.WaitChan(time.Second * 5)
		select {
		case <-p.ctx.Done():
			utils.SetEnvironment("PBS_AGENT_STATUS", "Agent service is not running")
			return
		case <-retryWait:
			ping()
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

	urlExists := func() bool {
		key, err := registry.OpenKey(registry.LOCAL_MACHINE, `Software\PBSPlus\Config`, registry.QUERY_VALUE)
		if err == nil {
			defer key.Close()

			if serverUrl, _, err := key.GetStringValue("ServerURL"); err == nil && serverUrl != "" {
				return true
			}
		}

		return false
	}

	if !urlExists() {
		for !urlExists() {
			retryWait := utils.WaitChan(time.Second * 5)
			select {
			case <-p.ctx.Done():
				return
			case <-retryWait:
			}
		}
	}

	drives := getLocalDrives()
	for _, drive := range drives {
		drive.ErrorChan = make(chan string)
		err = drive.serveSFTP(p)
		for err != nil {
			logger.Errorf("Drive SFTP error: %v", err)
			retryWait := utils.WaitChan(time.Second * 5)
			select {
			case <-p.ctx.Done():
				return
			case <-retryWait:
				err = drive.serveSFTP(p)
			}
		}

		go func() {
			defer close(drive.ErrorChan)

			for {
				select {
				case <-p.ctx.Done():
					return
				case err := <-drive.ErrorChan:
					logger.Errorf("SFTP %s drive error: %s", drive.Letter, err)
				}
			}
		}()
	}

	<-p.ctx.Done()
	snapshots.CloseAllSnapshots()
}

func (p *agentService) Stop(s service.Service) error {
	p.cancel()

	return nil
}
