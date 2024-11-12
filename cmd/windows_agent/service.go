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
			agent.SetStatus(fmt.Sprintf("Error - (%s)", pingErr.Error()))
		} else if !pingResp.Data.Pong {
			agent.SetStatus("Error - server did not return expected data")
		} else {
			agent.SetStatus("Connected")
		}
	}

	ping()

	for {
		select {
		case <-p.ctx.Done():
			agent.SetStatus("Agent service is not running")
			return
		case <-time.After(time.Second * 5):
			ping()
		}
	}
}

func (p *agentService) run() {
	agent.SetStatus("Starting")
	logger, err := syslog.InitializeLogger(p.svc)
	if err != nil {
		agent.SetStatus(fmt.Sprintf("Failed to initialize logger -> %s", err.Error()))
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
			select {
			case <-p.ctx.Done():
				return
			case <-time.After(time.Second * 5):
			}
		}
	}

	drives := getLocalDrives()
	for _, drive := range drives {
		drive.ErrorChan = make(chan string)
		err = drive.serveSFTP(p)
		for err != nil {
			logger.Errorf("Drive SFTP error: %v", err)
			select {
			case <-p.ctx.Done():
				return
			case <-time.After(time.Second * 5):
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
