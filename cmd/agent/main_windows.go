//go:build windows

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/kardianos/service"
	"github.com/pbs-plus/pbs-plus/internal/agent"
	"github.com/pbs-plus/pbs-plus/internal/agent/cli"
	"github.com/pbs-plus/pbs-plus/internal/agent/lifecycle"
	"github.com/pbs-plus/pbs-plus/internal/agent/updater"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils"
	"golang.org/x/sys/windows"
)

var Version = "v0.0.0"

type pbsService struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func (p *pbsService) Start(s service.Service) error {
	if os.Getenv("PBS_PLUS_DISABLE_AUTO_UPDATE") != "true" {
		_, _ = updater.New(updater.Config{
			MinConstraint: ">= 0.52.0", PollInterval: 2 * time.Minute, FetchOnStart: true,
			UpgradeConfirm: func(v string) bool { return true },
			Exit:           func(err error) {},
		})
	}
	go p.run()
	return nil
}

func (p *pbsService) run() {
	_ = syslog.L.SetServiceLogger()
	_ = windows.SetPriorityClass(windows.CurrentProcess(), 0x00000040)
	p.ctx, p.cancel = context.WithCancel(context.Background())

	agent.SetStatus("Starting")
	if err := lifecycle.WaitForServerURL(p.ctx); err != nil {
		return
	}
	if err := lifecycle.WaitForBootstrap(p.ctx); err != nil {
		return
	}

	if store, err := agent.NewBackupStore(); err == nil {
		_ = store.ClearAll()
	}

	p.wg.Go(func() {
		ticker := time.NewTicker(time.Hour)
		for {
			select {
			case <-p.ctx.Done():
				return
			case <-ticker.C:
				_ = agent.CheckAndRenewCertificate()
			}
		}
	})
	p.wg.Go(func() {
		_ = lifecycle.UpdateDrives()
		ticker := time.NewTicker(utils.ComputeDelay())
		for {
			select {
			case <-p.ctx.Done():
				return
			case <-ticker.C:
				_ = lifecycle.UpdateDrives()
				ticker.Reset(utils.ComputeDelay())
			}
		}
	})

	_ = lifecycle.ConnectARPC(p.ctx, p.cancel, Version)
	agent.SetStatus("Running")
	<-p.ctx.Done()
	agent.SetStatus("Stopping")
}

func (p *pbsService) Stop(s service.Service) error {
	if p.cancel != nil {
		p.cancel()
	}
	done := make(chan struct{})
	go func() { p.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
	}
	return nil
}

func main() {
	cli.Entry()
	constants.Version = Version
	svcConfig := &service.Config{
		Name: "PBSPlusAgent", DisplayName: "PBS Plus Agent",
		Option: service.KeyValue{"OnFailure": "restart", "OnFailureDelayDuration": "5s"},
	}
	prg := &pbsService{}
	s, err := service.New(prg, svcConfig)
	if err != nil {
		log.Fatal(err)
	}
	if len(os.Args) > 1 {
		if os.Args[1] == "version" {
			fmt.Print(Version)
			return
		}
		_ = service.Control(s, os.Args[1])
		return
	}
	if service.Interactive() {
		prg.run()
		return
	}
	_ = s.Run()
}
