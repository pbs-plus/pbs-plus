//go:build unix

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/kardianos/service"
	"github.com/pbs-plus/pbs-plus/internal/agent"
	"github.com/pbs-plus/pbs-plus/internal/agent/cli"
	"github.com/pbs-plus/pbs-plus/internal/agent/lifecycle"
	"github.com/pbs-plus/pbs-plus/internal/agent/registry"
	"github.com/pbs-plus/pbs-plus/internal/agent/updater"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils"

	"net/http"
	_ "net/http/pprof"

	_ "github.com/pbs-plus/pbs-plus/internal/utils/memlimit"
)

var Version = "v0.0.0"

type pbsService struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func (p *pbsService) Start(s service.Service) error {
	if os.Getenv("PBS_PLUS_DISABLE_AUTO_UPDATE") != "true" && os.Getenv("PBS_PLUS__I_AM_INSIDE_CONTAINER") != "true" {
		_, _ = updater.New(updater.Config{
			MinConstraint: ">= 0.52.0", PollInterval: 2 * time.Minute, FetchOnStart: true,
			UpgradeConfirm: func(v string) bool { return true },
			Exit:           func(err error) {},
			Service:        s,
			Context:        p.ctx,
		})
	}
	go p.run()
	return nil
}

func (p *pbsService) run() {
	_ = syslog.L.SetServiceLogger()
	_ = registry.CreateEntryIfNotExists(&registry.RegistryEntry{Path: registry.CONFIG, Key: "ServerURL", Value: os.Getenv("PBS_PLUS_INIT_SERVER_URL")})
	_ = registry.CreateEntryIfNotExists(&registry.RegistryEntry{Path: registry.CONFIG, Key: "BootstrapToken", Value: os.Getenv("PBS_PLUS_INIT_BOOTSTRAP_TOKEN")})

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
	if os.Getenv("PBS_PLUS_PPROF") == "true" {
		go func() {
			log.Println(http.ListenAndServe(":6060", nil))
		}()
	}

	err := lifecycle.ConnectARPC(p.ctx, p.cancel, Version)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to connect to arpc").Write()
	}
	<-p.ctx.Done()
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
	defer func() {
		if r := recover(); r != nil {
			_ = os.WriteFile("panic.log", []byte(fmt.Sprintf("Panic: %v\n%s", r, debug.Stack())), 0644)
			os.Exit(1)
		}
	}()
	constants.Version = Version
	cli.Entry()

	svcConfig := &service.Config{
		Name:        "pbs-plus-agent",
		DisplayName: "PBS Plus Agent",
		Description: "Agent for orchestrating backups with PBS Plus",
		UserName:    "pbsplus",
		Option: service.KeyValue{
			"SystemdScript": lifecycle.SYSTEMD_SCRIPT,
			"OpenRCScript":  lifecycle.OPENRC_SCRIPT,
		},
	}
	prg := &pbsService{}
	prg.ctx, prg.cancel = context.WithCancel(context.Background())

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

	dockerEnv := os.Getenv("PBS_PLUS__I_AM_INSIDE_CONTAINER")

	if service.Interactive() || dockerEnv == "true" {
		prg.run()
		return
	}

	err = s.Run()
	if err != nil {
		log.Fatal(err)
	}
}
