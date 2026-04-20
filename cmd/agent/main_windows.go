//go:build windows

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
	"github.com/pbs-plus/pbs-plus/internal/agent/updater"
	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"golang.org/x/sys/windows"

	_ "net/http/pprof"
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
			Service:        s,
			Context:        p.ctx,
		})
	}
	go p.run()
	return nil
}

func (p *pbsService) run() {
	_ = syslog.L.SetServiceLogger()
	_ = windows.SetPriorityClass(windows.CurrentProcess(), 0x00000040)

	if err := lifecycle.WaitForServerURL(p.ctx); err != nil {
		return
	}

	for {
		syslog.L.Info().
			WithMessage("waiting for bootstrap").
			Write()

		if err := lifecycle.WaitForBootstrap(p.ctx); err != nil {
			return
		}

		syslog.L.Info().
			WithMessage("bootstrap complete, starting session").
			Write()

		if store, err := agent.NewBackupStore(); err == nil {
			_ = store.ClearAll()
		}

		innerCtx, innerCancel := context.WithCancel(p.ctx)

		var innerWg sync.WaitGroup

		innerWg.Go(func() {
			ticker := time.NewTicker(time.Hour)
			defer ticker.Stop()
			for {
				select {
				case <-innerCtx.Done():
					return
				case <-ticker.C:
					_ = agent.CheckAndRenewCertificate()
				}
			}
		})

		innerWg.Go(func() {
			_ = lifecycle.UpdateDrives()
			ticker := time.NewTicker(agent.ComputeDelay())
			defer ticker.Stop()
			for {
				select {
				case <-innerCtx.Done():
					return
				case <-ticker.C:
					_ = lifecycle.UpdateDrives()
					ticker.Reset(agent.ComputeDelay())
				}
			}
		})

		certErrCh, err := lifecycle.ConnectARPC(innerCtx, Version)
		if err != nil {
			syslog.L.Error(err).
				WithMessage("failed to start arpc connection").
				Write()
			innerCancel()
			innerWg.Wait()

			select {
			case <-p.ctx.Done():
				return
			case <-time.After(10 * time.Second):
				continue
			}
		}

		select {
		case <-p.ctx.Done():
			innerCancel()
			innerWg.Wait()
			return

		case certErr, ok := <-certErrCh:
			innerCancel()
			innerWg.Wait()

			if !ok {
				return
			}

			syslog.L.Error(certErr).
				WithMessage("clearing certificates and re-bootstrapping").
				Write()
			lifecycle.ClearCertificates()

			select {
			case <-p.ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
		}
	}
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
	conf.Version = Version
	cli.Entry()

	svcConfig := &service.Config{
		Name: "PBSPlusAgent", DisplayName: "PBS Plus Agent",
		Option: service.KeyValue{"OnFailure": "restart", "OnFailureDelayDuration": "5s"},
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

	if service.Interactive() {
		prg.run()
		return
	}

	err = s.Run()
	if err != nil {
		log.Fatal(err)
	}
}
