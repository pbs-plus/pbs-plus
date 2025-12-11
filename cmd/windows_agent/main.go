//go:build windows

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/jpillora/overseer"
	"github.com/kardianos/service"
	"github.com/pbs-plus/pbs-plus/internal/agent"
	"github.com/pbs-plus/pbs-plus/internal/agent/controllers"
	"github.com/pbs-plus/pbs-plus/internal/agent/forks"
	"github.com/pbs-plus/pbs-plus/internal/agent/registry"
	"github.com/pbs-plus/pbs-plus/internal/agent/updater"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils"
	"golang.org/x/sys/windows"
)

var Version = "v0.0.0"

type pbsService struct {
	logger service.Logger
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func (p *pbsService) Start(s service.Service) error {
	go func() {
		if updateDisabled := os.Getenv("PBS_PLUS_DISABLE_AUTO_UPDATE"); updateDisabled == "true" {
			p.runForeground(s)(overseer.DisabledState)
			return
		}
		overseer.Run(overseer.Config{
			Program: p.runForeground(s),
			Fetcher: &updater.UpdateFetcher{},
		})
	}()
	return nil
}

func (p *pbsService) runForeground(s service.Service) func(overseer.State) {
	return func(os overseer.State) {
		if logger, err := s.Logger(nil); err == nil {
			p.logger = logger
			syslog.L.SetServiceLogger(logger)
		}

		syslog.L.Info().WithMessage("PBS Plus Agent service starting with version " + Version).Write()

		handle := windows.CurrentProcess()
		const IDLE_PRIORITY_CLASS = 0x00000040
		if err := windows.SetPriorityClass(handle, uint32(IDLE_PRIORITY_CLASS)); err != nil {
			syslog.L.Warn().WithMessage("Failed to set process priority").WithField("error", err.Error()).Write()
		} else {
			syslog.L.Info().WithMessage("Process priority set to idle successfully").Write()
		}

		p.ctx, p.cancel = context.WithCancel(context.Background())

		agent.SetStatus("Starting")
		syslog.L.Info().WithMessage("Waiting for PBS Plus Agent config").Write()

		if err := p.waitForConfig(); err != nil {
			syslog.L.Error(err).WithMessage("Failed to get configuration").Write()
			return
		}
		syslog.L.Info().WithMessage("Configuration acquired successfully").Write()

		syslog.L.Info().WithMessage("Starting bootstrap process").Write()
		if err := p.waitForBootstrap(); err != nil {
			syslog.L.Error(err).WithMessage("Failed to bootstrap").Write()
			return
		}
		syslog.L.Info().WithMessage("Bootstrap completed successfully").Write()

		syslog.L.Info().WithMessage("Initializing backup store").Write()
		if store, err := agent.NewBackupStore(); err != nil {
			syslog.L.Warn().WithMessage("Failed to initialize backup store").WithField("error", err.Error()).Write()
		} else if err := store.ClearAll(); err != nil {
			syslog.L.Warn().WithMessage("Failed to clear backup store").WithField("error", err.Error()).Write()
		} else {
			syslog.L.Info().WithMessage("Backup store initialized and cleared successfully").Write()
		}

		syslog.L.Info().WithMessage("Starting background tasks").Write()
		p.startBackgroundTasks()
		syslog.L.Info().WithMessage("Background tasks started successfully").Write()

		syslog.L.Info().WithMessage("Attempting to connect to ARPC").Write()
		if err := p.connectARPC(); err != nil {
			syslog.L.Error(err).WithMessage("Failed to connect to ARPC").Write()
			return
		}
		syslog.L.Info().WithMessage("ARPC connection established successfully").Write()

		agent.SetStatus("Running")
		syslog.L.Info().WithMessage("PBS Plus Agent fully initialized and running").Write()

		<-p.ctx.Done()
		agent.SetStatus("Stopping")
		syslog.L.Info().WithMessage("Context cancelled, shutting down").Write()
	}
}

func (p *pbsService) Stop(s service.Service) error {
	syslog.L.Info().WithMessage("PBS Plus Agent service stopping").Write()

	if p.cancel != nil {
		p.cancel()
	}

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		syslog.L.Info().WithMessage("Service stopped gracefully").Write()
	case <-time.After(30 * time.Second):
		syslog.L.Warn().WithMessage("Service stop timeout reached").Write()
	}

	return nil
}

func (p *pbsService) waitForConfig() error {
	syslog.L.Info().WithMessage("Starting configuration wait loop").Write()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		syslog.L.Info().WithMessage("Checking for ServerURL configuration").Write()
		entry, err := registry.GetEntry(registry.CONFIG, "ServerURL", false)
		if err == nil && entry != nil {
			syslog.L.Info().WithMessage("ServerURL configuration found").WithField("serverUrl", entry.Value).Write()
			return nil
		}
		syslog.L.Info().WithMessage("ServerURL configuration not found, waiting").WithField("error", fmt.Sprintf("%v", err)).Write()

		select {
		case <-p.ctx.Done():
			syslog.L.Info().WithMessage("Context cancelled while waiting for config").Write()
			return fmt.Errorf("context cancelled")
		case <-ticker.C:
			continue
		}
	}
}

func (p *pbsService) waitForBootstrap() error {
	syslog.L.Info().WithMessage("Starting bootstrap wait loop").Write()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		syslog.L.Info().WithMessage("Checking for authentication certificates").Write()
		serverCA, serverCAErr := registry.GetEntry(registry.AUTH, "ServerCA", true)
		cert, certErr := registry.GetEntry(registry.AUTH, "Cert", true)
		priv, privErr := registry.GetEntry(registry.AUTH, "Priv", true)

		syslog.L.Info().WithMessage("Certificate check results").
			WithField("serverCA", serverCA != nil).
			WithField("cert", cert != nil).
			WithField("priv", priv != nil).
			WithField("serverCAErr", fmt.Sprintf("%v", serverCAErr)).
			WithField("certErr", fmt.Sprintf("%v", certErr)).
			WithField("privErr", fmt.Sprintf("%v", privErr)).Write()

		if serverCA != nil && cert != nil && priv != nil {
			syslog.L.Info().WithMessage("All certificates found, checking validity").Write()
			if err := agent.CheckAndRenewCertificate(); err == nil {
				syslog.L.Info().WithMessage("Certificate validation successful").Write()
				return nil
			} else {
				syslog.L.Warn().WithMessage("Certificate validation failed").WithField("error", err.Error()).Write()
			}
		} else {
			syslog.L.Info().WithMessage("Missing certificates, attempting bootstrap").Write()
			if err := agent.Bootstrap(); err != nil {
				syslog.L.Warn().WithMessage("Bootstrap attempt failed").WithField("error", err.Error()).Write()
			} else {
				syslog.L.Info().WithMessage("Bootstrap attempt completed").Write()
			}
		}

		select {
		case <-p.ctx.Done():
			syslog.L.Info().WithMessage("Context cancelled while waiting for bootstrap").Write()
			return fmt.Errorf("context cancelled")
		case <-ticker.C:
			continue
		}
	}
}

func (p *pbsService) startBackgroundTasks() {
	syslog.L.Info().WithMessage("Starting certificate renewal background task").Write()
	p.wg.Go(func() {
		defer p.wg.Done()
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()

		for {
			select {
			case <-p.ctx.Done():
				syslog.L.Info().WithMessage("Certificate renewal task shutting down").Write()
				return
			case <-ticker.C:
				syslog.L.Info().WithMessage("Running scheduled certificate renewal check").Write()
				if err := agent.CheckAndRenewCertificate(); err != nil {
					syslog.L.Warn().WithMessage("Certificate renewal failed").WithField("error", err.Error()).Write()
				} else {
					syslog.L.Info().WithMessage("Certificate renewal check completed successfully").Write()
				}
			}
		}
	})

	syslog.L.Info().WithMessage("Starting drive update background task").Write()
	p.wg.Go(func() {
		defer p.wg.Done()

		syslog.L.Info().WithMessage("Running initial drive update").Write()
		if err := p.updateDrives(); err != nil {
			syslog.L.Warn().WithMessage("Initial drive update failed").WithField("error", err.Error()).Write()
		} else {
			syslog.L.Info().WithMessage("Initial drive update completed successfully").Write()
		}

		ticker := time.NewTicker(utils.ComputeDelay())
		defer ticker.Stop()

		for {
			select {
			case <-p.ctx.Done():
				syslog.L.Info().WithMessage("Drive update task shutting down").Write()
				return
			case <-ticker.C:
				syslog.L.Info().WithMessage("Running scheduled drive update").Write()
				if err := p.updateDrives(); err != nil {
					syslog.L.Warn().WithMessage("Scheduled drive update failed").WithField("error", err.Error()).Write()
				} else {
					syslog.L.Info().WithMessage("Scheduled drive update completed successfully").Write()
				}
				ticker.Reset(utils.ComputeDelay())
			}
		}
	})
}

func (p *pbsService) updateDrives() error {
	hostname, err := utils.GetAgentHostname()
	if err != nil {
		return err
	}

	drives, err := utils.GetLocalDrives()
	if err != nil {
		return err
	}

	reqBody := map[string]interface{}{
		"hostname": hostname,
		"drives":   drives,
	}

	reqJSON, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}

	resp, err := agent.ProxmoxHTTPRequest(
		http.MethodPost,
		"/api2/json/d2d/target/agent",
		bytes.NewBuffer(reqJSON),
		nil,
	)
	if err != nil {
		return err
	}
	defer resp.Close()

	return nil
}

func (p *pbsService) connectARPC() error {
	syslog.L.Info().WithMessage("Retrieving server URL for ARPC connection").Write()
	serverUrl, err := registry.GetEntry(registry.CONFIG, "ServerURL", false)
	if err != nil {
		syslog.L.Error(err).WithMessage("Failed to retrieve server URL").Write()
		return err
	}
	syslog.L.Info().WithMessage("Server URL retrieved").WithField("serverUrl", serverUrl.Value).Write()

	syslog.L.Info().WithMessage("Parsing server URL").Write()
	uri, err := url.Parse(serverUrl.Value)
	if err != nil {
		syslog.L.Error(err).WithMessage("Failed to parse server URL").Write()
		return err
	}
	syslog.L.Info().WithMessage("Server URL parsed successfully").WithField("host", uri.Host).Write()

	syslog.L.Info().WithMessage("Getting TLS configuration").Write()
	tlsConfig, err := agent.GetTLSConfig()
	if err != nil {
		syslog.L.Error(err).WithMessage("Failed to get TLS configuration").Write()
		return err
	}
	syslog.L.Info().WithMessage("TLS configuration obtained successfully").Write()

	syslog.L.Info().WithMessage("Getting hostname for ARPC headers").Write()
	hostname, err := utils.GetAgentHostname()
	if err != nil {
		syslog.L.Error(err).WithMessage("Failed to get hostname").Write()
		return err
	}
	syslog.L.Info().WithMessage("Hostname obtained").WithField("hostname", hostname).Write()

	headers := http.Header{}
	headers.Add("X-PBS-Agent", hostname)
	headers.Add("X-PBS-Plus-Version", Version)
	syslog.L.Info().WithMessage("ARPC headers prepared").WithField("version", Version).Write()

	syslog.L.Info().WithMessage("Attempting ARPC connection to server").Write()
	session, err := arpc.ConnectToServer(p.ctx, true, uri.Host, headers, tlsConfig)
	if err != nil {
		syslog.L.Error(err).WithMessage("Failed to connect to ARPC server").Write()
		return err
	}
	syslog.L.Info().WithMessage("ARPC session established").Write()

	syslog.L.Info().WithMessage("Setting up ARPC router and handlers").Write()
	router := arpc.NewRouter()
	router.Handle("ping", p.handlePing)
	router.Handle("backup", func(req arpc.Request) (arpc.Response, error) {
		return controllers.BackupStartHandler(req, session)
	})
	router.Handle("target_status", controllers.StatusHandler)
	router.Handle("cleanup", controllers.BackupCloseHandler)

	session.SetRouter(router)
	syslog.L.Info().WithMessage("ARPC router configured successfully").Write()

	syslog.L.Info().WithMessage("Starting ARPC session handler goroutine").Write()

	p.wg.Go(func() {
		defer p.wg.Done()
		defer session.Close()
		defer func() {
			syslog.L.Info().WithMessage("ARPC session handler shutting down").Write()
		}()

		base := 500 * time.Millisecond
		maxWait := 30 * time.Second
		factor := 2.0
		jitter := 0.2

		backoff := base

		for {
			select {
			case <-p.ctx.Done():
				return
			default:
				if err := session.Serve(); err != nil {
					mult := 1 + jitter*(2*rand.Float64()-1)
					sleep := time.Duration(float64(backoff) * mult)
					sleep = min(sleep, maxWait)

					syslog.L.Warn().WithMessage(fmt.Sprintf("ARPC connection error, retrying after %v", sleep)).WithField("error", err.Error()).Write()

					select {
					case <-p.ctx.Done():
						return
					case <-time.After(sleep):
					}

					next := time.Duration(float64(backoff) * factor)
					next = min(next, maxWait)

					backoff = next

					if err = session.Reconnect(); err != nil {
						syslog.L.Warn().WithMessage("ARPC reconnection error").WithField("error", err.Error()).Write()
					}
				}
			}
		}
	})

	return nil
}

func (p *pbsService) handlePing(req arpc.Request) (arpc.Response, error) {
	hostname, _ := utils.GetAgentHostname()
	resp := map[string]string{
		"version":  Version,
		"hostname": hostname,
	}

	respData, err := json.Marshal(resp)
	if err != nil {
		return arpc.Response{}, err
	}

	return arpc.Response{Status: 200, Data: respData}, nil
}

func main() {
	forks.CmdBackup()
	constants.Version = Version

	svcConfig := &service.Config{
		Name:        "PBSPlusAgent",
		DisplayName: "PBS Plus Agent",
		Description: "Agent for orchestrating backups with PBS Plus",
		Option: service.KeyValue{
			"DelayedAutoStart":       false,
			"OnFailure":              "restart",
			"OnFailureDelayDuration": "5s",
			"OnFailureResetPeriod":   300,
		},
	}

	prg := &pbsService{}
	s, err := service.New(prg, svcConfig)
	if err != nil {
		log.Fatal("Failed to create service: ", err)
	}

	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "version":
			fmt.Print(Version)
			return
		case "install":
			err := service.Control(s, "install")
			if err != nil {
				log.Fatal("Failed to install service: ", err)
			}
			fmt.Println("Service installed successfully")
		case "uninstall":
			err := service.Control(s, "uninstall")
			if err != nil {
				log.Fatal("Failed to uninstall service: ", err)
			}
			fmt.Println("Service uninstalled successfully")
		default:
			err := service.Control(s, os.Args[1])
			if err != nil {
				log.Fatal("Failed to execute command: ", err)
			}
		}
		return
	}

	err = s.Run()
	if err != nil {
		log.Fatal("Service failed: ", err)
	}
}
