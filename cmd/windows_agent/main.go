//go:build windows

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/kardianos/service"
	"github.com/pbs-plus/pbs-plus/internal/agent"
	"github.com/pbs-plus/pbs-plus/internal/agent/controllers"
	"github.com/pbs-plus/pbs-plus/internal/agent/forks"
	"github.com/pbs-plus/pbs-plus/internal/agent/registry"
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
	if logger, err := s.Logger(nil); err == nil {
		p.logger = logger
		syslog.L.SetServiceLogger(s)
	}

	p.logger.Info("Starting PBS Plus Agent ", Version)

	handle := windows.CurrentProcess()
	const IDLE_PRIORITY_CLASS = 0x00000040
	if err := windows.SetPriorityClass(handle, uint32(IDLE_PRIORITY_CLASS)); err != nil {
		p.logger.Warning("Failed to set process priority: ", err)
	}

	p.ctx, p.cancel = context.WithCancel(context.Background())

	go p.run()

	return nil
}

func (p *pbsService) Stop(s service.Service) error {
	p.logger.Info("Stopping PBS Plus Agent")

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
		p.logger.Info("Service stopped gracefully")
	case <-time.After(30 * time.Second):
		p.logger.Warning("Service stop timeout reached")
	}

	return nil
}

func (p *pbsService) run() {
	defer func() {
		if r := recover(); r != nil {
			p.logger.Error("Service panicked: ", r)
		}
	}()

	agent.SetStatus("Starting")
	p.logger.Info("Waiting for PBS Plus Agent config")

	if err := p.waitForConfig(); err != nil {
		p.logger.Error("Failed to get configuration: ", err)
		return
	}

	p.logger.Info("Waiting for PBS Plus Agent bootstrap")
	if err := p.waitForBootstrap(); err != nil {
		p.logger.Error("Failed to bootstrap: ", err)
		return
	}

	p.logger.Info("Waiting for PBS Plus Agent backup store")
	if store, err := agent.NewBackupStore(); err != nil {
		p.logger.Warning("Failed to initialize backup store: ", err)
	} else if err := store.ClearAll(); err != nil {
		p.logger.Warning("Failed to clear backup store: ", err)
	}

	p.logger.Info("Waiting for PBS Plus Agent background tasks to initiate")
	p.startBackgroundTasks()

	p.logger.Info("Connecting to ARPC")
	if err := p.connectARPC(); err != nil {
		p.logger.Error("Failed to connect ARPC: ", err)
		return
	}

	agent.SetStatus("Running")
	p.logger.Info("PBS Plus Agent started successfully")

	<-p.ctx.Done()
	agent.SetStatus("Stopping")
}

func (p *pbsService) waitForConfig() error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		entry, err := registry.GetEntry(registry.CONFIG, "ServerURL", false)
		if err == nil && entry != nil {
			return nil
		}

		select {
		case <-p.ctx.Done():
			return fmt.Errorf("context cancelled")
		case <-ticker.C:
			continue
		}
	}
}

func (p *pbsService) waitForBootstrap() error {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		serverCA, _ := registry.GetEntry(registry.AUTH, "ServerCA", true)
		cert, _ := registry.GetEntry(registry.AUTH, "Cert", true)
		priv, _ := registry.GetEntry(registry.AUTH, "Priv", true)

		if serverCA != nil && cert != nil && priv != nil {
			if err := agent.CheckAndRenewCertificate(); err == nil {
				return nil
			}
		} else {
			_ = agent.Bootstrap()
		}

		select {
		case <-p.ctx.Done():
			return fmt.Errorf("context cancelled")
		case <-ticker.C:
			continue
		}
	}
}

func (p *pbsService) startBackgroundTasks() {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()

		for {
			select {
			case <-p.ctx.Done():
				return
			case <-ticker.C:
				if err := agent.CheckAndRenewCertificate(); err != nil {
					p.logger.Warning("Certificate renewal failed: ", err)
				}
			}
		}
	}()

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()

		_ = p.updateDrives()

		ticker := time.NewTicker(utils.ComputeDelay())
		defer ticker.Stop()

		for {
			select {
			case <-p.ctx.Done():
				return
			case <-ticker.C:
				_ = p.updateDrives()
				ticker.Reset(utils.ComputeDelay())
			}
		}
	}()
}

func (p *pbsService) updateDrives() error {
	hostname, err := os.Hostname()
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
	serverUrl, err := registry.GetEntry(registry.CONFIG, "ServerURL", false)
	if err != nil {
		return err
	}

	uri, err := url.Parse(serverUrl.Value)
	if err != nil {
		return err
	}

	tlsConfig, err := agent.GetTLSConfig()
	if err != nil {
		return err
	}

	hostname, err := os.Hostname()
	if err != nil {
		return err
	}

	headers := http.Header{}
	headers.Add("X-PBS-Agent", hostname)
	headers.Add("X-PBS-Plus-Version", Version)

	session, err := arpc.ConnectToServer(p.ctx, true, uri.Host, headers, tlsConfig)
	if err != nil {
		return err
	}

	router := arpc.NewRouter()
	router.Handle("ping", p.handlePing)
	router.Handle("backup", func(req arpc.Request) (arpc.Response, error) {
		return controllers.BackupStartHandler(req, session)
	})
	router.Handle("cleanup", controllers.BackupCloseHandler)

	session.SetRouter(router)

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer session.Close()

		for {
			select {
			case <-p.ctx.Done():
				return
			default:
				if err := session.Serve(); err != nil {
					p.logger.Warning("ARPC connection error, retrying: ", err)
					time.Sleep(5 * time.Second)
				}
			}
		}
	}()

	return nil
}

func (p *pbsService) handlePing(req arpc.Request) (arpc.Response, error) {
	hostname, _ := os.Hostname()
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
