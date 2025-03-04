//go:build windows
// +build windows

package main

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/alexflint/go-filemutex"
	"github.com/kardianos/service"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/controllers"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/registry"
	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
	"golang.org/x/sys/windows"
)

type PingData struct {
	Pong bool `json:"pong"`
}

type PingResp struct {
	Data PingData `json:"data"`
}

type AgentDrivesRequest struct {
	Hostname string            `json:"hostname"`
	Drives   []utils.DriveInfo `json:"drives"`
}

type agentService struct {
	svc    service.Service
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func (p *agentService) Start(s service.Service) error {
	handle := windows.CurrentProcess()

	const IDLE_PRIORITY_CLASS = 0x00000040
	err := windows.SetPriorityClass(handle, uint32(IDLE_PRIORITY_CLASS))
	if err != nil {
		syslog.L.Errorf("failed to set process priority: %v", err)
	}

	p.svc = s
	p.ctx, p.cancel = context.WithCancel(context.Background())

	p.wg.Add(2)
	go func() {
		defer p.wg.Done()
		p.run()
	}()
	go func() {
		defer p.wg.Done()
		for {
			select {
			case <-p.ctx.Done():
				return
			case <-time.After(time.Hour):
				err := agent.CheckAndRenewCertificate()
				if err != nil {
					syslog.L.Errorf("Certificate renewal manager: %v", err)
				}
			}
		}
	}()

	store, err := agent.NewBackupStore()
	if err != nil {
		syslog.L.Errorf("Error initializing backup store: %v", err)
	} else {
		err = store.ClearAll()
		if err != nil {
			syslog.L.Errorf("Error clearing backup store: %v", err)
		}
	}

	return nil
}

func (p *agentService) Stop(s service.Service) error {
	p.cancel()
	p.wg.Wait()
	return nil
}

func (p *agentService) run() {
	agent.SetStatus("Starting")
	if err := p.waitForServerURL(); err != nil {
		syslog.L.Errorf("Failed waiting for server URL: %v", err)
		return
	}

	if err := p.waitForBootstrap(); err != nil {
		syslog.L.Errorf("Failed waiting for bootstrap: %v", err)
		return
	}

	if err := p.initializeDrives(); err != nil {
		syslog.L.Errorf("Failed to initialize drives: %v", err)
		return
	}

	if err := p.connectARPC(); err != nil {
		syslog.L.Errorf("ARPC connection failed: %v", err)
		return
	}

	go func() {
		delay := utils.ComputeDelay()
		for {
			select {
			case <-p.ctx.Done():
				return
			case <-time.After(delay):
				_ = p.initializeDrives()
				delay = utils.ComputeDelay()
			}
		}
	}()

	<-p.ctx.Done()
}

func (p *agentService) waitForServerURL() error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		entry, err := registry.GetEntry(registry.CONFIG, "ServerURL", false)
		if err == nil && entry != nil {
			return nil
		}

		select {
		case <-p.ctx.Done():
			return fmt.Errorf("context cancelled while waiting for server URL")
		case <-ticker.C:
			continue
		}
	}
}

func (p *agentService) waitForBootstrap() error {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		serverCA, _ := registry.GetEntry(registry.AUTH, "ServerCA", true)
		cert, _ := registry.GetEntry(registry.AUTH, "Cert", true)
		priv, _ := registry.GetEntry(registry.AUTH, "Priv", true)

		if serverCA != nil && cert != nil && priv != nil {
			err := agent.CheckAndRenewCertificate()
			if err == nil {
				return nil
			}
			syslog.L.Errorf("Renewal error: %v", err)
		} else {
			err := agent.Bootstrap()
			if err != nil {
				syslog.L.Errorf("Bootstrap error: %v", err)
			}
		}

		select {
		case <-p.ctx.Done():
			return fmt.Errorf("context cancelled while waiting for server URL")
		case <-ticker.C:
			continue
		}
	}
}

func (p *agentService) initializeDrives() error {
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("failed to get hostname: %w", err)
	}

	drives, err := utils.GetLocalDrives()
	if err != nil {
		return fmt.Errorf("failed to get local drives list: %w", err)
	}

	reqBody, err := json.Marshal(&AgentDrivesRequest{
		Hostname: hostname,
		Drives:   drives,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal drive request: %w", err)
	}

	resp, err := agent.ProxmoxHTTPRequest(
		http.MethodPost,
		"/api2/json/d2d/target/agent",
		bytes.NewBuffer(reqBody),
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to update agent drives: %w", err)
	}
	defer resp.Close()
	_, _ = io.Copy(io.Discard, resp)

	return nil
}

func (p *agentService) writeVersionToFile() error {
	ex, err := os.Executable()
	if err != nil {
		return fmt.Errorf("failed to get executable path: %w", err)
	}

	versionLockPath := filepath.Join(filepath.Dir(ex), "version.lock")
	mutex, err := filemutex.New(versionLockPath)
	if err != nil {
		return fmt.Errorf("failed to execute mutex: %w", err)
	}

	mutex.Lock()
	defer mutex.Close()

	versionFile := filepath.Join(filepath.Dir(ex), "version.txt")
	err = os.WriteFile(versionFile, []byte(Version), 0644)
	if err != nil {
		return fmt.Errorf("failed to write version file: %w", err)
	}

	return nil
}

func (p *agentService) connectARPC() error {
	serverUrl, err := registry.GetEntry(registry.CONFIG, "ServerURL", false)
	if err != nil {
		return fmt.Errorf("invalid server URL: %v", err)
	}
	uri, err := url.Parse(serverUrl.Value)
	if err != nil {
		return fmt.Errorf("invalid server URL: %v", err)
	}

	tlsConfig, err := agent.GetTLSConfig()
	if err != nil {
		syslog.L.Errorf("ARPC client tls config error: %s", err)
		return err
	}

	clientId, err := os.Hostname()
	if err != nil {
		syslog.L.Errorf("hostname retrieval error: %s", err)
		return err
	}

	headers := http.Header{}
	headers.Add("X-PBS-Agent", clientId)
	headers.Add("X-PBS-Plus-Version", Version)
	syslog.L.Infof("Setting connection headers: %s, %s", clientId, Version)

	session, err := arpc.ConnectToServer(p.ctx, uri.Host, headers, tlsConfig)
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}

	router := arpc.NewRouter()
	router.Handle("ping", func(req arpc.Request) (arpc.Response, error) {
		resp := arpc.MapStringStringMsg{"version": Version, "hostname": clientId}
		b, err := resp.MarshalMsg(nil)
		if err != nil {
			return arpc.Response{}, err
		}
		return arpc.Response{Status: 200, Data: b}, nil
	})
	router.Handle("backup", func(req arpc.Request) (arpc.Response, error) {
		return controllers.BackupStartHandler(req, session)
	})
	router.Handle("cleanup", controllers.BackupCloseHandler)

	session.SetRouter(router)

	go func() {
		defer session.Close()
		for {
			select {
			case <-p.ctx.Done():
				return
			default:
				syslog.L.Info("Connecting aRPC endpoint from /plus/arpc")
				if err := session.Serve(); err != nil {
					syslog.L.Errorf("session closed: %v", err)

					store, err := agent.NewBackupStore()
					if err != nil {
						syslog.L.Errorf("Error initializing backup store: %v", err)
					} else {
						err = store.ClearAll()
						if err != nil {
							syslog.L.Errorf("Error clearing backup store: %v", err)
						}
					}
				}
			}
		}
	}()

	return nil
}
