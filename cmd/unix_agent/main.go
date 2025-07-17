//go:build unix

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"sync"
	"syscall"
	"time"

	"github.com/gofrs/flock"
	"github.com/pbs-plus/pbs-plus/internal/agent"
	"github.com/pbs-plus/pbs-plus/internal/agent/controllers"
	"github.com/pbs-plus/pbs-plus/internal/agent/forks"
	"github.com/pbs-plus/pbs-plus/internal/agent/registry"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils"
)

var Version = "v0.0.0"

type AgentDrivesRequest struct {
	Hostname string            `json:"hostname"`
	Drives   []utils.DriveInfo `json:"drives"`
}

type agentService struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func (p *agentService) Start() error {
	p.ctx, p.cancel = context.WithCancel(context.Background())

	serverUrlConfigDefault := registry.RegistryEntry{
		Path:     registry.CONFIG,
		Key:      "ServerURL",
		Value:    "",
		IsSecret: false,
	}
	_ = registry.CreateEntryIfNotExists(&serverUrlConfigDefault)

	bootstrapTokenConfigDefault := registry.RegistryEntry{
		Path:     registry.CONFIG,
		Key:      "BootstrapToken",
		Value:    "",
		IsSecret: false,
	}
	_ = registry.CreateEntryIfNotExists(&bootstrapTokenConfigDefault)

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
					syslog.L.Error(err).WithMessage("failed to check and renew certificate").Write()
				}
			}
		}
	}()

	store, err := agent.NewBackupStore()
	if err != nil {
		syslog.L.Error(err).WithMessage("error initializing backup store").Write()
	} else {
		err = store.ClearAll()
		if err != nil {
			syslog.L.Error(err).WithMessage("error clearing backup store").Write()
		}
	}

	return nil
}

func (p *agentService) Stop() error {
	p.cancel()
	p.wg.Wait()
	return nil
}

func (p *agentService) run() {
	syslog.L.Info().WithMessage("checking server URL config").Write()
	if err := p.waitForServerURL(); err != nil {
		syslog.L.Error(err).WithMessage("failed waiting for server url").Write()
		return
	}

	syslog.L.Info().WithMessage("checking if bootstrap required from config").Write()
	if err := p.waitForBootstrap(); err != nil {
		syslog.L.Error(err).WithMessage("failed waiting for bootstrap").Write()
		return
	}

	go func() {
		syslog.L.Info().WithMessage("initializing drives status").Write()
		if err := p.initializeDrives(); err != nil {
			syslog.L.Error(err).WithMessage("failed to initializing drives").Write()
			return
		}
	}()

	syslog.L.Info().WithMessage("attempting to connect via aRPC").Write()
	if err := p.connectARPC(); err != nil {
		syslog.L.Error(err).WithMessage("failed to connect via aRPC").Write()
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
			syslog.L.Error(err).WithMessage("error renewing certificate").Write()
		} else {
			err := agent.Bootstrap()
			if err != nil {
				syslog.L.Error(err).WithMessage("error bootstrapping agent").Write()
			}
		}

		select {
		case <-p.ctx.Done():
			return fmt.Errorf("context cancelled while waiting for bootstrap")
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
		syslog.L.Error(err).WithMessage("failed to get tls config error for arpc client").Write()
		return err
	}

	clientId, err := os.Hostname()
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to retrieve machine hostname").Write()
		return err
	}

	headers := http.Header{}
	headers.Add("X-PBS-Agent", clientId)
	headers.Add("X-PBS-Plus-Version", Version)

	session, err := arpc.ConnectToServer(p.ctx, true, uri.Host, headers, tlsConfig)
	if err != nil {
		return err
	}

	router := arpc.NewRouter()
	router.Handle("ping", func(req arpc.Request) (arpc.Response, error) {
		resp := arpc.MapStringStringMsg{"version": Version, "hostname": clientId}
		b, err := resp.Encode()
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
				syslog.L.Info().WithMessage("connecting arpc endpoint from /plus/arpc").WithField("hostname", clientId).Write()
				if err := session.Serve(); err != nil {
					store, err := agent.NewBackupStore()
					if err != nil {
						syslog.L.Error(err).WithMessage("error initializing backup store").Write()
					} else {
						err = store.ClearAll()
						if err != nil {
							syslog.L.Error(err).WithMessage("error clearing backup store").Write()
						}
					}
				}
			}
		}
	}()

	return nil
}

func (p *agentService) writeVersionToFile() error {
	if err := os.MkdirAll("/etc/pbs-plus-agent", 0755); err != nil {
		return err
	}

	versionLockPath := filepath.Join("/etc/pbs-plus-agent", "version.lock")
	mutex := flock.New(versionLockPath)

	mutex.Lock()
	defer mutex.Close()

	versionFile := filepath.Join("/etc/pbs-plus-agent", "version.txt")
	err := os.WriteFile(versionFile, []byte(Version), 0644)
	if err != nil {
		return fmt.Errorf("failed to write version file: %w", err)
	}

	return nil
}

func main() {
	defer func() {
		if r := recover(); r != nil {
			msg := fmt.Sprintf("Panic occurred: %v\nStack trace:\n%s", r, debug.Stack())

			logFile, err := os.OpenFile("panic.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
			if err == nil {
				defer logFile.Close()
				logFile.WriteString(msg)
			} else {
				fmt.Println("Error opening log file:", err)
			}

			os.Exit(1)
		}
	}()

	forks.CmdBackup()

	constants.Version = Version

	prg := &agentService{}

	err := prg.writeVersionToFile()
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to write version to file").Write()
		return
	}

	if err := prg.Start(); err != nil {
		syslog.L.Error(err).WithMessage("failed to start service").Write()
		os.Exit(1)
	}

	// Wait for termination signal
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig

	if err := prg.Stop(); err != nil {
		syslog.L.Error(err).WithMessage("failed to stop service").Write()
		os.Exit(1)
	}
}
