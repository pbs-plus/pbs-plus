//go:build unix

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent"
	"github.com/pbs-plus/pbs-plus/internal/agent/controllers"
	"github.com/pbs-plus/pbs-plus/internal/agent/forks"
	"github.com/pbs-plus/pbs-plus/internal/agent/registry"
	"github.com/pbs-plus/pbs-plus/internal/agent/updater"
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

func initializeDrives() error {
	syslog.L.Info().WithMessage("Initializing backup store").Write()

	hostname, err := utils.GetAgentHostname()
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

	resp, err := agent.AgentHTTPRequest(
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

func waitForServerURL(ctx context.Context) error {
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
		case <-ctx.Done():
			syslog.L.Info().WithMessage("Context cancelled while waiting for config").Write()
			return fmt.Errorf("context cancelled while waiting for server URL")
		case <-ticker.C:
		}
	}
}

func waitForBootstrap(ctx context.Context) error {
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
		case <-ctx.Done():
			syslog.L.Info().WithMessage("Context cancelled while waiting for bootstrap").Write()
			return fmt.Errorf("context cancelled while waiting for bootstrap")
		case <-ticker.C:
		}
	}
}

func connectARPC(ctx context.Context) error {
	syslog.L.Info().WithMessage("Retrieving server URL for ARPC connection").Write()
	serverUrl, err := registry.GetEntry(registry.CONFIG, "ServerURL", false)
	if err != nil {
		syslog.L.Error(err).WithMessage("Failed to retrieve server URL").Write()
		return fmt.Errorf("invalid server URL: %v", err)
	}

	syslog.L.Info().WithMessage("Parsing server URL").Write()
	uri, err := utils.ParseURI(serverUrl.Value)
	if err != nil {
		syslog.L.Error(err).WithMessage("Failed to parse server URL").Write()
		return fmt.Errorf("invalid server URL: %v", err)
	}
	syslog.L.Info().WithMessage("Server URL parsed successfully").WithField("host", uri.Hostname()).Write()

	syslog.L.Info().WithMessage("Getting TLS configuration").Write()
	tlsConfig, err := agent.GetTLSConfig()
	if err != nil {
		syslog.L.Error(err).WithMessage("Failed to get TLS configuration").Write()
		return err
	}
	syslog.L.Info().WithMessage("TLS configuration obtained successfully").Write()

	syslog.L.Info().WithMessage("Getting hostname for ARPC headers").Write()
	clientId, err := utils.GetAgentHostname()
	if err != nil {
		syslog.L.Error(err).WithMessage("Failed to get hostname").Write()
		return err
	}
	syslog.L.Info().WithMessage("Hostname obtained").WithField("hostname", clientId).Write()

	headers := http.Header{}
	headers.Add("X-PBS-Agent", clientId)
	headers.Add("X-PBS-Plus-Version", Version)
	syslog.L.Info().WithMessage("ARPC headers prepared").WithField("version", Version).Write()

	syslog.L.Info().WithMessage("Attempting ARPC connection to server").Write()
	session, err := arpc.ConnectToServer(ctx, fmt.Sprintf("%s%s", strings.TrimSuffix(uri.Hostname(), ":"), constants.ARPCServerPort), headers, tlsConfig)
	if err != nil {
		syslog.L.Error(err).WithMessage("Failed to connect to ARPC server").Write()
		return err
	}
	syslog.L.Info().WithMessage("ARPC session established").Write()

	syslog.L.Info().WithMessage("Setting up ARPC router and handlers").Write()
	router := arpc.NewRouter()
	router.Handle("ping", func(req *arpc.Request) (arpc.Response, error) {
		resp := map[string]string{"version": Version, "hostname": clientId}
		b, err := cbor.Marshal(resp)
		if err != nil {
			return arpc.Response{}, err
		}
		return arpc.Response{Status: 200, Data: b}, nil
	})
	router.Handle("backup", func(req *arpc.Request) (arpc.Response, error) {
		return controllers.BackupStartHandler(req, session)
	})
	router.Handle("restore", func(req *arpc.Request) (arpc.Response, error) {
		return controllers.RestoreStartHandler(req, session)
	})
	router.Handle("target_status", controllers.StatusHandler)
	router.Handle("cleanup", controllers.BackupCloseHandler)
	router.Handle("cleanup_restore", controllers.RestoreCloseHandler)
	session.SetRouter(router)
	syslog.L.Info().WithMessage("ARPC router configured successfully").Write()

	syslog.L.Info().WithMessage("Starting ARPC session handler goroutine").Write()
	go func(pipe *arpc.StreamPipe) {
		defer func() {
			syslog.L.Info().WithMessage("ARPC session handler shutting down").Write()
		}()

		base := 500 * time.Millisecond
		maxWait := 5 * time.Second
		factor := 2.0
		jitter := 0.2

		backoff := base

		for {
			select {
			case <-ctx.Done():
				return
			default:
				if err := pipe.Serve(); err != nil {
					mult := 1 + jitter*(2*rand.Float64()-1)
					sleep := time.Duration(float64(backoff) * mult)
					sleep = min(sleep, maxWait)

					syslog.L.Warn().WithMessage(fmt.Sprintf("ARPC connection error, retrying after %v", sleep)).WithField("error", err.Error()).Write()

					select {
					case <-ctx.Done():
						return
					case <-time.After(sleep):
					}

					next := time.Duration(float64(backoff) * factor)
					next = min(next, maxWait)

					backoff = next
				}
			}
		}
	}(session)

	return nil
}

func run() {
	if err := syslog.L.SetServiceLogger(); err != nil {
		log.Println(err)
	}

	syslog.L.Info().WithMessage("PBS Plus Agent service starting with version " + Version).Write()

	// Ensure registry defaults exist
	_ = registry.CreateEntryIfNotExists(&registry.RegistryEntry{
		Path:     registry.CONFIG,
		Key:      "ServerURL",
		Value:    os.Getenv("PBS_PLUS_INIT_SERVER_URL"),
		IsSecret: false,
	})
	_ = registry.CreateEntryIfNotExists(&registry.RegistryEntry{
		Path:     registry.CONFIG,
		Key:      "BootstrapToken",
		Value:    os.Getenv("PBS_PLUS_INIT_BOOTSTRAP_TOKEN"),
		IsSecret: false,
	})

	// Context and lifecycle
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	// Background: cert renew hourly
	syslog.L.Info().WithMessage("Starting certificate renewal background task").Write()
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
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
	}()

	// Backup store clear on start
	syslog.L.Info().WithMessage("Initializing backup store").Write()
	if store, err := agent.NewBackupStore(); err != nil {
		syslog.L.Warn().WithMessage("Failed to initialize backup store").WithField("error", err.Error()).Write()
	} else if err := store.ClearAll(); err != nil {
		syslog.L.Warn().WithMessage("Failed to clear backup store").WithField("error", err.Error()).Write()
	} else {
		syslog.L.Info().WithMessage("Backup store initialized and cleared successfully").Write()
	}

	// Run sequence
	syslog.L.Info().WithMessage("Waiting for PBS Plus Agent config").Write()
	if err := waitForServerURL(ctx); err != nil {
		syslog.L.Error(err).WithMessage("Failed to get configuration").Write()
		return
	}
	syslog.L.Info().WithMessage("Configuration acquired successfully").Write()

	syslog.L.Info().WithMessage("Starting bootstrap process").Write()
	if err := waitForBootstrap(ctx); err != nil {
		syslog.L.Error(err).WithMessage("Failed to bootstrap").Write()
		return
	}
	syslog.L.Info().WithMessage("Bootstrap completed successfully").Write()

	// Initialize drives once (async)
	syslog.L.Info().WithMessage("Starting drive update background task").Write()
	wg.Add(1)
	go func() {
		defer wg.Done()
		syslog.L.Info().WithMessage("Running initial drive update").Write()
		if err := initializeDrives(); err != nil {
			syslog.L.Warn().WithMessage("Initial drive update failed").WithField("error", err.Error()).Write()
		} else {
			syslog.L.Info().WithMessage("Initial drive update completed successfully").Write()
		}
	}()

	// Connect ARPC
	syslog.L.Info().WithMessage("Attempting to connect to ARPC").Write()
	if err := connectARPC(ctx); err != nil {
		syslog.L.Error(err).WithMessage("Failed to connect to ARPC").Write()
		return
	}
	syslog.L.Info().WithMessage("ARPC connection established successfully").Write()

	syslog.L.Info().WithMessage("PBS Plus Agent fully initialized and running").Write()

	// Periodic drives init like original
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(utils.ComputeDelay())
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				syslog.L.Info().WithMessage("Drive update task shutting down").Write()
				return
			case <-ticker.C:
				syslog.L.Info().WithMessage("Running scheduled drive update").Write()
				if err := initializeDrives(); err != nil {
					syslog.L.Warn().WithMessage("Scheduled drive update failed").WithField("error", err.Error()).Write()
				} else {
					syslog.L.Info().WithMessage("Scheduled drive update completed successfully").Write()
				}
				ticker.Reset(utils.ComputeDelay())
			}
		}
	}()

	// Wait for termination
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	syslog.L.Info().WithMessage("Context cancelled, shutting down").Write()
	cancel()
	wg.Wait()
	syslog.L.Info().WithMessage("Service stopped gracefully").Write()
}

func main() {
	defer func() {
		if r := recover(); r != nil {
			msg := fmt.Sprintf("Panic occurred: %v\nStack trace:\n%s", r, debug.Stack())

			logFile, err := os.OpenFile("panic.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
			if err == nil {
				defer logFile.Close()
				_, _ = logFile.WriteString(msg)
			} else {
				fmt.Println("Error opening log file:", err)
			}

			os.Exit(1)
		}
	}()

	forks.CmdForkEntry()
	constants.Version = Version

	dockerEnv := os.Getenv("PBS_PLUS__I_AM_INSIDE_CONTAINER")
	if updateDisabled := os.Getenv("PBS_PLUS_DISABLE_AUTO_UPDATE"); updateDisabled != "true" && dockerEnv != "true" {
		_, err := updater.New(updater.Config{
			MinConstraint: ">= 0.52.0",
			PollInterval:  2 * time.Minute,
			FetchOnStart:  true,

			UpgradeConfirm: func(newVersion string) bool {
				syslog.L.Info().WithMessage("attempting to update agent").WithField("version", newVersion).Write()
				return true
			},
			Exit: func(err error) {
				if err != nil {
					syslog.L.Error(err).WithMessage("Updater exit with error").Write()
				}
			},
		})
		if err != nil {
			syslog.L.Error(err).WithMessage("failed to init updater").Write()
		}
	}

	run()
}
