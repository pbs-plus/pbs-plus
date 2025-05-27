//go:build linux

package server

import (
	"fmt"
	"net/http"
	"net/http/pprof"
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/auth/server"
	"github.com/pbs-plus/pbs-plus/internal/proxy/controllers/agents"
	"github.com/pbs-plus/pbs-plus/internal/proxy/controllers/arpc"
	"github.com/pbs-plus/pbs-plus/internal/proxy/controllers/exclusions"
	"github.com/pbs-plus/pbs-plus/internal/proxy/controllers/jobs"
	"github.com/pbs-plus/pbs-plus/internal/proxy/controllers/plus"
	"github.com/pbs-plus/pbs-plus/internal/proxy/controllers/scripts"
	"github.com/pbs-plus/pbs-plus/internal/proxy/controllers/targets"
	"github.com/pbs-plus/pbs-plus/internal/proxy/controllers/tokens"
	mw "github.com/pbs-plus/pbs-plus/internal/proxy/middlewares"
	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

var Version = "v0.0.0"

func StartServers(serverConfig *server.Config, storeInstance *store.Store) error {
	serverTLSConfig, err := createServerTLSConfig()
	if err != nil {
		return fmt.Errorf("failed to create server TLS config: %w", err)
	}

	apiMux := http.NewServeMux()

	apiMux.HandleFunc("/plus/token", mw.ServerOnly(mw.CORS(storeInstance, plus.TokenHandler(storeInstance))))
	apiMux.HandleFunc("/api2/json/plus/version", mw.ServerOnly(mw.CORS(storeInstance, plus.VersionHandler(storeInstance, Version))))
	apiMux.HandleFunc("/api2/json/plus/binary", mw.CORS(storeInstance, plus.DownloadBinary(storeInstance, Version)))
	apiMux.HandleFunc("/api2/json/plus/updater-binary", mw.CORS(storeInstance, plus.DownloadUpdater(storeInstance, Version)))
	apiMux.HandleFunc("/api2/json/plus/binary/checksum", mw.ServerOnly(mw.CORS(storeInstance, plus.DownloadChecksum(storeInstance, Version))))
	apiMux.HandleFunc("/api2/json/d2d/backup", mw.ServerOnly(mw.CORS(storeInstance, jobs.D2DJobHandler(storeInstance))))
	apiMux.HandleFunc("/api2/json/d2d/target", mw.ServerOnly(mw.CORS(storeInstance, targets.D2DTargetHandler(storeInstance))))
	apiMux.HandleFunc("/api2/json/d2d/script", mw.ServerOnly(mw.CORS(storeInstance, scripts.D2DScriptHandler(storeInstance))))
	apiMux.HandleFunc("/api2/json/d2d/token", mw.ServerOnly(mw.CORS(storeInstance, tokens.D2DTokenHandler(storeInstance))))
	apiMux.HandleFunc("/api2/json/d2d/exclusion", mw.ServerOnly(mw.CORS(storeInstance, exclusions.D2DExclusionHandler(storeInstance))))

	apiMux.HandleFunc("/api2/extjs/d2d/backup/{job}", mw.ServerOnly(mw.CORS(storeInstance, jobs.ExtJsJobRunHandler(storeInstance))))
	apiMux.HandleFunc("/api2/extjs/config/d2d-target", mw.ServerOnly(mw.CORS(storeInstance, targets.ExtJsTargetHandler(storeInstance))))
	apiMux.HandleFunc("/api2/extjs/config/d2d-target/{target}", mw.ServerOnly(mw.CORS(storeInstance, targets.ExtJsTargetSingleHandler(storeInstance))))
	apiMux.HandleFunc("/api2/extjs/config/d2d-script", mw.ServerOnly(mw.CORS(storeInstance, scripts.ExtJsScriptHandler(storeInstance))))
	apiMux.HandleFunc("/api2/extjs/config/d2d-script/{path}", mw.ServerOnly(mw.CORS(storeInstance, scripts.ExtJsScriptSingleHandler(storeInstance))))
	apiMux.HandleFunc("/api2/extjs/config/d2d-token", mw.ServerOnly(mw.CORS(storeInstance, tokens.ExtJsTokenHandler(storeInstance))))
	apiMux.HandleFunc("/api2/extjs/config/d2d-token/{token}", mw.ServerOnly(mw.CORS(storeInstance, tokens.ExtJsTokenSingleHandler(storeInstance))))
	apiMux.HandleFunc("/api2/extjs/config/d2d-exclusion", mw.ServerOnly(mw.CORS(storeInstance, exclusions.ExtJsExclusionHandler(storeInstance))))
	apiMux.HandleFunc("/api2/extjs/config/d2d-exclusion/{exclusion}", mw.ServerOnly(mw.CORS(storeInstance, exclusions.ExtJsExclusionSingleHandler(storeInstance))))
	apiMux.HandleFunc("/api2/extjs/config/disk-backup-job", mw.ServerOnly(mw.CORS(storeInstance, jobs.ExtJsJobHandler(storeInstance))))
	apiMux.HandleFunc("/api2/extjs/config/disk-backup-job/{job}", mw.ServerOnly(mw.CORS(storeInstance, jobs.ExtJsJobSingleHandler(storeInstance))))

	apiMux.HandleFunc("/plus/agent/bootstrap", mw.CORS(storeInstance, agents.AgentBootstrapHandler(storeInstance)))
	apiMux.HandleFunc("/plus/agent/install/win", mw.CORS(storeInstance, plus.AgentInstallScriptHandler(storeInstance, Version)))

	// pprof routes
	apiMux.HandleFunc("/debug/pprof/", pprof.Index)
	apiMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	apiMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	apiMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	apiMux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	apiServer := &http.Server{
		Addr:           serverConfig.Address,
		Handler:        apiMux,
		TLSConfig:      serverTLSConfig,
		ReadTimeout:    serverConfig.ReadTimeout,
		WriteTimeout:   serverConfig.WriteTimeout,
		IdleTimeout:    serverConfig.IdleTimeout,
		MaxHeaderBytes: serverConfig.MaxHeaderBytes,
	}

	agentsMux := http.NewServeMux()

	agentsMux.HandleFunc("/api2/json/plus/version", mw.AgentOnly(storeInstance, mw.CORS(storeInstance, plus.VersionHandler(storeInstance, Version))))
	agentsMux.HandleFunc("/api2/json/plus/binary/checksum", mw.AgentOnly(storeInstance, mw.CORS(storeInstance, plus.DownloadChecksum(storeInstance, Version))))
	agentsMux.HandleFunc("/api2/json/d2d/target/agent", mw.AgentOnly(storeInstance, mw.CORS(storeInstance, targets.D2DTargetAgentHandler(storeInstance))))
	agentsMux.HandleFunc("/api2/json/d2d/exclusion", mw.AgentOnly(storeInstance, mw.CORS(storeInstance, exclusions.D2DExclusionHandler(storeInstance))))
	agentsMux.HandleFunc("/api2/json/d2d/agent-log", mw.AgentOnly(storeInstance, mw.CORS(storeInstance, agents.AgentLogHandler(storeInstance))))

	agentsMux.HandleFunc("/plus/arpc", mw.AgentOnly(storeInstance, arpc.ARPCHandler(storeInstance)))
	agentsMux.HandleFunc("/plus/agent/renew", mw.AgentOnly(storeInstance, mw.CORS(storeInstance, agents.AgentRenewHandler(storeInstance))))

	agentTLSConfig, err := serverConfig.LoadTLSConfig()
	if err != nil {
		return err
	}

	agentServer := &http.Server{
		Addr:           serverConfig.AgentAddress,
		Handler:        agentsMux,
		TLSConfig:      agentTLSConfig,
		ReadTimeout:    serverConfig.ReadTimeout,
		WriteTimeout:   serverConfig.WriteTimeout,
		IdleTimeout:    serverConfig.IdleTimeout,
		MaxHeaderBytes: serverConfig.MaxHeaderBytes,
	}

	var wg sync.WaitGroup
	errChan := make(chan error, 2)

	wg.Add(1)
	go func() {
		defer wg.Done()
		syslog.L.Info().WithMessage(fmt.Sprintf("starting API server on %s", serverConfig.Address)).Write()
		if err := apiServer.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
			errChan <- fmt.Errorf("API server failed: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		syslog.L.Info().WithMessage(fmt.Sprintf("starting Agent server on %s", serverConfig.AgentAddress)).Write()
		if err := agentServer.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
			errChan <- fmt.Errorf("Agent server failed: %w", err)
		}
	}()

	// Wait for either server to fail or context cancellation
	go func() {
		wg.Wait()
		close(errChan)
	}()

	// Return first error if any
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}
