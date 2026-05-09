//go:build linux

package web

import (
	"context"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/web/api"
	"net/http/pprof"
)

// Server holds all HTTP servers and their configuration.
type Server struct {
	APIServer   *http.Server
	AgentServer *http.Server
	ARPCRouter  arpc.Router
	Store       *store.Store
	Version     string

	shutdownCh chan struct{}
	wg         sync.WaitGroup
}

// NewServer creates and configures all HTTP servers with middleware chains applied.
func NewServer(storeInstance *store.Store, version string) (*Server, error) {
	apiLogger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	apiMux := http.NewServeMux()
	agentMux := http.NewServeMux()

	// API routes
	apiMux.HandleFunc("/api2/json/d2d/backup", ServerOnly(storeInstance, api.D2DBackupHandler(storeInstance)))
	apiMux.HandleFunc("/api2/json/d2d/restore", ServerOnly(storeInstance, api.D2DRestoreHandler(storeInstance)))
	apiMux.HandleFunc("/api2/json/d2d/target", ServerOnly(storeInstance, api.D2DTargetHandler(storeInstance)))
	apiMux.HandleFunc("/api2/json/d2d/script", ServerOnly(storeInstance, api.D2DScriptHandler(storeInstance)))
	apiMux.HandleFunc("/api2/json/d2d/token", ServerOnly(storeInstance, api.D2DTokenHandler(storeInstance)))
	apiMux.HandleFunc("/api2/json/d2d/filetree/{target}", ServerOnly(storeInstance, api.D2DFileTree(storeInstance)))
	apiMux.HandleFunc("/api2/json/d2d/exclusion", AgentOrServer(storeInstance, api.D2DExclusionHandler(storeInstance)))

	// ExtJS routes
	apiMux.HandleFunc("/api2/extjs/d2d/backup", ServerOnly(storeInstance, api.ExtJsBackupRunHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/d2d/restore", ServerOnly(storeInstance, api.ExtJsRestoreRunHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-target", ServerOnly(storeInstance, api.ExtJsTargetHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-target/{target}", ServerOnly(storeInstance, api.ExtJsTargetSingleHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-target/{target}/s3-secret", ServerOnly(storeInstance, api.ExtJsTargetS3SecretHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-agent/{agent}", ServerOnly(storeInstance, api.ExtJsAgentSingleHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-mount/{datastore}", ServerOnly(storeInstance, api.ExtJsMountHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-unmount/{datastore}", ServerOnly(storeInstance, api.ExtJsUnmountHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-unmount-all/{datastore}", ServerOnly(storeInstance, api.ExtJsUnmountAllHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-script", ServerOnly(storeInstance, api.ExtJsScriptHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-script/{path}", ServerOnly(storeInstance, api.ExtJsScriptSingleHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-token", ServerOnly(storeInstance, api.ExtJsTokenHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-token/{token}", ServerOnly(storeInstance, api.ExtJsTokenSingleHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-exclusion", ServerOnly(storeInstance, api.ExtJsExclusionHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-exclusion/{exclusion}", ServerOnly(storeInstance, api.ExtJsExclusionSingleHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/disk-backup", ServerOnly(storeInstance, api.ExtJsBackupHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/disk-backup/{backup}", ServerOnly(storeInstance, api.ExtJsBackupSingleHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/disk-backup/{backup}/upids", ServerOnly(storeInstance, api.ExtJsBackupUPIDsHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/disk-restore", ServerOnly(storeInstance, api.ExtJsRestoreHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/disk-restore/{restore}", ServerOnly(storeInstance, api.ExtJsRestoreSingleHandler(storeInstance)))
	apiMux.HandleFunc("/plus/agent/install/win", api.AgentInstallScriptHandler(storeInstance, version))
	apiMux.HandleFunc("/plus/metrics", api.PrometheusMetricsHandler(storeInstance))

	// Agent routes
	agentMux.HandleFunc("/api2/json/plus/version", api.VersionHandler(storeInstance, version))
	agentMux.HandleFunc("/api2/json/plus/binary", api.DownloadBinaryHandler(storeInstance, version))
	agentMux.HandleFunc("/api2/json/plus/msi", api.DownloadMsiHandler(storeInstance, version))
	agentMux.HandleFunc("/api2/json/plus/binary/sig", api.DownloadSigHandler(storeInstance, version))
	agentMux.HandleFunc("/api2/json/plus/binary/checksum", api.DownloadChecksumHandler(storeInstance, version))
	agentMux.HandleFunc("/api2/json/d2d/target/agent", AgentOnly(storeInstance, api.D2DTargetAgentHandler(storeInstance)))
	agentMux.HandleFunc("/api2/json/d2d/agent-log", AgentOnly(storeInstance, api.AgentLogHandler(storeInstance)))

	// Agent auth routes
	agentMux.HandleFunc("/plus/agent/bootstrap", api.AgentBootstrapHandler(storeInstance))
	agentMux.HandleFunc("/plus/agent/renew", AgentOnly(storeInstance, api.AgentRenewHandler(storeInstance)))

	// Health probes
	apiMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	apiMux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := storeInstance.Database.Ping(ctx); err != nil {
			syslog.L.Error(err).WithMessage("readiness check failed").Write()
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	// pprof routes
	apiMux.HandleFunc("/debug/pprof/", pprof.Index)
	apiMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	apiMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	apiMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	apiMux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	// Apply middleware chain: RequestID → RequestLogger → Recovery
	apiHandler := Recovery(RequestLogger(apiLogger)(RequestID(apiMux)))
	agentHandler := Recovery(RequestLogger(apiLogger)(RequestID(agentMux)))

	// Build TLS config for agent server
	serverConfig, err := storeInstance.CertManager.APIServerTLSConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to build server TLS config: %w", err)
	}

	apiServer := &http.Server{
		Addr:           conf.ServerAPIExtPort,
		Handler:        apiHandler,
		ReadTimeout:    conf.HTTPReadTimeout,
		WriteTimeout:   conf.HTTPWriteTimeout,
		IdleTimeout:    conf.HTTPIdleTimeout,
		MaxHeaderBytes: conf.HTTPMaxHeaderBytes,
	}

	agentServer := &http.Server{
		Addr:           conf.AgentAPIPort,
		Handler:        agentHandler,
		TLSConfig:      serverConfig,
		ReadTimeout:    conf.HTTPReadTimeout,
		WriteTimeout:   conf.HTTPWriteTimeout,
		IdleTimeout:    conf.HTTPIdleTimeout,
		MaxHeaderBytes: conf.HTTPMaxHeaderBytes,
	}

	// ARPC router setup
	router := arpc.NewRouter()
	router.Handle("echo", func(req *arpc.Request) (arpc.Response, error) {
		var msg string
		if err := cbor.Unmarshal(req.Payload, &msg); err != nil {
			return arpc.Response{}, arpc.WrapError(err)
		}
		data, err := cbor.Marshal(msg)
		if err != nil {
			return arpc.Response{}, arpc.WrapError(err)
		}
		return arpc.Response{Status: 200, Data: data}, nil
	})

	return &Server{
		APIServer:   apiServer,
		AgentServer: agentServer,
		ARPCRouter:  router,
		Store:       storeInstance,
		Version:     version,
		shutdownCh:  make(chan struct{}),
	}, nil
}

// StartARPC starts the ARPC server with proper TLS configuration.
func (s *Server) StartARPC() error {
	arpcTlsConfig, err := s.Store.CertManager.ARPCServerTLSConfig()
	if err != nil {
		return fmt.Errorf("failed to build server TLS config: %w", err)
	}

	s.Store.ARPCAgentsManager.SetExtraExpectFunc(func(id string, certs []*x509.Certificate) bool {
		if len(strings.Split(id, "|")) > 1 {
			return false
		}

		syslog.L.Debug().WithMessage("checking client authorization").WithField("id", id).Write()

		if len(certs) == 0 {
			syslog.L.Error(fmt.Errorf("no client certificates received")).WithMessage("client unauthorized").WithField("id", id).Write()
			return false
		}

		trustedCert, err := s.Store.Database.LoadAgentHostCert(id)
		if err != nil {
			syslog.L.Error(err).WithMessage("client unauthorized").WithField("id", id).Write()
			return false
		}

		for _, cert := range certs {
			if cert.Equal(trustedCert) {
				syslog.L.Debug().WithMessage("client authorized").WithField("id", id).Write()
				return true
			}
		}

		syslog.L.Error(fmt.Errorf("did not match trusted certificate")).WithMessage("client unauthorized").WithField("id", id).Write()
		return false
	})

	return arpc.ListenAndServe(s.Store.Ctx, conf.ARPCServerPort, s.Store.ARPCAgentsManager, arpcTlsConfig, s.ARPCRouter)
}

// StartAll starts all HTTP and ARPC servers in background goroutines.
func (s *Server) StartAll() {
	s.wg.Go(func() {
		WatchAndServe(s.APIServer, conf.CertFile, conf.KeyFile, []string{conf.CertFile, conf.KeyFile}, s.shutdownCh)
	})

	s.wg.Go(func() {
		syslog.L.Info().WithMessage(fmt.Sprintf("Starting agent endpoint on %s", s.AgentServer.Addr)).Write()
		if err := s.Store.CertManager.ServeTLS(s.AgentServer); err != nil {
			syslog.L.Error(err).WithMessage("http agent endpoint server failed")
		}
	})

	s.wg.Go(func() {
		syslog.L.Info().WithMessage(fmt.Sprintf("Starting aRPC endpoint on TCP %s", conf.ARPCServerPort)).Write()
		if err := s.StartARPC(); err != nil {
			syslog.L.Error(err).WithMessage("arpc agent endpoint server failed")
		}
	})
}

// Shutdown gracefully stops all servers, allowing in-flight requests to complete.
func (s *Server) Shutdown(ctx context.Context) error {
	close(s.shutdownCh)
	syslog.L.Info().WithMessage("shutting down HTTP servers").Write()

	shutdownCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var errs []error
	if err := s.APIServer.Shutdown(shutdownCtx); err != nil {
		errs = append(errs, fmt.Errorf("api server: %w", err))
	}
	if err := s.AgentServer.Shutdown(shutdownCtx); err != nil {
		errs = append(errs, fmt.Errorf("agent server: %w", err))
	}

	s.wg.Wait()

	if len(errs) > 0 {
		return fmt.Errorf("shutdown errors: %v", errs)
	}
	return nil
}
