//go:build linux

package web

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	stdlog "log"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"net/http/pprof"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/web/api"
)

type Server struct {
	APIServer   *http.Server
	AgentServer *http.Server
	ARPCRouter  arpc.Router
	Store       *application.Runtime
	Version     string

	shutdownCh chan struct{}
	wg         sync.WaitGroup
}

func NewServer(app *application.Runtime, version string) (*Server, error) {
	apiLogger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	apiMux := http.NewServeMux()
	agentMux := http.NewServeMux()

	apiMux.HandleFunc("/api2/json/d2d/backup", ServerOnly(app, api.D2DBackupHandler(app)))
	apiMux.HandleFunc("/api2/json/d2d/restore", ServerOnly(app, api.D2DRestoreHandler(app)))
	apiMux.HandleFunc("/api2/json/d2d/target", ServerOnly(app, api.D2DTargetHandler(app)))
	apiMux.HandleFunc("/api2/json/d2d/target/tree", ServerOnly(app, api.D2DTargetTreeHandler(app)))
	apiMux.HandleFunc("/api2/json/d2d/script", ServerOnly(app, api.D2DScriptHandler(app)))
	apiMux.HandleFunc("/api2/json/d2d/token", ServerOnly(app, api.D2DTokenHandler(app)))
	apiMux.HandleFunc("/api2/json/d2d/filetree/{target}", ServerOnly(app, api.D2DFileTree(app)))
	apiMux.HandleFunc("/api2/json/d2d/exclusion", AgentOrServer(app, api.D2DExclusionHandler(app)))

	apiMux.HandleFunc("/api2/extjs/d2d/backup", ServerOnly(app, api.ExtJsBackupRunHandler(app)))
	apiMux.HandleFunc("/api2/extjs/d2d/backup/export", ServerOnly(app, api.ExtJsBackupCSVExportHandler(app)))
	apiMux.HandleFunc("/api2/extjs/d2d/restore", ServerOnly(app, api.ExtJsRestoreRunHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-target", ServerOnly(app, api.ExtJsTargetHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-target-status", ServerOnly(app, api.D2DTargetStatusHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-push-update", ServerOnly(app, api.ExtJsPushUpdateHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-target/{target}", ServerOnly(app, api.ExtJsTargetSingleHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-target/{target}/s3-secret", ServerOnly(app, api.ExtJsTargetS3SecretHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-agent/{agent}", ServerOnly(app, api.ExtJsAgentSingleHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-mount/{datastore}", ServerOnly(app, api.ExtJsMountHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-unmount/{datastore}", ServerOnly(app, api.ExtJsUnmountHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-unmount-all/{datastore}", ServerOnly(app, api.ExtJsUnmountAllHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-script", ServerOnly(app, api.ExtJsScriptHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-script/{path}", ServerOnly(app, api.ExtJsScriptSingleHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-token", ServerOnly(app, api.ExtJsTokenHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-token/{token}", ServerOnly(app, api.ExtJsTokenSingleHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-exclusion", ServerOnly(app, api.ExtJsExclusionHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-exclusion/{exclusion}", ServerOnly(app, api.ExtJsExclusionSingleHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/disk-backup", ServerOnly(app, api.ExtJsBackupHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/disk-backup/{backup}", ServerOnly(app, api.ExtJsBackupSingleHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/disk-backup/{backup}/upids", ServerOnly(app, api.ExtJsBackupUPIDsHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/disk-restore", ServerOnly(app, api.ExtJsRestoreHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/disk-restore/{restore}", ServerOnly(app, api.ExtJsRestoreSingleHandler(app)))
	apiMux.HandleFunc("/plus/agent/install/win", api.AgentInstallScriptHandler(app, version))
	apiMux.HandleFunc("/api2/json/d2d/verification", ServerOnly(app, api.D2DVerificationHandler(app)))
	apiMux.HandleFunc("/api2/extjs/d2d/verification", ServerOnly(app, api.ExtJsVerificationRunHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-verification", ServerOnly(app, api.ExtJsVerificationConfigHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-verification/{id}", ServerOnly(app, api.ExtJsVerificationConfigSingleHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-verification/{id}/results", ServerOnly(app, api.ExtJsVerificationResultsHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-verification/{id}/results/export", ServerOnly(app, api.VerificationResultsExportHandler(app)))
	apiMux.HandleFunc("/api2/json/d2d/verification/aggregate", ServerOnly(app, api.VerificationAggregateHandler(app)))

	apiMux.HandleFunc("/api2/extjs/d2d/mtf-job", ServerOnly(app, api.ExtJsMtfJobRunHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/mtf-job", ServerOnly(app, api.ExtJsMtfJobHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/mtf-job/{job}", ServerOnly(app, api.ExtJsMtfJobSingleHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/mtf-job/{job}/upids", ServerOnly(app, api.ExtJsMtfJobUPIDsHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/mtf-inventory", ServerOnly(app, api.ExtJsMtfInventoryHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/mtf-scan", ServerOnly(app, api.ExtJsMtfScanHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/mtf-mapping", ServerOnly(app, api.ExtJsMtfMappingHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/mtf-mapping/{id}", ServerOnly(app, api.ExtJsMtfMappingSingleHandler(app)))

	apiMux.HandleFunc("/api2/json/d2d/notification-batch", ServerOnly(app, api.NotificationBatchHandler(app)))
	apiMux.HandleFunc("/api2/json/d2d/notification-batch/jobs", ServerOnly(app, api.NotificationBatchJobsHandler(app)))
	apiMux.HandleFunc("/api2/json/d2d/notification-batch/status", ServerOnly(app, api.NotificationBatchStatusHandler(app)))

	apiMux.HandleFunc("/api2/json/d2d/alert-settings", ServerOnly(app, api.AlertSettingsHandler(app)))
	apiMux.HandleFunc("/api2/json/d2d/alert-settings/{name}", ServerOnly(app, api.AlertSettingSingleHandler(app)))
	apiMux.HandleFunc("/api2/json/d2d/alert-exclusions", ServerOnly(app, api.AlertExclusionsHandler(app)))
	apiMux.HandleFunc("/api2/json/d2d/alert-exclusions/{id}", ServerOnly(app, api.AlertExclusionSingleHandler(app)))

	apiMux.HandleFunc("/plus/metrics", api.PrometheusMetricsHandler(app))
	apiMux.HandleFunc("/api2/json/plus/ca-fingerprint", ServerOnly(app, api.CAFingerprintHandler(app)))

	agentMux.HandleFunc("/api2/json/plus/version", api.VersionHandler(app, version))
	agentMux.HandleFunc("/api2/json/plus/binary", api.DownloadBinaryHandler(app, version))
	agentMux.HandleFunc("/api2/json/plus/msi", api.DownloadMsiHandler(app, version))
	agentMux.HandleFunc("/api2/json/plus/binary/sig", api.DownloadSigHandler(app, version))
	agentMux.HandleFunc("/api2/json/plus/binary/ecdsa-sig", api.DownloadECDSASigHandler(app, version))
	agentMux.HandleFunc("/api2/json/plus/binary/checksum", api.DownloadChecksumHandler(app, version))
	agentMux.HandleFunc("/api2/json/d2d/target/agent", AgentOnly(app, api.D2DTargetAgentHandler(app)))
	agentMux.HandleFunc("/api2/json/d2d/agent-log", AgentOnly(app, api.AgentLogHandler(app)))

	agentMux.HandleFunc("/plus/agent/bootstrap", api.AgentBootstrapHandler(app))
	agentMux.HandleFunc("/plus/agent/renew", AgentOnly(app, api.AgentRenewHandler(app)))

	apiMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	apiMux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := app.CoreDB.Ping(ctx); err != nil {
			log.Error(err, "readiness check failed")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	apiMux.HandleFunc("/debug/pprof/", pprof.Index)
	apiMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	apiMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	apiMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	apiMux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	apiHandler := SecurityHeaders(RateLimit(Recovery(RequestLogger(apiLogger)(RequestID(apiMux)))))
	agentHandler := SecurityHeaders(RateLimit(Recovery(RequestLogger(apiLogger)(RequestID(agentMux)))))

	serverConfig, err := app.CertManager.APIServerTLSConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to build server TLS config: %w", err)
	}

	apiServer := &http.Server{
		Addr:           conf.ServerAPIExtPort,
		Handler:        apiHandler,
		ErrorLog:       stdlog.New(io.Discard, "", 0),
		ReadTimeout:    conf.HTTPReadTimeout,
		WriteTimeout:   conf.HTTPWriteTimeout,
		IdleTimeout:    conf.HTTPIdleTimeout,
		MaxHeaderBytes: conf.HTTPMaxHeaderBytes,
	}

	agentServer := &http.Server{
		Addr:           conf.AgentAPIPort,
		Handler:        agentHandler,
		TLSConfig:      serverConfig,
		ErrorLog:       stdlog.New(io.Discard, "", 0),
		ReadTimeout:    conf.HTTPReadTimeout,
		WriteTimeout:   conf.HTTPWriteTimeout,
		IdleTimeout:    conf.HTTPIdleTimeout,
		MaxHeaderBytes: conf.HTTPMaxHeaderBytes,
	}

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
		Store:       app,
		Version:     version,
		shutdownCh:  make(chan struct{}),
	}, nil
}

func (s *Server) StartARPC() error {
	arpcTlsConfig, err := s.Store.CertManager.ARPCServerTLSConfig()
	if err != nil {
		return fmt.Errorf("failed to build server TLS config: %w", err)
	}

	s.Store.Agents.SetExtraExpectFunc(func(id string, certs []*x509.Certificate) bool {
		if len(strings.Split(id, "|")) > 1 {
			return false
		}
		log.Debug("checking client authorization", "id", id)

		if len(certs) == 0 {
			log.Error(fmt.Errorf("no client certificates received"), "client unauthorized", "id", id)
			return false
		}

		trustedCert, err := s.Store.CoreDB.LoadAgentHostCert(id)
		if err != nil {
			log.Error(err, "client unauthorized", "id", id)
			return false
		}

		for _, cert := range certs {
			if cert.Equal(trustedCert) {
				log.Debug("client authorized", "id", id)
				return true
			}
		}
		log.Error(fmt.Errorf("did not match trusted certificate"), "client unauthorized", "id", id)
		return false
	})

	return arpc.ListenAndServe(s.Store.Ctx, conf.ARPCServerPort, s.Store.Agents, arpcTlsConfig, s.ARPCRouter)
}

func (s *Server) StartARPCQuic() error {
	arpcTlsConfig, err := s.Store.CertManager.ARPCServerTLSConfig()
	if err != nil {
		return fmt.Errorf("failed to build server TLS config: %w", err)
	}

	return arpc.ListenAndServeQuic(s.Store.Ctx, conf.ARPCQuicPort, s.Store.Agents, arpcTlsConfig, s.ARPCRouter)
}

func (s *Server) StartAll() {
	s.wg.Go(func() {
		WatchAndServe(s.APIServer, conf.CertFile, conf.KeyFile, []string{conf.CertFile, conf.KeyFile}, s.shutdownCh)
	})

	s.wg.Go(func() {
		log.Info(fmt.Sprintf("Starting agent endpoint on %s", s.AgentServer.Addr))
		if err := s.Store.CertManager.ServeTLS(s.AgentServer); err != nil {
			log.Error(err, "http agent endpoint server failed")
		}
	})

	s.wg.Go(func() {
		log.Info(fmt.Sprintf("arpc: endpoint starting on tcp %s", conf.ARPCServerPort))
		if err := s.StartARPC(); err != nil {
			log.Error(err, "arpc agent endpoint server failed")
		}
	})

	s.wg.Go(func() {
		log.Info(fmt.Sprintf("arpc: quic endpoint starting on udp %s", conf.ARPCQuicPort))
		if err := s.StartARPCQuic(); err != nil {
			log.Error(err, "arpc quic agent endpoint server failed")
		}
	})
}

func (s *Server) Shutdown(ctx context.Context) error {
	close(s.shutdownCh)
	log.Info("shutting down HTTP servers")

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
		return fmt.Errorf("shutdown errors: %w", errors.Join(errs...))
	}
	return nil
}
