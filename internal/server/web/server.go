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
	"github.com/pbs-plus/pbs-plus/internal/server/web/api/agentdist"
	"github.com/pbs-plus/pbs-plus/internal/server/web/api/backupapi"
	"github.com/pbs-plus/pbs-plus/internal/server/web/api/exclusionapi"
	"github.com/pbs-plus/pbs-plus/internal/server/web/api/metricsapi"
	"github.com/pbs-plus/pbs-plus/internal/server/web/api/mountapi"
	"github.com/pbs-plus/pbs-plus/internal/server/web/api/mtfapi"
	"github.com/pbs-plus/pbs-plus/internal/server/web/api/notificationapi"
	"github.com/pbs-plus/pbs-plus/internal/server/web/api/restoreapi"
	"github.com/pbs-plus/pbs-plus/internal/server/web/api/scriptapi"
	"github.com/pbs-plus/pbs-plus/internal/server/web/api/targetapi"
	"github.com/pbs-plus/pbs-plus/internal/server/web/api/tokenapi"
	"github.com/pbs-plus/pbs-plus/internal/server/web/api/verificationapi"
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

	apiMux.HandleFunc("/api2/json/d2d/backup", ServerOnly(app, backupapi.D2DBackupHandler(app)))
	apiMux.HandleFunc("/api2/json/d2d/restore", ServerOnly(app, restoreapi.D2DRestoreHandler(app)))
	apiMux.HandleFunc("/api2/json/d2d/target", ServerOnly(app, targetapi.D2DTargetHandler(app)))
	apiMux.HandleFunc("/api2/json/d2d/target/tree", ServerOnly(app, backupapi.D2DTargetTreeHandler(app)))
	apiMux.HandleFunc("/api2/json/d2d/script", ServerOnly(app, scriptapi.D2DScriptHandler(app)))
	apiMux.HandleFunc("/api2/json/d2d/token", ServerOnly(app, tokenapi.D2DTokenHandler(app)))
	apiMux.HandleFunc("/api2/json/d2d/filetree/{target}", ServerOnly(app, mountapi.D2DFileTree(app)))
	apiMux.HandleFunc("/api2/json/d2d/exclusion", AgentOrServer(app, exclusionapi.D2DExclusionHandler(app)))

	apiMux.HandleFunc("/api2/extjs/d2d/backup", ServerOnly(app, backupapi.ExtJsBackupRunHandler(app)))
	apiMux.HandleFunc("/api2/extjs/d2d/backup/export", ServerOnly(app, backupapi.ExtJsBackupCSVExportHandler(app)))
	apiMux.HandleFunc("/api2/extjs/d2d/restore", ServerOnly(app, restoreapi.ExtJsRestoreRunHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-target", ServerOnly(app, targetapi.ExtJsTargetHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-target-status", ServerOnly(app, targetapi.D2DTargetStatusHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-push-update", ServerOnly(app, targetapi.ExtJsPushUpdateHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-target/{target}", ServerOnly(app, targetapi.ExtJsTargetSingleHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-target/{target}/s3-secret", ServerOnly(app, targetapi.ExtJsTargetS3SecretHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-agent/{agent}", ServerOnly(app, targetapi.ExtJsAgentSingleHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-mount/{datastore}", ServerOnly(app, mountapi.ExtJsMountHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-mounts", ServerOnly(app, mountapi.ExtJsMountsHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-mounts/{datastore}", ServerOnly(app, mountapi.ExtJsMountsHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-unmount/{datastore}", ServerOnly(app, mountapi.ExtJsUnmountHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-unmount-all/{datastore}", ServerOnly(app, mountapi.ExtJsUnmountAllHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-commit/{datastore}", ServerOnly(app, mountapi.ExtJsCommitHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-init/{datastore}", ServerOnly(app, mountapi.ExtJsInitHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-compose/{datastore}", ServerOnly(app, mountapi.ExtJsComposeHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-mount-profiles", ServerOnly(app, mountapi.ExtJsMountProfilesHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-mount-profiles/{id}", ServerOnly(app, mountapi.ExtJsMountProfileSingleHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-mount-profiles/{id}/mount", ServerOnly(app, mountapi.ExtJsMountProfileMountHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-script", ServerOnly(app, scriptapi.ExtJsScriptHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-script/{path}", ServerOnly(app, scriptapi.ExtJsScriptSingleHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-token", ServerOnly(app, tokenapi.ExtJsTokenHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-token/{token}", ServerOnly(app, tokenapi.ExtJsTokenSingleHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-exclusion", ServerOnly(app, exclusionapi.ExtJsExclusionHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-exclusion/{exclusion}", ServerOnly(app, exclusionapi.ExtJsExclusionSingleHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/disk-backup", ServerOnly(app, backupapi.ExtJsBackupHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/disk-backup/{backup}", ServerOnly(app, backupapi.ExtJsBackupSingleHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/disk-backup/{backup}/upids", ServerOnly(app, backupapi.ExtJsBackupUPIDsHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/disk-restore", ServerOnly(app, restoreapi.ExtJsRestoreHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/disk-restore/{restore}", ServerOnly(app, restoreapi.ExtJsRestoreSingleHandler(app)))
	apiMux.HandleFunc("/plus/agent/install/win", agentdist.AgentInstallScriptHandler(app, version))
	apiMux.HandleFunc("/api2/json/d2d/verification", ServerOnly(app, verificationapi.D2DVerificationHandler(app)))
	apiMux.HandleFunc("/api2/extjs/d2d/verification", ServerOnly(app, verificationapi.ExtJsVerificationRunHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-verification", ServerOnly(app, verificationapi.ExtJsVerificationConfigHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-verification/{id}", ServerOnly(app, verificationapi.ExtJsVerificationConfigSingleHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-verification/{id}/results", ServerOnly(app, verificationapi.ExtJsVerificationResultsHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-verification/{id}/results/export", ServerOnly(app, verificationapi.VerificationResultsExportHandler(app)))
	apiMux.HandleFunc("/api2/json/d2d/verification/aggregate", ServerOnly(app, verificationapi.VerificationAggregateHandler(app)))

	apiMux.HandleFunc("/api2/extjs/d2d/mtf-job", ServerOnly(app, mtfapi.ExtJsMtfJobRunHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/mtf-job", ServerOnly(app, mtfapi.ExtJsMtfJobHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/mtf-job/{job}", ServerOnly(app, mtfapi.ExtJsMtfJobSingleHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/mtf-job/{job}/upids", ServerOnly(app, mtfapi.ExtJsMtfJobUPIDsHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/mtf-inventory", ServerOnly(app, mtfapi.ExtJsMtfInventoryHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/mtf-scan", ServerOnly(app, mtfapi.ExtJsMtfScanHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/mtf-mapping", ServerOnly(app, mtfapi.ExtJsMtfMappingHandler(app)))
	apiMux.HandleFunc("/api2/extjs/config/mtf-mapping/{id}", ServerOnly(app, mtfapi.ExtJsMtfMappingSingleHandler(app)))

	apiMux.HandleFunc("/api2/json/d2d/notification-batch", ServerOnly(app, notificationapi.NotificationBatchHandler(app)))
	apiMux.HandleFunc("/api2/json/d2d/notification-batch/jobs", ServerOnly(app, notificationapi.NotificationBatchJobsHandler(app)))
	apiMux.HandleFunc("/api2/json/d2d/notification-batch/status", ServerOnly(app, notificationapi.NotificationBatchStatusHandler(app)))

	apiMux.HandleFunc("/api2/json/d2d/alert-settings", ServerOnly(app, notificationapi.AlertSettingsHandler(app)))
	apiMux.HandleFunc("/api2/json/d2d/alert-settings/{name}", ServerOnly(app, notificationapi.AlertSettingSingleHandler(app)))
	apiMux.HandleFunc("/api2/json/d2d/alert-exclusions", ServerOnly(app, notificationapi.AlertExclusionsHandler(app)))
	apiMux.HandleFunc("/api2/json/d2d/alert-exclusions/{id}", ServerOnly(app, notificationapi.AlertExclusionSingleHandler(app)))

	apiMux.HandleFunc("/plus/metrics", metricsapi.PrometheusMetricsHandler(app))
	apiMux.HandleFunc("/api2/json/plus/ca-fingerprint", ServerOnly(app, agentdist.CAFingerprintHandler(app)))

	agentMux.HandleFunc("/api2/json/plus/version", agentdist.VersionHandler(app, version))
	agentMux.HandleFunc("/api2/json/plus/binary", agentdist.DownloadBinaryHandler(app, version))
	agentMux.HandleFunc("/api2/json/plus/msi", agentdist.DownloadMsiHandler(app, version))
	agentMux.HandleFunc("/api2/json/plus/binary/sig", agentdist.DownloadSigHandler(app, version))
	agentMux.HandleFunc("/api2/json/plus/binary/ecdsa-sig", agentdist.DownloadECDSASigHandler(app, version))
	agentMux.HandleFunc("/api2/json/plus/binary/checksum", agentdist.DownloadChecksumHandler(app, version))
	agentMux.HandleFunc("/api2/json/d2d/target/agent", AgentOnly(app, targetapi.D2DTargetAgentHandler(app)))
	agentMux.HandleFunc("/api2/json/d2d/agent-log", AgentOnly(app, targetapi.AgentLogHandler(app)))

	agentMux.HandleFunc("/plus/agent/bootstrap", targetapi.AgentBootstrapHandler(app))
	agentMux.HandleFunc("/plus/agent/renew", AgentOnly(app, targetapi.AgentRenewHandler(app)))

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

	resetSecret, err := os.ReadFile(s.Store.CertManager.ServerKeyPath)
	if err != nil {
		log.Error(err, "arpc: cannot read server key for quic stateless reset",
			"path", s.Store.CertManager.ServerKeyPath)
		resetSecret = nil
	}

	return arpc.ListenAndServeQuic(s.Store.Ctx, conf.ARPCQuicPort, s.Store.Agents, arpcTlsConfig, s.ARPCRouter, resetSecret)
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
