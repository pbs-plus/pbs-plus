package lifecycle

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent"
	"github.com/pbs-plus/pbs-plus/internal/agent/controllers"
	"github.com/pbs-plus/pbs-plus/internal/agent/registry"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils"
)

func isCertError(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "x509:") ||
		strings.Contains(msg, "tls:") ||
		strings.Contains(msg, "certificate")
}

func ClearCertificates() {
	_ = registry.DeleteEntry(registry.AUTH, "ServerCA")
	_ = registry.DeleteEntry(registry.AUTH, "Cert")
	_ = registry.DeleteEntry(registry.AUTH, "Priv")
	agent.InvalidateTLSConfigCache()
}

func UpdateDrives() error {
	hostname, err := utils.GetAgentHostname()
	if err != nil {
		return err
	}
	drives, err := utils.GetLocalDrives()
	if err != nil {
		return err
	}
	reqBody, err := json.Marshal(map[string]any{
		"hostname": hostname,
		"drives":   drives,
		"os":       runtime.GOOS,
	})
	if err != nil {
		return err
	}
	syslog.L.Debug().
		WithField("body", reqBody).
		WithMessage("updating target").
		Write()
	resp, err := agent.AgentHTTPRequest(
		http.MethodPost,
		"/api2/json/d2d/target/agent",
		bytes.NewBuffer(reqBody),
		nil,
	)
	if err != nil {
		return err
	}
	defer resp.Close()
	_, _ = io.Copy(io.Discard, resp)
	return nil
}

func WaitForServerURL(ctx context.Context) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		entry, err := registry.GetEntry(registry.CONFIG, "ServerURL", false)
		if err == nil && entry != nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func WaitForBootstrap(ctx context.Context) error {
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
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func ConnectARPC(
	ctx context.Context,
	version string,
) (<-chan error, error) {
	serverUrl, err := registry.GetEntry(registry.CONFIG, "ServerURL", false)
	if err != nil {
		return nil, err
	}
	uri, err := utils.ParseURI(serverUrl.Value)
	if err != nil {
		return nil, err
	}
	tlsConfig, err := agent.GetTLSConfig()
	if err != nil {
		return nil, err
	}
	clientId, err := utils.GetAgentHostname()
	if err != nil {
		return nil, err
	}

	address := fmt.Sprintf(
		"%s%s",
		strings.TrimSuffix(uri.Hostname(), ":"),
		constants.ARPCServerPort,
	)
	headers := http.Header{}
	headers.Add("X-PBS-Agent", clientId)
	headers.Add("X-PBS-Plus-Version", version)

	certErrCh := make(chan error, 1)

	signalCertError := func(err error) {
		select {
		case certErrCh <- err:
		default:
		}
	}

	go func() {
		defer close(certErrCh)

		base := 500 * time.Millisecond
		maxWait := 30 * time.Second
		factor := 2.0
		jitter := 0.2
		backoff := base
		var session *arpc.StreamPipe

		for {
			select {
			case <-ctx.Done():
				if session != nil {
					session.Close()
				}
				return
			default:
			}

			var connErr error
			session, connErr = arpc.ConnectToServer(
				ctx, address, headers, tlsConfig,
			)
			if connErr != nil {
				if isCertError(connErr) {
					syslog.L.Error(connErr).
						WithMessage("certificate error on connect, requesting re-bootstrap").
						Write()
					signalCertError(connErr)
					return
				}

				sleep := min(
					time.Duration(
						float64(backoff)*(1+jitter*(2*rand.Float64()-1)),
					),
					maxWait,
				)
				select {
				case <-ctx.Done():
					return
				case <-time.After(sleep):
					backoff = min(
						time.Duration(float64(backoff)*factor),
						maxWait,
					)
					continue
				}
			}

			router := arpc.NewRouter()
			router.Handle(
				"ping",
				func(req *arpc.Request) (arpc.Response, error) {
					b, _ := cbor.Marshal(map[string]string{
						"version":  version,
						"hostname": clientId,
					})
					return arpc.Response{Status: 200, Data: b}, nil
				},
			)
			router.Handle(
				"backup",
				func(req *arpc.Request) (arpc.Response, error) {
					return controllers.BackupStartHandler(req, session)
				},
			)
			router.Handle(
				"restore",
				func(req *arpc.Request) (arpc.Response, error) {
					return controllers.RestoreStartHandler(req, session)
				},
			)
			router.Handle(
				"filetree",
				func(req *arpc.Request) (arpc.Response, error) {
					return controllers.FileTreeHandler(req, session)
				},
			)
			router.Handle("target_status", controllers.StatusHandler)
			router.Handle("cleanup", controllers.BackupCloseHandler)
			router.Handle("cleanup_restore", controllers.RestoreCloseHandler)
			session.SetRouter(router)
			backoff = base

			serveErr := session.Serve()
			session.Close()
			session = nil

			if serveErr != nil && isCertError(serveErr) {
				syslog.L.Error(serveErr).
					WithMessage("certificate error during session, requesting re-bootstrap").
					Write()
				signalCertError(serveErr)
				return
			}
		}
	}()

	return certErrCh, nil
}
