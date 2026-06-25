package lifecycle

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent"
	"github.com/pbs-plus/pbs-plus/internal/agent/cli"
	"github.com/pbs-plus/pbs-plus/internal/agent/registry"
	"github.com/pbs-plus/pbs-plus/internal/agent/sync"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/host"
	"github.com/pbs-plus/pbs-plus/internal/log"
)

// until manually re-bootstrapped or re-added by an admin.
var ErrHostNotExpected = errors.New("host not expected by server - requires re-bootstrap")

func isCertError(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "x509:") ||
		strings.Contains(msg, "tls:") ||
		strings.Contains(msg, "certificate")
}

// IsHostNotExpected checks if an error indicates the agent host was
// because ConnectARPC uses plain hostname without X-PBS-Plus-BackupID
func IsHostNotExpected(err error) bool {
	if errors.Is(err, ErrHostNotExpected) {
		return true
	}
	msg := err.Error()
	// Server-side rejection from AgentsManager.isExpected:
	// HTTP middleware rejection from checkAgentAuth when
	//   "CheckAgentAuth: certificate not trusted"
	return strings.Contains(msg, "is not expected by server") ||
		strings.Contains(msg, "CheckAgentAuth: certificate not trusted")
}

func ClearCertificates() {
	if err := registry.DeleteEntry(registry.AUTH, "ServerCA"); err != nil {
		log.Error(err, "")
	}
	if err := registry.DeleteEntry(registry.AUTH, "Cert"); err != nil {
		log.Error(err, "")
	}
	if err := registry.DeleteEntry(registry.AUTH, "Priv"); err != nil {
		log.Error(err, "")
	}
	agent.InvalidateTLSConfigCache()
}

func UpdateDrives() error {
	hostname, err := host.AgentHostname()
	if err != nil {
		return err
	}
	drives, err := agent.GetLocalDrives()
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
	log.Debug("updating target", "body", reqBody)

	resp, err := agent.AgentHTTPRequest(
		http.MethodPost,
		"/api2/json/d2d/target/agent",
		bytes.NewBuffer(reqBody),
		nil,
	)
	if err != nil {
		return err
	}
	if _, err := io.Copy(io.Discard, resp); err != nil {
		log.Error(err, "")
	}
	if err := resp.Close(); err != nil {
		log.Error(err, "")
	}
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
		serverCA, errCA := registry.GetEntry(registry.AUTH, "ServerCA", true)
		cert, errCert := registry.GetEntry(registry.AUTH, "Cert", true)
		priv, errPriv := registry.GetEntry(registry.AUTH, "Priv", true)

		if errCA == nil && errCert == nil && errPriv == nil && serverCA != nil && cert != nil && priv != nil {
			if err := agent.RenewCertificateIfExpiring(); err == nil {
				return nil
			}
		} else {
			if err := agent.Bootstrap(); err != nil {
				log.Error(err, "")
			}
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
	uri, err := agent.ParseURI(serverUrl.Value)
	if err != nil {
		return nil, err
	}
	tlsConfig, err := agent.GetTLSConfig()
	if err != nil {
		return nil, err
	}
	clientId, err := host.AgentHostname()
	if err != nil {
		return nil, err
	}

	address := fmt.Sprintf(
		"%s%s",
		strings.TrimSuffix(uri.Hostname(), ":"),
		conf.ARPCServerPort,
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
		var session arpc.Session

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
			session, connErr = arpc.DialQuic(
				ctx, address, tlsConfig, headers,
			)
			if connErr != nil {
				if isCertError(connErr) {
					log.Error(connErr,
						"certificate error on connect, requesting re-bootstrap")

					signalCertError(connErr)
					return
				}

				if IsHostNotExpected(connErr) {
					log.Warn("host not expected by server, stopping retries until re-bootstrap",
						"error", connErr.Error())

					signalCertError(ErrHostNotExpected)
					return
				}
				log.Warn("qUIC connection failed, falling back to TCP/mTLS", "error", connErr.Error())

				var tcpPipe *arpc.StreamPipe
				tcpPipe, connErr = arpc.ConnectToServer(ctx, address, headers, tlsConfig)
				if connErr == nil {
					session = tcpPipe
					log.Info("tCP/mTLS fallback connection established")

				}
			}

			if connErr != nil {
				if isCertError(connErr) {
					log.Error(connErr,
						"certificate error on connect, requesting re-bootstrap")

					signalCertError(connErr)
					return
				}

				if IsHostNotExpected(connErr) {
					log.Warn("host not expected by server, stopping retries until re-bootstrap",
						"error", connErr.Error())

					signalCertError(ErrHostNotExpected)
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

			session.SetHeaders(headers)

			router := arpc.NewRouter()
			router.Handle(
				"ping",
				func(req *arpc.Request) (arpc.Response, error) {
					b, err := cbor.Marshal(map[string]string{
						"version":  version,
						"hostname": clientId,
					})
					if err != nil {
						return arpc.Response{}, err
					}
					return arpc.Response{Status: 200, Data: b}, nil
				},
			)
			router.Handle(
				"backup",
				func(req *arpc.Request) (arpc.Response, error) {
					return sync.BackupStartHandler(req, nil)
				},
			)
			router.Handle(
				"restore",
				func(req *arpc.Request) (arpc.Response, error) {
					return sync.RestoreStartHandler(req, nil)
				},
			)
			router.Handle(
				"filetree",
				func(req *arpc.Request) (arpc.Response, error) {
					return sync.FileTreeHandler(req, nil)
				},
			)
			router.Handle("target_status", sync.StatusHandler)
			router.Handle("cleanup", sync.BackupCloseHandler)
			router.Handle("cleanup_restore", sync.RestoreCloseHandler)
			router.Handle("verify_start", cli.VerifyStartHandler)
			session.SetRouter(router)
			backoff = base

			serveErr := session.Serve()
			session.Close()
			session = nil

			if serveErr != nil {
				if isCertError(serveErr) {
					log.Error(serveErr,
						"certificate error during session, requesting re-bootstrap")

					signalCertError(serveErr)
					return
				}
				// QUIC closes the connection without a rejection frame
				tcpPipe, probeErr := arpc.ConnectToServer(ctx, address, headers, tlsConfig)
				if probeErr != nil {
					tcpPipe = nil
					if IsHostNotExpected(probeErr) {
						log.Warn("host not expected by server (detected via probe), stopping retries until re-bootstrap",
							"error", probeErr.Error())

						signalCertError(ErrHostNotExpected)
						return
					}
				}
				if tcpPipe != nil {
					tcpPipe.Close()
				}
			}
		}
	}()

	return certErrCh, nil
}
