package lifecycle

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strings"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent"
	"github.com/pbs-plus/pbs-plus/internal/agent/controllers"
	"github.com/pbs-plus/pbs-plus/internal/agent/registry"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/utils"
)

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
	})
	if err != nil {
		return err
	}
	resp, err := agent.AgentHTTPRequest(http.MethodPost, "/api2/json/d2d/target/agent", bytes.NewBuffer(reqBody), nil)
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

func ConnectARPC(ctx context.Context, version string) error {
	serverUrl, err := registry.GetEntry(registry.CONFIG, "ServerURL", false)
	if err != nil {
		return err
	}
	uri, err := utils.ParseURI(serverUrl.Value)
	if err != nil {
		return err
	}
	tlsConfig, err := agent.GetTLSConfig()
	if err != nil {
		return err
	}
	clientId, err := utils.GetAgentHostname()
	if err != nil {
		return err
	}

	address := fmt.Sprintf("%s%s", strings.TrimSuffix(uri.Hostname(), ":"), constants.ARPCServerPort)
	headers := http.Header{}
	headers.Add("X-PBS-Agent", clientId)
	headers.Add("X-PBS-Plus-Version", version)

	go func() {
		base, maxWait, factor, jitter := 500*time.Millisecond, 30*time.Second, 2.0, 0.2
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
				if session == nil {
					var err error
					session, err = arpc.ConnectToServer(ctx, address, headers, tlsConfig)
					if err != nil {
						sleep := min(time.Duration(float64(backoff)*(1+jitter*(2*rand.Float64()-1))), maxWait)
						select {
						case <-ctx.Done():
							return
						case <-time.After(sleep):
							backoff = min(time.Duration(float64(backoff)*factor), maxWait)
							continue
						}
					}
					router := arpc.NewRouter()
					router.Handle("ping", func(req *arpc.Request) (arpc.Response, error) {
						b, _ := cbor.Marshal(map[string]string{"version": version, "hostname": clientId})
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
					backoff = base
				}
				if err := session.Serve(); err != nil {
					if newS, err := session.Reconnect(ctx); err == nil {
						session = newS
					} else {
						session = nil
					}
				}
			}
		}
	}()
	return nil
}
