//go:build linux

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/auth/certificates"
	"github.com/sonroyaalmerol/pbs-plus/internal/auth/server"
	"github.com/sonroyaalmerol/pbs-plus/internal/auth/token"
	"github.com/sonroyaalmerol/pbs-plus/internal/backend/backup"
	"github.com/sonroyaalmerol/pbs-plus/internal/proxy"
	"github.com/sonroyaalmerol/pbs-plus/internal/proxy/controllers/agents"
	"github.com/sonroyaalmerol/pbs-plus/internal/proxy/controllers/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/proxy/controllers/exclusions"
	"github.com/sonroyaalmerol/pbs-plus/internal/proxy/controllers/jobs"
	"github.com/sonroyaalmerol/pbs-plus/internal/proxy/controllers/plus"
	"github.com/sonroyaalmerol/pbs-plus/internal/proxy/controllers/targets"
	"github.com/sonroyaalmerol/pbs-plus/internal/proxy/controllers/tokens"
	mw "github.com/sonroyaalmerol/pbs-plus/internal/proxy/middlewares"
	rpcmount "github.com/sonroyaalmerol/pbs-plus/internal/proxy/rpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/constants"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/proxmox"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
)

var Version = "v0.0.0"

func main() {
	proxmox.InitializeProxmox()

	jobRun := flag.String("job", "", "Job ID to execute")
	flag.Parse()

	argsWithoutProg := os.Args[1:]

	if len(argsWithoutProg) > 0 && argsWithoutProg[0] == "clean-task-logs" {
		fmt.Println("WARNING: You are about to remove all junk logs recursively from:")
		fmt.Println("         /var/log/proxmox-backup/tasks")
		fmt.Println()
		fmt.Println("All log entries with the following substrings will be removed if found in any log file:")
		for _, substr := range backup.JunkSubstrings {
			fmt.Printf(" - %s\n", substr)
		}
		fmt.Println()
		fmt.Println("If this is not what you intend, press Ctrl+C within the next 10 seconds to cancel.")
		fmt.Println()

		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt)

		cancelChan := make(chan struct{})
		go func() {
			<-sigChan
			fmt.Println("\nOperation cancelled by user.")
			close(cancelChan)
		}()

		for i := 10; i > 0; i-- {
			select {
			case <-cancelChan:
				// User cancelled the operation.
				return
			default:
				fmt.Printf("Proceeding in %d seconds...\n", i)
				time.Sleep(1 * time.Second)
			}
		}

		fmt.Println("Proceeding with log cleanup...")

		removed, err := backup.RemoveJunkLogsRecursively("/var/log/proxmox-backup/tasks")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Successfully removed %d of junk lines from all task logs files.\n", removed)
		return
	}

	storeInstance, err := store.Initialize(nil)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to initialize store").Write()
		return
	}

	apiToken, err := proxmox.GetAPITokenFromFile()
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to get token from file").Write()
	}
	proxmox.Session.APIToken = apiToken

	// Handle single job execution
	if *jobRun != "" {
		if proxmox.Session.APIToken == nil {
			return
		}

		jobTask, err := storeInstance.Database.GetJob(*jobRun)
		if err != nil {
			syslog.L.Error(err).Write()
			return
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		op := backup.RunBackup(ctx, jobTask, storeInstance, true)
		if waitErr := op.Wait(); waitErr != nil {
			syslog.L.Error(waitErr).Write()
		}

		return
	}

	pbsJsLocation := "/usr/share/javascript/proxmox-backup/js/proxmox-backup-gui.js"
	proxmoxLibLocation := "/usr/share/javascript/proxmox-widget-toolkit/proxmoxlib.js"

	if err := proxy.WatchAndReplace(pbsJsLocation, proxy.ModifyJS); err != nil {
		syslog.L.Error(err).WithMessage("failed to mount modified proxmox-backup-gui.js").Write()
		return
	}

	if err := proxy.WatchAndReplace(proxmoxLibLocation, proxy.ModifyLib); err != nil {
		syslog.L.Error(err).WithMessage("failed to mount modified proxmoxlib.js").Write()
		return
	}

	certOpts := certificates.DefaultOptions()
	generator, err := certificates.NewGenerator(certOpts)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to initialize certificate generator").Write()
		return
	}

	csrfKey, err := os.ReadFile("/etc/proxmox-backup/csrf.key")
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to read csrf.key").Write()
		return
	}

	serverConfig := server.DefaultConfig()
	serverConfig.CertFile = filepath.Join(certOpts.OutputDir, "server.crt")
	serverConfig.KeyFile = filepath.Join(certOpts.OutputDir, "server.key")
	serverConfig.CAFile = filepath.Join(certOpts.OutputDir, "ca.crt")
	serverConfig.CAKey = filepath.Join(certOpts.OutputDir, "ca.key")
	serverConfig.TokenSecret = string(csrfKey)

	if err := generator.ValidateExistingCerts(); err != nil {
		if err := generator.GenerateCA(); err != nil {
			syslog.L.Error(err).WithMessage("failed to generate certificate").Write()
			return
		}

		if err := generator.GenerateCert("server"); err != nil {
			syslog.L.Error(err).WithMessage("failed to generate certificate").Write()
			return
		}
	}

	if err := serverConfig.Validate(); err != nil {
		syslog.L.Error(err).WithMessage("failed to validate server config").Write()
		return
	}

	storeInstance.CertGenerator = generator

	err = os.Chown(serverConfig.KeyFile, 0, 34)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to change cert key permissions").Write()
		return
	}

	err = os.Chown(serverConfig.CertFile, 0, 34)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to change cert permissions").Write()
		return
	}

	err = serverConfig.Mount()
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to mount new certificate for mTLS").Write()
		return
	}
	defer func() {
		_ = serverConfig.Unmount()
	}()

	proxy := exec.Command("/usr/bin/systemctl", "restart", "proxmox-backup-proxy")
	proxy.Env = os.Environ()
	_ = proxy.Run()

	// Initialize token manager
	tokenManager, err := token.NewManager(token.Config{
		TokenExpiration: serverConfig.TokenExpiration,
		SecretKey:       serverConfig.TokenSecret,
	})
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to initialize token manager").Write()
		return
	}
	storeInstance.Database.TokenManager = tokenManager

	// Setup HTTP server
	tlsConfig, err := serverConfig.LoadTLSConfig()
	if err != nil {
		return
	}

	caRenewalCtx, cancelRenewal := context.WithCancel(context.Background())
	defer cancelRenewal()
	go func() {
		for {
			select {
			case <-caRenewalCtx.Done():
				return
			case <-time.After(time.Hour):
				if err := generator.ValidateExistingCerts(); err != nil {
					if err := generator.GenerateCA(); err != nil {
						syslog.L.Error(err).WithMessage("failed to generate CA").Write()
					}

					if err := generator.GenerateCert("server"); err != nil {
						syslog.L.Error(err).WithMessage("failed to generate server certificate").Write()
					}
				}

			}
		}
	}()

	// Unmount and remove all stale mount points
	// Get all mount points under the base path
	mountPoints, err := filepath.Glob(filepath.Join(constants.AgentMountBasePath, "*"))
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to find agent mount base path").Write()
	}

	// Unmount each one
	for _, mountPoint := range mountPoints {
		umount := exec.Command("umount", "-lf", mountPoint)
		umount.Env = os.Environ()
		if err := umount.Run(); err != nil {
			// Optionally handle individual unmount errors
			syslog.L.Error(err).WithMessage("failed to unmount some mounted agents").Write()
		}
	}

	if err := os.RemoveAll(constants.AgentMountBasePath); err != nil {
		syslog.L.Error(err).WithMessage("failed to remove directory").Write()
	}

	if err := os.Mkdir(constants.AgentMountBasePath, 0700); err != nil {
		syslog.L.Error(err).WithMessage("failed to recreate directory").Write()
	}

	rpcCtx, rpcCancel := context.WithCancel(context.Background())
	defer rpcCancel()

	go func() {
		for {
			select {
			case <-rpcCtx.Done():
				syslog.L.Error(rpcCtx.Err()).WithMessage("rpc server cancelled")
				return
			default:
				if err := rpcmount.StartRPCServer(constants.MountSocketPath, storeInstance); err != nil {
					syslog.L.Error(err).WithMessage("rpc server failed, restarting")
				}
			}
		}
	}()

	mux := http.NewServeMux()

	// API routes
	mux.HandleFunc("/plus/token", mw.ServerOnly(storeInstance, mw.CORS(storeInstance, plus.TokenHandler(storeInstance))))
	mux.HandleFunc("/api2/json/plus/version", mw.AgentOrServer(storeInstance, mw.CORS(storeInstance, plus.VersionHandler(storeInstance, Version))))
	mux.HandleFunc("/api2/json/plus/binary", mw.CORS(storeInstance, plus.DownloadBinary(storeInstance, Version)))
	mux.HandleFunc("/api2/json/plus/updater-binary", mw.CORS(storeInstance, plus.DownloadUpdater(storeInstance, Version)))
	mux.HandleFunc("/api2/json/plus/binary/checksum", mw.AgentOrServer(storeInstance, mw.CORS(storeInstance, plus.DownloadChecksum(storeInstance, Version))))
	mux.HandleFunc("/api2/json/d2d/backup", mw.ServerOnly(storeInstance, mw.CORS(storeInstance, jobs.D2DJobHandler(storeInstance))))
	mux.HandleFunc("/api2/json/d2d/target", mw.ServerOnly(storeInstance, mw.CORS(storeInstance, targets.D2DTargetHandler(storeInstance))))
	mux.HandleFunc("/api2/json/d2d/target/agent", mw.AgentOnly(storeInstance, mw.CORS(storeInstance, targets.D2DTargetAgentHandler(storeInstance))))
	mux.HandleFunc("/api2/json/d2d/token", mw.ServerOnly(storeInstance, mw.CORS(storeInstance, tokens.D2DTokenHandler(storeInstance))))
	mux.HandleFunc("/api2/json/d2d/exclusion", mw.AgentOrServer(storeInstance, mw.CORS(storeInstance, exclusions.D2DExclusionHandler(storeInstance))))
	mux.HandleFunc("/api2/json/d2d/agent-log", mw.AgentOnly(storeInstance, mw.CORS(storeInstance, agents.AgentLogHandler(storeInstance))))

	// ExtJS routes with path parameters
	mux.HandleFunc("/api2/extjs/d2d/backup/{job}", mw.ServerOnly(storeInstance, mw.CORS(storeInstance, jobs.ExtJsJobRunHandler(storeInstance))))
	mux.HandleFunc("/api2/extjs/config/d2d-target", mw.ServerOnly(storeInstance, mw.CORS(storeInstance, targets.ExtJsTargetHandler(storeInstance))))
	mux.HandleFunc("/api2/extjs/config/d2d-target/{target}", mw.ServerOnly(storeInstance, mw.CORS(storeInstance, targets.ExtJsTargetSingleHandler(storeInstance))))
	mux.HandleFunc("/api2/extjs/config/d2d-token", mw.ServerOnly(storeInstance, mw.CORS(storeInstance, tokens.ExtJsTokenHandler(storeInstance))))
	mux.HandleFunc("/api2/extjs/config/d2d-token/{token}", mw.ServerOnly(storeInstance, mw.CORS(storeInstance, tokens.ExtJsTokenSingleHandler(storeInstance))))
	mux.HandleFunc("/api2/extjs/config/d2d-exclusion", mw.ServerOnly(storeInstance, mw.CORS(storeInstance, exclusions.ExtJsExclusionHandler(storeInstance))))
	mux.HandleFunc("/api2/extjs/config/d2d-exclusion/{exclusion}", mw.ServerOnly(storeInstance, mw.CORS(storeInstance, exclusions.ExtJsExclusionSingleHandler(storeInstance))))
	mux.HandleFunc("/api2/extjs/config/disk-backup-job", mw.ServerOnly(storeInstance, mw.CORS(storeInstance, jobs.ExtJsJobHandler(storeInstance))))
	mux.HandleFunc("/api2/extjs/config/disk-backup-job/{job}", mw.ServerOnly(storeInstance, mw.CORS(storeInstance, jobs.ExtJsJobSingleHandler(storeInstance))))

	// aRPC route
	mux.HandleFunc("/plus/arpc", mw.AgentOnly(storeInstance, arpc.ARPCHandler(storeInstance)))

	// Agent auth routes
	mux.HandleFunc("/plus/agent/bootstrap", mw.CORS(storeInstance, agents.AgentBootstrapHandler(storeInstance)))
	mux.HandleFunc("/plus/agent/renew", mw.AgentOnly(storeInstance, mw.CORS(storeInstance, agents.AgentRenewHandler(storeInstance))))
	mux.HandleFunc("/plus/agent/install/win", mw.CORS(storeInstance, plus.AgentInstallScriptHandler(storeInstance, Version)))

	server := &http.Server{
		Addr:           serverConfig.Address,
		Handler:        mux,
		TLSConfig:      tlsConfig,
		ReadTimeout:    serverConfig.ReadTimeout,
		WriteTimeout:   serverConfig.WriteTimeout,
		IdleTimeout:    serverConfig.IdleTimeout,
		MaxHeaderBytes: serverConfig.MaxHeaderBytes,
	}

	syslog.L.Info().WithMessage("starting proxy server on :8008").Write()
	if err := server.ListenAndServeTLS(serverConfig.CertFile, serverConfig.KeyFile); err != nil {
		syslog.L.Error(err).WithMessage("http server failed")
	}
}
