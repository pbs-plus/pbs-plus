//go:build linux

package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/sonroyaalmerol/pbs-plus/internal/auth/certificates"
	"github.com/sonroyaalmerol/pbs-plus/internal/auth/server"
	"github.com/sonroyaalmerol/pbs-plus/internal/auth/token"
	"github.com/sonroyaalmerol/pbs-plus/internal/backend/backup"
	"github.com/sonroyaalmerol/pbs-plus/internal/proxy"
	"github.com/sonroyaalmerol/pbs-plus/internal/proxy/controllers/agents"
	"github.com/sonroyaalmerol/pbs-plus/internal/proxy/controllers/exclusions"
	"github.com/sonroyaalmerol/pbs-plus/internal/proxy/controllers/jobs"
	"github.com/sonroyaalmerol/pbs-plus/internal/proxy/controllers/plus"
	"github.com/sonroyaalmerol/pbs-plus/internal/proxy/controllers/targets"
	"github.com/sonroyaalmerol/pbs-plus/internal/proxy/controllers/tokens"
	mw "github.com/sonroyaalmerol/pbs-plus/internal/proxy/middlewares"
	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/proxmox"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/websockets"
)

var Version = "v0.0.0"

func main() {
	err := syslog.InitializeLogger()
	if err != nil {
		log.Fatalf("Failed to initialize logger: %s", err)
	}
	proxmox.InitializeProxmox()

	jobRun := flag.String("job", "", "Job ID to execute")
	flag.Parse()

	var wsHub *websockets.Server
	wsHub = nil

	if *jobRun == "" {
		wsHub = websockets.NewServer(context.Background())
		go wsHub.Run()
	}

	storeInstance, err := store.Initialize(wsHub, nil)
	if err != nil {
		syslog.L.Errorf("Failed to initialize store: %v", err)
		return
	}

	apiToken, err := proxmox.GetAPITokenFromFile()
	if err != nil {
		syslog.L.Error(err)
	}
	proxmox.Session.APIToken = apiToken

	// Handle single job execution
	if *jobRun != "" {
		if proxmox.Session.APIToken == nil {
			return
		}

		jobTask, err := storeInstance.Database.GetJob(*jobRun)
		if err != nil {
			syslog.L.Error(err)
			return
		}

		if jobTask.LastRunState == nil && jobTask.LastRunUpid != "" {
			syslog.L.Info("A job is still running, skipping this schedule.")
			return
		}

		op, err := backup.RunBackup(jobTask, storeInstance, true)
		if err != nil {
			syslog.L.Error(err)
			return
		}

		op.Wait()
		return
	}

	pbsJsLocation := "/usr/share/javascript/proxmox-backup/js/proxmox-backup-gui.js"
	err = proxy.MountCompiledJS(pbsJsLocation)
	if err != nil {
		syslog.L.Errorf("Modified JS mounting failed: %v", err)
		return
	}

	proxmoxLibLocation := "/usr/share/javascript/proxmox-widget-toolkit/proxmoxlib.js"
	err = proxy.MountModdedProxmoxLib(proxmoxLibLocation)
	if err != nil {
		syslog.L.Errorf("Modified JS mounting failed: %v", err)
		return
	}

	defer func() {
		_ = proxy.UnmountModdedFile(pbsJsLocation)
		_ = proxy.UnmountModdedFile(proxmoxLibLocation)
	}()

	certOpts := certificates.DefaultOptions()
	generator, err := certificates.NewGenerator(certOpts)
	if err != nil {
		syslog.L.Errorf("Initializing certificate generator failed: %v", err)
		return
	}

	csrfKey, err := os.ReadFile("/etc/proxmox-backup/csrf.key")
	if err != nil {
		syslog.L.Errorf("CSRF key not found: %v", err)
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
			syslog.L.Errorf("Generating certificates failed: %v", err)
			return
		}

		if err := generator.GenerateCert("server"); err != nil {
			syslog.L.Errorf("Generating certificates failed: %v", err)
			return
		}
	}

	if err := serverConfig.Validate(); err != nil {
		syslog.L.Errorf("Validating server config failed: %v", err)
		return
	}

	storeInstance.CertGenerator = generator

	err = os.Chown(serverConfig.KeyFile, 0, 34)
	if err != nil {
		syslog.L.Errorf("Changing permissions of key failed: %v", err)
		return
	}

	err = os.Chown(serverConfig.CertFile, 0, 34)
	if err != nil {
		syslog.L.Errorf("Changing permissions of cert failed: %v", err)
		return
	}

	err = serverConfig.Mount()
	if err != nil {
		syslog.L.Errorf("Mounting certificates failed: %v", err)
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
		syslog.L.Errorf("Initializing token manager failed: %v", err)
		return
	}
	storeInstance.Database.TokenManager = tokenManager

	// Setup HTTP server
	tlsConfig, err := serverConfig.LoadTLSConfig()
	if err != nil {
		return
	}

	// Initialize router with Go 1.22's new pattern syntax
	mux := http.NewServeMux()

	// API routes
	mux.HandleFunc("/plus/token", mw.ServerOnly(storeInstance, mw.CORS(storeInstance, plus.TokenHandler(storeInstance))))
	mux.HandleFunc("/api2/json/plus/version", mw.AgentOrServer(storeInstance, mw.CORS(storeInstance, plus.VersionHandler(storeInstance, Version))))
	mux.HandleFunc("/api2/json/plus/binary", mw.AgentOrServer(storeInstance, mw.CORS(storeInstance, plus.DownloadBinary(storeInstance, Version))))
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

	// WebSocket-related routes
	mux.HandleFunc("/plus/ws", mw.AgentOnly(storeInstance, plus.WSHandler(storeInstance)))
	mux.HandleFunc("/plus/mount/{target}/{drive}", mw.ServerOnly(storeInstance, plus.MountHandler(storeInstance)))

	// Agent auth routes
	mux.HandleFunc("/plus/agent/bootstrap", mw.CORS(storeInstance, agents.AgentBootstrapHandler(storeInstance)))
	mux.HandleFunc("/plus/agent/renew", mw.AgentOnly(storeInstance, mw.CORS(storeInstance, agents.AgentRenewHandler(storeInstance))))

	server := &http.Server{
		Addr:           serverConfig.Address,
		Handler:        mux,
		TLSConfig:      tlsConfig,
		ReadTimeout:    serverConfig.ReadTimeout,
		WriteTimeout:   serverConfig.WriteTimeout,
		MaxHeaderBytes: serverConfig.MaxHeaderBytes,
	}

	syslog.L.Info("Starting proxy server on :8008")
	if err := server.ListenAndServeTLS(serverConfig.CertFile, serverConfig.KeyFile); err != nil {
		if syslog.L != nil {
			syslog.L.Errorf("Server failed: %v", err)
		}
	}
}
