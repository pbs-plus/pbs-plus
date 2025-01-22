//go:build linux

package main

import (
	"context"
	"flag"
	"log"
	"net/http"

	"github.com/sonroyaalmerol/pbs-plus/internal/backend/backup"
	"github.com/sonroyaalmerol/pbs-plus/internal/proxy"
	"github.com/sonroyaalmerol/pbs-plus/internal/proxy/controllers/agents"
	"github.com/sonroyaalmerol/pbs-plus/internal/proxy/controllers/exclusions"
	"github.com/sonroyaalmerol/pbs-plus/internal/proxy/controllers/jobs"
	"github.com/sonroyaalmerol/pbs-plus/internal/proxy/controllers/partial_files"
	"github.com/sonroyaalmerol/pbs-plus/internal/proxy/controllers/plus"
	"github.com/sonroyaalmerol/pbs-plus/internal/proxy/controllers/targets"
	mw "github.com/sonroyaalmerol/pbs-plus/internal/proxy/middlewares"
	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/websockets"
)

var Version = "v0.0.0"

func main() {
	err := syslog.InitializeLogger()
	if err != nil {
		log.Fatalf("Failed to initialize logger: %s", err)
	}

	jobRun := flag.String("job", "", "Job ID to execute")
	flag.Parse()

	wsHub := websockets.NewServer(context.Background())
	go wsHub.Run()

	storeInstance, err := store.Initialize(wsHub, nil)
	if err != nil {
		syslog.L.Errorf("Failed to initialize store: %v", err)
		return
	}

	token, err := store.GetAPITokenFromFile()
	if err != nil {
		syslog.L.Error(err)
	}
	storeInstance.APIToken = token

	// Handle single job execution
	if *jobRun != "" {
		if storeInstance.APIToken == nil {
			return
		}

		jobTask, err := storeInstance.GetJob(*jobRun)
		if err != nil {
			syslog.L.Error(err)
			return
		}

		if jobTask.LastRunState == nil && jobTask.LastRunUpid != nil {
			syslog.L.Info("A job is still running, skipping this schedule.")
			return
		}

		if _, err = backup.RunBackup(jobTask, storeInstance); err != nil {
			syslog.L.Error(err)
		}
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

	// Initialize router with Go 1.22's new pattern syntax
	mux := http.NewServeMux()

	// API routes
	mux.HandleFunc("/plus/token", mw.Auth(storeInstance, mw.CORS(storeInstance, plus.TokenHandler(storeInstance))))
	mux.HandleFunc("/api2/json/plus/version", mw.Auth(storeInstance, mw.CORS(storeInstance, plus.VersionHandler(storeInstance, Version))))
	mux.HandleFunc("/api2/json/plus/binary", mw.Auth(storeInstance, mw.CORS(storeInstance, plus.DownloadBinary(storeInstance, Version))))
	mux.HandleFunc("/api2/json/plus/binary/checksum", mw.Auth(storeInstance, mw.CORS(storeInstance, plus.DownloadChecksum(storeInstance, Version))))
	mux.HandleFunc("/api2/json/d2d/backup", mw.Auth(storeInstance, mw.CORS(storeInstance, jobs.D2DJobHandler(storeInstance))))
	mux.HandleFunc("/api2/json/d2d/target", mw.Auth(storeInstance, mw.CORS(storeInstance, targets.D2DTargetHandler(storeInstance))))
	mux.HandleFunc("/api2/json/d2d/target/agent", mw.Auth(storeInstance, mw.CORS(storeInstance, targets.D2DTargetAgentHandler(storeInstance))))
	mux.HandleFunc("/api2/json/d2d/exclusion", mw.Auth(storeInstance, mw.CORS(storeInstance, exclusions.D2DExclusionHandler(storeInstance))))
	mux.HandleFunc("/api2/json/d2d/partial-file", mw.Auth(storeInstance, mw.CORS(storeInstance, partial_files.D2DPartialFileHandler(storeInstance))))
	mux.HandleFunc("/api2/json/d2d/agent-log", mw.Auth(storeInstance, mw.CORS(storeInstance, agents.AgentLogHandler(storeInstance))))

	// ExtJS routes with path parameters
	mux.HandleFunc("/api2/extjs/d2d/backup/{job}", mw.Auth(storeInstance, mw.CORS(storeInstance, jobs.ExtJsJobRunHandler(storeInstance))))
	mux.HandleFunc("/api2/extjs/config/d2d-target", mw.Auth(storeInstance, mw.CORS(storeInstance, targets.ExtJsTargetHandler(storeInstance))))
	mux.HandleFunc("/api2/extjs/config/d2d-target/{target}", mw.Auth(storeInstance, mw.CORS(storeInstance, targets.ExtJsTargetSingleHandler(storeInstance))))
	mux.HandleFunc("/api2/extjs/config/d2d-exclusion", mw.Auth(storeInstance, mw.CORS(storeInstance, exclusions.ExtJsExclusionHandler(storeInstance))))
	mux.HandleFunc("/api2/extjs/config/d2d-exclusion/{exclusion}", mw.Auth(storeInstance, mw.CORS(storeInstance, exclusions.ExtJsExclusionSingleHandler(storeInstance))))
	mux.HandleFunc("/api2/extjs/config/d2d-partial-file", mw.Auth(storeInstance, mw.CORS(storeInstance, partial_files.ExtJsPartialFileHandler(storeInstance))))
	mux.HandleFunc("/api2/extjs/config/d2d-partial-file/{partial_file}", mw.Auth(storeInstance, mw.CORS(storeInstance, partial_files.ExtJsPartialFileSingleHandler(storeInstance))))
	mux.HandleFunc("/api2/extjs/config/disk-backup-job", mw.Auth(storeInstance, mw.CORS(storeInstance, jobs.ExtJsJobHandler(storeInstance))))
	mux.HandleFunc("/api2/extjs/config/disk-backup-job/{job}", mw.Auth(storeInstance, mw.CORS(storeInstance, jobs.ExtJsJobSingleHandler(storeInstance))))

	// WebSocket-related routes
	mux.HandleFunc("/plus/ws", mw.Auth(storeInstance, plus.WSHandler(storeInstance)))
	mux.HandleFunc("/plus/mount/{target}/{drive}", mw.Auth(storeInstance, plus.MountHandler(storeInstance)))

	server := &http.Server{
		Addr:    ":8008",
		Handler: mux,
	}

	syslog.L.Info("Starting proxy server on :8008")
	if err := server.ListenAndServeTLS(store.CertFile, store.KeyFile); err != nil {
		if syslog.L != nil {
			syslog.L.Errorf("Server failed: %v", err)
		}
	}
}
