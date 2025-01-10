//go:build linux

package main

import (
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
	"github.com/sonroyaalmerol/pbs-plus/internal/proxy/middlewares"
	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/websockets"
)

var Version = "v0.0.0"

func main() {
	s, err := syslog.InitializeLogger()
	if err != nil {
		log.Fatalf("Failed to initialize logger: %s", err)
	}

	jobRun := flag.String("job", "", "Job ID to execute")
	flag.Parse()

	wsHub := websockets.NewServer()
	storeInstance, err := store.Initialize(wsHub)
	if err != nil {
		s.Errorf("Failed to initialize store: %v", err)
		return
	}

	token, err := store.GetAPITokenFromFile()
	if err != nil {
		s.Error(err)
	}
	storeInstance.APIToken = token

	if err = storeInstance.CreateTables(); err != nil {
		s.Errorf("Failed to create store tables: %v", err)
		return
	}

	// Handle single job execution
	if *jobRun != "" {
		if storeInstance.APIToken == nil {
			return
		}

		jobTask, err := storeInstance.GetJob(*jobRun)
		if err != nil {
			s.Error(err)
			return
		}

		if jobTask.LastRunState == nil && jobTask.LastRunUpid != nil {
			s.Info("A job is still running, skipping this schedule.")
			return
		}

		if _, err = backup.RunBackup(jobTask, storeInstance); err != nil {
			s.Error(err)
		}
		return
	}

	pbsJsLocation := "/usr/share/javascript/proxmox-backup/js/proxmox-backup-gui.js"
	err = proxy.MountCompiledJS(pbsJsLocation)
	if err != nil {
		s.Errorf("Modified JS mounting failed: %v", err)
		return
	}

	defer func() {
		_ = proxy.UnmountCompiledJS(pbsJsLocation)
	}()

	// Initialize router with Go 1.22's new pattern syntax
	mux := http.NewServeMux()

	// API routes
	mux.HandleFunc("/plus/token", middlewares.CORS(storeInstance, plus.TokenHandler(storeInstance)))
	mux.HandleFunc("/api2/json/plus/version", middlewares.CORS(storeInstance, plus.VersionHandler(storeInstance, Version)))
	mux.HandleFunc("/api2/json/plus/binary", middlewares.CORS(storeInstance, plus.DownloadBinary(storeInstance, Version)))
	mux.HandleFunc("/api2/json/d2d/backup", middlewares.CORS(storeInstance, jobs.D2DJobHandler(storeInstance)))
	mux.HandleFunc("/api2/json/d2d/target", middlewares.CORS(storeInstance, targets.D2DTargetHandler(storeInstance)))
	mux.HandleFunc("/api2/json/d2d/target/agent", middlewares.CORS(storeInstance, targets.D2DTargetAgentHandler(storeInstance)))
	mux.HandleFunc("/api2/json/d2d/exclusion", middlewares.CORS(storeInstance, exclusions.D2DExclusionHandler(storeInstance)))
	mux.HandleFunc("/api2/json/d2d/partial-file", middlewares.CORS(storeInstance, partial_files.D2DPartialFileHandler(storeInstance)))
	mux.HandleFunc("/api2/json/d2d/agent-log", middlewares.CORS(storeInstance, agents.AgentLogHandler(storeInstance)))

	// ExtJS routes with path parameters
	mux.HandleFunc("/api2/extjs/d2d/backup/{job}", middlewares.CORS(storeInstance, jobs.ExtJsJobRunHandler(storeInstance)))
	mux.HandleFunc("/api2/extjs/config/d2d-target", middlewares.CORS(storeInstance, targets.ExtJsTargetHandler(storeInstance)))
	mux.HandleFunc("/api2/extjs/config/d2d-target/{target}", middlewares.CORS(storeInstance, targets.ExtJsTargetSingleHandler(storeInstance)))
	mux.HandleFunc("/api2/extjs/config/d2d-exclusion", middlewares.CORS(storeInstance, exclusions.ExtJsExclusionHandler(storeInstance)))
	mux.HandleFunc("/api2/extjs/config/d2d-exclusion/{exclusion}", middlewares.CORS(storeInstance, exclusions.ExtJsExclusionSingleHandler(storeInstance)))
	mux.HandleFunc("/api2/extjs/config/d2d-partial-file", middlewares.CORS(storeInstance, partial_files.ExtJsPartialFileHandler(storeInstance)))
	mux.HandleFunc("/api2/extjs/config/d2d-partial-file/{partial_file}", middlewares.CORS(storeInstance, partial_files.ExtJsPartialFileSingleHandler(storeInstance)))
	mux.HandleFunc("/api2/extjs/config/disk-backup-job", middlewares.CORS(storeInstance, jobs.ExtJsJobHandler(storeInstance)))
	mux.HandleFunc("/api2/extjs/config/disk-backup-job/{job}", middlewares.CORS(storeInstance, jobs.ExtJsJobSingleHandler(storeInstance)))

	// WebSocket-related routes
	mux.HandleFunc("/plus/ws", plus.WSHandler(storeInstance, wsHub))
	mux.HandleFunc("/plus/mount/{target}/{drive}", middlewares.CORS(storeInstance, plus.MountHandler(storeInstance, wsHub)))

	s.Info("Starting proxy server on :8008")
	if err := http.ListenAndServeTLS(":8008", store.CertFile, store.KeyFile, mux); err != nil {
		if s != nil {
			s.Errorf("Server failed: %v", err)
		}
	}
}
