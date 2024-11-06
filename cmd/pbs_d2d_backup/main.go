//go:build linux

package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"

	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/backend/backup"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/logger"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/proxy"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/proxy/controllers/agents"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/proxy/controllers/jobs"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/proxy/controllers/targets"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/store"
)

func main() {
	s, _ := logger.InitializeSyslogger()
	jobRun := flag.String("job", "", "Job ID to execute")
	flag.Parse()

	mux := http.NewServeMux()

	targetURL, err := url.Parse(store.ProxyTargetURL)
	if err != nil {
		if s != nil {
			s.Err(fmt.Sprintf("Failed to parse target URL: %v", err))
		}
		log.Fatalf("Failed to parse target URL: %v", err)
	}

	storeInstance, err := store.Initialize()
	if err != nil {
		if s != nil {
			s.Err(fmt.Sprintf("Failed to initialize store: %v", err))
		}
		log.Fatalf("Failed to initialize store: %v", err)
	}

	proxy := proxy.CreateProxy(targetURL, storeInstance)

	err = storeInstance.CreateTables()
	if err != nil {
		if s != nil {
			s.Err(fmt.Sprintf("Failed to create store tables: %v", err))
		}
		log.Fatalf("Failed to create store tables: %v", err)
	}

	token, err := store.GetAPITokenFromFile()
	if err != nil {
		if s != nil {
			s.Err(err.Error())
		}
		log.Println(err)
	}

	storeInstance.APIToken = token

	if *jobRun != "" {
		if storeInstance.APIToken == nil {
			return
		}

		jobTask, err := storeInstance.GetJob(*jobRun)
		if err != nil {
			if s != nil {
				s.Err(err.Error())
			}
			log.Println(err)
			return
		}

		if jobTask.LastRunState == nil {
			log.Println("A job is still running, skipping this schedule.")
			return
		}

		_, err = backup.RunBackup(jobTask, storeInstance)
		if err != nil {
			if s != nil {
				s.Err(err.Error())
			}
			log.Println(err)
		}

		return
	}

	mux.HandleFunc("/api2/json/d2d/backup", jobs.D2DJobHandler(storeInstance))
	mux.HandleFunc("/api2/json/d2d/target", targets.D2DTargetHandler(storeInstance))
	mux.HandleFunc("/api2/json/d2d/agent-log", agents.AgentLogHandler(storeInstance))

	mux.HandleFunc("/api2/extjs/d2d/backup/{job}", jobs.ExtJsJobRunHandler(storeInstance))

	mux.HandleFunc("/api2/extjs/config/d2d-target", targets.ExtJsTargetHandler(storeInstance))
	mux.HandleFunc("/api2/extjs/config/d2d-target/{target}", targets.ExtJsTargetSingleHandler(storeInstance))

	mux.HandleFunc("/api2/extjs/config/disk-backup-job", jobs.ExtJsJobHandler(storeInstance))
	mux.HandleFunc("/api2/extjs/config/disk-backup-job/{job}", jobs.ExtJsJobSingleHandler(storeInstance))

	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxy.ServeHTTP(w, r)
	}))

	log.Println("Starting proxy server on :8008")
	if err := http.ListenAndServeTLS(":8008", store.CertFile, store.KeyFile, mux); err != nil {
		if s != nil {
			s.Err(fmt.Sprintf("Server failed: %v", err))
		}
		log.Fatalf("Server failed: %v", err)
	}
}
