//go:build linux

package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"

	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/backend/backup"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/logger"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/proxy"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/proxy/controllers/agents"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/proxy/controllers/jobs"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/proxy/controllers/targets"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/store"
)

// CustomRouter to handle specific paths and fallback to proxy
type CustomRouter struct {
	store       *store.Store
	proxy       *http.Handler
	targetPaths map[string]http.HandlerFunc
}

func NewCustomRouter(storeInstance *store.Store, proxy http.Handler) *CustomRouter {
	router := &CustomRouter{
		store: storeInstance,
		proxy: &proxy,
		targetPaths: map[string]http.HandlerFunc{
			"/api2/json/d2d/backup":               jobs.D2DJobHandler(storeInstance),
			"/api2/json/d2d/target":               targets.D2DTargetHandler(storeInstance),
			"/api2/json/d2d/agent-log":            agents.AgentLogHandler(storeInstance),
			"/api2/extjs/d2d/backup/":             jobs.ExtJsJobRunHandler(storeInstance),
			"/api2/extjs/config/d2d-target":       targets.ExtJsTargetHandler(storeInstance),
			"/api2/extjs/config/d2d-target/":      targets.ExtJsTargetSingleHandler(storeInstance),
			"/api2/extjs/config/disk-backup-job":  jobs.ExtJsJobHandler(storeInstance),
			"/api2/extjs/config/disk-backup-job/": jobs.ExtJsJobSingleHandler(storeInstance),
		},
	}
	return router
}

func (router *CustomRouter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Route request based on custom paths
	for path, handler := range router.targetPaths {
		if strings.HasPrefix(r.URL.Path, path) {
			handler(w, r)
			return
		}
	}

	// If no match, fall back to proxy
	(*router.proxy).ServeHTTP(w, r)
}

func main() {
	s, _ := logger.InitializeSyslogger()
	jobRun := flag.String("job", "", "Job ID to execute")
	flag.Parse()

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

	router := NewCustomRouter(storeInstance, proxy.CreateProxy(targetURL, storeInstance))

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

	log.Println("Starting proxy server on :8008")
	if err := http.ListenAndServeTLS(":8008", store.CertFile, store.KeyFile, router); err != nil {
		if s != nil {
			s.Err(fmt.Sprintf("Server failed: %v", err))
		}
		log.Fatalf("Server failed: %v", err)
	}
}
