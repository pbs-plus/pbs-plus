//go:build linux

package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"regexp"

	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/backend/backup"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/logger"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/proxy"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/proxy/controllers/agents"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/proxy/controllers/jobs"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/proxy/controllers/targets"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/store"
)

// routeHandler holds a pattern and handler that accepts path variables
type routeHandler struct {
	pattern *regexp.Regexp
	handler func(http.ResponseWriter, *http.Request, map[string]string)
}

// CustomRouter handles dynamic routes with path variables and a default handler
type CustomRouter struct {
	routes         []routeHandler
	defaultHandler http.Handler
}

// AddRoute registers a route with pattern and handler supporting path variables
func (cr *CustomRouter) AddRoute(pattern string, handler func(http.ResponseWriter, *http.Request, map[string]string)) {
	// Convert "{variable}" segments to named capture groups in regex
	regexPattern := regexp.MustCompile(`\{([a-zA-Z0-9_]+)\}`).ReplaceAllString(pattern, `(?P<$1>[^/]+)`)
	cr.routes = append(cr.routes, routeHandler{
		pattern: regexp.MustCompile("^" + regexPattern + "$"),
		handler: handler,
	})
}

// ServeHTTP checks routes for a match, extracts path variables, and calls handlers
func (cr *CustomRouter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	for _, route := range cr.routes {
		if match := route.pattern.FindStringSubmatch(r.URL.Path); match != nil {
			// Extract variables from matched path
			vars := make(map[string]string)
			for i, name := range route.pattern.SubexpNames() {
				if i > 0 && name != "" {
					vars[name] = match[i]
				}
			}
			route.handler(w, r, vars)
			return
		}
	}
	// Use the default handler if no routes match
	cr.defaultHandler.ServeHTTP(w, r)
}

func main() {
	s, _ := logger.InitializeSyslogger()
	jobRun := flag.String("job", "", "Job ID to execute")
	flag.Parse()

	storeInstance, err := store.Initialize()
	if err != nil {
		if s != nil {
			s.Err(fmt.Sprintf("Failed to initialize store: %v", err))
		}
		log.Fatalf("Failed to initialize store: %v", err)
	}

	err = storeInstance.CreateTables()
	if err != nil {
		if s != nil {
			s.Err(fmt.Sprintf("Failed to create store tables: %v", err))
		}
		log.Fatalf("Failed to create store tables: %v", err)
	}

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

	targetURL, err := url.Parse(store.ProxyTargetURL)
	if err != nil {
		if s != nil {
			s.Err(fmt.Sprintf("Failed to parse target URL: %v", err))
		}
		log.Fatalf("Failed to parse target URL: %v", err)
	}

	proxy := proxy.CreateProxy(targetURL, storeInstance)

	token, err := store.GetAPITokenFromFile()
	if err != nil {
		if s != nil {
			s.Err(err.Error())
		}
		log.Println(err)
	}

	storeInstance.APIToken = token

	// Set up router with routes and a reverse proxy as the default handler
	router := &CustomRouter{
		defaultHandler: proxy, // Set proxy as fallback for unmatched paths
	}

	// Register routes
	router.AddRoute("/api2/json/d2d/backup", jobs.D2DJobHandler(storeInstance))
	router.AddRoute("/api2/json/d2d/target", targets.D2DTargetHandler(storeInstance))
	router.AddRoute("/api2/json/d2d/agent-log", agents.AgentLogHandler(storeInstance))
	router.AddRoute("/api2/extjs/d2d/backup/{job}", jobs.ExtJsJobRunHandler(storeInstance))
	router.AddRoute("/api2/extjs/config/d2d-target", targets.ExtJsTargetHandler(storeInstance))
	router.AddRoute("/api2/extjs/config/d2d-target/{target}", targets.ExtJsTargetSingleHandler(storeInstance))
	router.AddRoute("/api2/extjs/config/disk-backup-job", jobs.ExtJsJobHandler(storeInstance))
	router.AddRoute("/api2/extjs/config/disk-backup-job/{job}", jobs.ExtJsJobSingleHandler(storeInstance))

	log.Println("Starting proxy server on :8008")
	if err := http.ListenAndServeTLS(":8008", store.CertFile, store.KeyFile, router); err != nil {
		if s != nil {
			s.Err(fmt.Sprintf("Server failed: %v", err))
		}
		log.Fatalf("Server failed: %v", err)
	}
}
