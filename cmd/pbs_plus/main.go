//go:build linux

package main

import (
	"flag"
	"log"
	"net/http"
	"net/url"
	"regexp"

	"github.com/sonroyaalmerol/pbs-plus/internal/backend/backup"
	"github.com/sonroyaalmerol/pbs-plus/internal/proxy"
	"github.com/sonroyaalmerol/pbs-plus/internal/proxy/controllers/agents"
	"github.com/sonroyaalmerol/pbs-plus/internal/proxy/controllers/jobs"
	"github.com/sonroyaalmerol/pbs-plus/internal/proxy/controllers/targets"
	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
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
	s, err := syslog.InitializeLogger()
	if err != nil {
		log.Fatalf("Failed to initialize logger: %s", err)
	}

	jobRun := flag.String("job", "", "Job ID to execute")
	flag.Parse()

	storeInstance, err := store.Initialize()
	if err != nil {
		s.Errorf("Failed to initialize store: %v", err)
		return
	}

	targetURL, err := url.Parse(store.ProxyTargetURL)
	if err != nil {
		s.Errorf("Failed to parse target URL: %v", err)
	}

	proxy := proxy.CreateProxy(targetURL, storeInstance)

	token, err := store.GetAPITokenFromFile()
	if err != nil {
		s.Error(err)
	}

	storeInstance.APIToken = token

	err = storeInstance.CreateTables()
	if err != nil {
		s.Errorf("Failed to create store tables: %v", err)
		return
	}

	if *jobRun != "" {
		if storeInstance.APIToken == nil {
			return
		}

		jobTask, err := storeInstance.GetJob(*jobRun)
		if err != nil {
			s.Error(err)
			return
		}

		if jobTask.LastRunState == nil {
			s.Info("A job is still running, skipping this schedule.")
			return
		}

		waitChan := make(chan struct{})
		_, err = backup.RunBackup(jobTask, storeInstance, waitChan)
		if err != nil {
			close(waitChan)
			s.Error(err)
		} else {
			<-waitChan
		}

		return
	}

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

	s.Info("Starting proxy server on :8008")
	if err := http.ListenAndServeTLS(":8008", store.CertFile, store.KeyFile, router); err != nil {
		if s != nil {
			s.Errorf("Server failed: %v", err)
			return
		}
	}
}
