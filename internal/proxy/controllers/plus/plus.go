//go:build linux

package plus

import (
	"embed"
	"encoding/json"
	"fmt"
	"net/http"
	"text/template"

	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
)

//go:embed install-agent.ps1
var scriptFS embed.FS

func AgentInstallScriptHandler(storeInstance *store.Store, version string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		// Dynamically set ServerUrl based on the incoming request's host
		// Default scheme to HTTPS, but respect X-Forwarded-Proto if available
		scheme := "https"
		if forwardedProto := r.Header.Get("X-Forwarded-Proto"); forwardedProto != "" {
			scheme = forwardedProto
		} else if r.TLS == nil {
			// If no X-Forwarded-Proto and no TLS, assume HTTP
			scheme = "http"
		}

		// Use the host from the request, respecting X-Forwarded-Host if available
		host := r.Host
		if forwardedHost := r.Header.Get("X-Forwarded-Host"); forwardedHost != "" {
			host = forwardedHost
		}

		baseServerUrl := fmt.Sprintf("%s://%s", scheme, host)

		config := ScriptConfig{
			ServerUrl:  baseServerUrl,
			AgentUrl:   baseServerUrl + "/api2/json/plus/binary",
			UpdaterUrl: baseServerUrl + "/api2/json/plus/updater-binary",
		}

		if token := r.URL.Query().Get("t"); token != "" {
			config.BootstrapToken = token
		}

		// Read the embedded PowerShell script
		scriptContent, err := scriptFS.ReadFile("install-agent.ps1")
		if err != nil {
			syslog.L.Error(err).Write()
			http.Error(w, "failed to write response body", http.StatusInternalServerError)
			return
		}

		// Parse the template
		tmpl, err := template.New("script").Parse(string(scriptContent))
		if err != nil {
			syslog.L.Error(err).Write()
			http.Error(w, "failed to write response body", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		err = tmpl.Execute(w, config)
		if err != nil {
			syslog.L.Error(err).Write()
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
	}
}

func VersionHandler(storeInstance *store.Store, version string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		toReturn := VersionResponse{
			Version: version,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(toReturn)
	}
}

const PBS_DOWNLOAD_BASE = "https://github.com/sonroyaalmerol/pbs-plus/releases/download/"

func DownloadBinary(storeInstance *store.Store, version string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		if version == "v0.0.0" {
			version = "dev"
		}

		// Construct the passthrough URL
		targetURL := fmt.Sprintf("%s%s/pbs-plus-agent-%s-windows-amd64.exe", PBS_DOWNLOAD_BASE, version, version)

		proxyUrl(targetURL, w, r)
	}
}

func DownloadUpdater(storeInstance *store.Store, version string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		if version == "v0.0.0" {
			version = "dev"
		}

		// Construct the passthrough URL
		targetURL := fmt.Sprintf("%s%s/pbs-plus-updater-%s-windows-amd64.exe", PBS_DOWNLOAD_BASE, version, version)

		proxyUrl(targetURL, w, r)
	}
}

func DownloadChecksum(storeInstance *store.Store, version string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		if version == "v0.0.0" {
			version = "dev"
		}

		// Construct the passthrough URL
		targetURL := fmt.Sprintf("%s%s/pbs-plus-agent-%s-windows-amd64.exe.md5", PBS_DOWNLOAD_BASE, version, version)

		proxyUrl(targetURL, w, r)
	}
}
