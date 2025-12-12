//go:build linux

package plus

import (
	"embed"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"text/template"

	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
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
			scheme = "http"
		}

		hostname := r.URL.Hostname()
		if forwardedHost := r.Header.Get("X-Forwarded-Host"); forwardedHost != "" {
			if hostWithoutPort, _, err := net.SplitHostPort(forwardedHost); err == nil {
				hostname = hostWithoutPort
			}
		}

		baseServerUrl := fmt.Sprintf("%s://%s:%s", scheme, hostname, constants.AgentAPIPort)

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

const PBS_DOWNLOAD_BASE = "https://github.com/pbs-plus/pbs-plus/releases/download/"

type PlatformInfo struct {
	OS     string
	Arch   string
	Format string
	Ext    string
}

func parsePlatformParams(r *http.Request) PlatformInfo {
	// Default to Windows
	platform := PlatformInfo{
		OS:     "windows",
		Arch:   "amd64",
		Format: "binary",
		Ext:    ".exe",
	}

	// Parse query parameters
	format := r.URL.Query().Get("format")
	arch := r.URL.Query().Get("arch")

	if arch != "" {
		platform.Arch = arch
	}

	if format != "" {
		platform.Format = format
		switch format {
		case "deb":
			platform.OS = "linux"
			platform.Ext = ".deb"
		case "rpm":
			platform.OS = "linux"
			platform.Ext = ".rpm"
		case "apk":
			platform.OS = "linux"
			platform.Ext = ".apk"
		case "ipk":
			platform.OS = "linux"
			platform.Ext = ".ipk"
		case "binary":
			// Check if we should use Linux binary instead of Windows
			if r.URL.Query().Get("os") == "linux" {
				platform.OS = "linux"
				platform.Ext = ""
			}
		}
	}

	return platform
}

func buildFilename(component, version string, platform PlatformInfo) string {
	if platform.Format == "binary" {
		if platform.OS == "linux" {
			return fmt.Sprintf("%s-%s-%s-%s", component, version, platform.OS, platform.Arch)
		}
		// Windows binary
		return fmt.Sprintf("%s-%s-%s-%s%s", component, version, platform.OS, platform.Arch, platform.Ext)
	}

	// Package formats
	return fmt.Sprintf("%s-%s-%s-%s%s", component, version, platform.OS, platform.Arch, platform.Ext)
}

func DownloadBinary(storeInstance *store.Store, version string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		if version == "v0.0.0" {
			version = "dev"
		}

		platform := parsePlatformParams(r)
		filename := buildFilename("pbs-plus-agent", version, platform)

		// Construct the passthrough URL
		targetURL := fmt.Sprintf("%s%s/%s", PBS_DOWNLOAD_BASE, version, filename)

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

		platform := parsePlatformParams(r)
		filename := buildFilename("pbs-plus-updater", version, platform)

		// Construct the passthrough URL
		targetURL := fmt.Sprintf("%s%s/%s", PBS_DOWNLOAD_BASE, version, filename)

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

		platform := parsePlatformParams(r)
		filename := buildFilename("pbs-plus-agent", version, platform)

		// Construct the passthrough URL for checksum
		targetURL := fmt.Sprintf("%s%s/%s.md5", PBS_DOWNLOAD_BASE, version, filename)

		proxyUrl(targetURL, w, r)
	}
}
