//go:build linux

package plus

import (
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"text/template"

	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

//go:embed install-agent.ps1
var scriptFS embed.FS

var (
	validOS = map[string]bool{
		"windows": true,
		"linux":   true,
		"darwin":  true,
	}
	validArch = map[string]bool{
		"amd64": true,
		"arm64": true,
		"386":   true,
	}
	// Prevent path traversal in version strings
	versionRegex = regexp.MustCompile(`^[a-zA-Z0-9\._-]+$`)
)

func init() {
	_ = os.RemoveAll(getCacheDir())
}

func getCacheDir() string {
	base := os.TempDir()
	dir := filepath.Join(base, "pbs-plus-binaries")

	if err := os.MkdirAll(dir, 0755); err != nil {
		syslog.L.Error(fmt.Errorf("failed to create cache dir %s: %v", dir, err)).Write()
	}
	return dir
}

func getCachedOrFetch(targetURL, filename string, w http.ResponseWriter, r *http.Request) {
	cachePath := filepath.Join(getCacheDir(), filename)

	if info, err := os.Stat(cachePath); err == nil && !info.IsDir() {
		w.Header().Set("X-Cache", "HIT")
		http.ServeFile(w, r, cachePath)
		return
	}

	w.Header().Set("X-Cache", "MISS")

	resp, err := http.Get(targetURL)
	if err != nil {
		syslog.L.Error(err).Write()
		http.Error(w, "Failed to reach upstream", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		http.Error(w, "Upstream returned error", resp.StatusCode)
		return
	}

	tmpFile, err := os.CreateTemp(getCacheDir(), "download-*")
	if err != nil {
		syslog.L.Error(err).Write()
		io.Copy(w, resp.Body)
		return
	}

	multiWriter := io.MultiWriter(w, tmpFile)
	_, err = io.Copy(multiWriter, resp.Body)
	tmpFile.Close()

	if err != nil {
		os.Remove(tmpFile.Name())
		return
	}

	os.Rename(tmpFile.Name(), cachePath)
}

func AgentInstallScriptHandler(storeInstance *store.Store, version string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		scheme := "https"
		if forwardedProto := r.Header.Get("X-Forwarded-Proto"); forwardedProto != "" {
			scheme = forwardedProto
		} else if r.TLS == nil {
			scheme = "http"
		}

		hostname := r.Host
		if forwardedHost := r.Header.Get("X-Forwarded-Host"); forwardedHost != "" {
			hostname = forwardedHost
		}
		if hostnameWithoutPort, _, err := net.SplitHostPort(hostname); err == nil && hostnameWithoutPort != "" {
			hostname = hostnameWithoutPort
		}

		baseServerUrl := fmt.Sprintf("%s://%s%s", scheme, hostname, constants.AgentAPIPort)

		config := ScriptConfig{
			ServerUrl: baseServerUrl,
			AgentUrl:  baseServerUrl + "/api2/json/plus/binary",
		}

		if token := r.URL.Query().Get("t"); token != "" {
			config.BootstrapToken = token
		}

		scriptContent, err := scriptFS.ReadFile("install-agent.ps1")
		if err != nil {
			syslog.L.Error(err).Write()
			http.Error(w, "failed to read script", http.StatusInternalServerError)
			return
		}

		tmpl, err := template.New("script").Parse(string(scriptContent))
		if err != nil {
			syslog.L.Error(err).Write()
			http.Error(w, "failed to parse template", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		tmpl.Execute(w, config)
	}
}

func VersionHandler(storeInstance *store.Store, version string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		toReturn := VersionResponse{Version: version}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(toReturn)
	}
}

const PBS_DOWNLOAD_BASE = "https://github.com/pbs-plus/pbs-plus/releases/download/"

type PlatformInfo struct {
	OS   string
	Arch string
	Ext  string
}

func validateVersion(version string) error {
	if !versionRegex.MatchString(version) {
		return fmt.Errorf("invalid version format: %s", version)
	}
	return nil
}

func parsePlatformParams(r *http.Request) (PlatformInfo, error) {
	os := r.URL.Query().Get("os")
	if os == "" {
		os = "windows"
	}

	if !validOS[os] {
		return PlatformInfo{}, fmt.Errorf("invalid os parameter: %s", os)
	}

	arch := r.URL.Query().Get("arch")
	if arch == "" {
		arch = "amd64"
	}

	if !validArch[arch] {
		return PlatformInfo{}, fmt.Errorf("invalid arch parameter: %s", arch)
	}

	platform := PlatformInfo{OS: os, Arch: arch, Ext: ".exe"}
	if platform.OS != "windows" {
		platform.Ext = ""
	}
	return platform, nil
}

func buildFilename(component, version string, platform PlatformInfo) string {
	if platform.OS != "windows" {
		return fmt.Sprintf("%s-%s-%s-%s", component, version, platform.OS, platform.Arch)
	}
	return fmt.Sprintf("%s-%s-%s-%s%s", component, version, platform.OS, platform.Arch, platform.Ext)
}

func DownloadMsi(storeInstance *store.Store, version string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if version == "v0.0.0" {
			version = "dev"
		}

		if err := validateVersion(version); err != nil {
			syslog.L.Error(err).Write()
			http.Error(w, "Invalid version", http.StatusBadRequest)
			return
		}

		platform, err := parsePlatformParams(r)
		if err != nil {
			syslog.L.Error(err).Write()
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		filename := fmt.Sprintf("pbs-plus-agent-%s-%s-%s.msi", version, platform.OS, platform.Arch)
		targetURL := fmt.Sprintf("%s%s/%s", PBS_DOWNLOAD_BASE, version, filename)
		getCachedOrFetch(targetURL, filename, w, r)
	}
}

func DownloadBinary(storeInstance *store.Store, version string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if version == "v0.0.0" {
			version = "dev"
		}

		if err := validateVersion(version); err != nil {
			syslog.L.Error(err).Write()
			http.Error(w, "Invalid version", http.StatusBadRequest)
			return
		}

		platform, err := parsePlatformParams(r)
		if err != nil {
			syslog.L.Error(err).Write()
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		filename := buildFilename("pbs-plus-agent", version, platform)
		targetURL := fmt.Sprintf("%s%s/%s", PBS_DOWNLOAD_BASE, version, filename)
		getCachedOrFetch(targetURL, filename, w, r)
	}
}

func DownloadSig(storeInstance *store.Store, version string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if version == "v0.0.0" {
			version = "dev"
		}

		if err := validateVersion(version); err != nil {
			syslog.L.Error(err).Write()
			http.Error(w, "Invalid version", http.StatusBadRequest)
			return
		}

		platform, err := parsePlatformParams(r)
		if err != nil {
			syslog.L.Error(err).Write()
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		filename := fmt.Sprintf("pbs-plus-agent-%s-%s-%s.sig", version, platform.OS, platform.Arch)
		targetURL := fmt.Sprintf("%s%s/%s", PBS_DOWNLOAD_BASE, version, filename)
		getCachedOrFetch(targetURL, filename, w, r)
	}
}

func DownloadChecksum(storeInstance *store.Store, version string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if version == "v0.0.0" {
			version = "dev"
		}

		if err := validateVersion(version); err != nil {
			syslog.L.Error(err).Write()
			http.Error(w, "Invalid version", http.StatusBadRequest)
			return
		}

		platform, err := parsePlatformParams(r)
		if err != nil {
			syslog.L.Error(err).Write()
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		filename := buildFilename("pbs-plus-agent", version, platform) + ".md5"
		targetURL := fmt.Sprintf("%s%s/%s", PBS_DOWNLOAD_BASE, version, filename)
		getCachedOrFetch(targetURL, filename, w, r)
	}
}
