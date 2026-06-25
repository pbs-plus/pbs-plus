//go:build linux

package api

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
	"strings"
	"text/template"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/server/store"
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
	versionRegex = regexp.MustCompile(`^[a-zA-Z0-9\._-]+$`)
)

const maxDownloadSize = 200 << 20

var githubDownloadClient = &http.Client{
	Timeout: 10 * time.Minute,
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) >= 3 {
			return fmt.Errorf("too many redirects")
		}
		host := req.URL.Hostname()
		if host != "github.com" && host != "objects.githubusercontent.com" && !strings.HasSuffix(host, ".githubusercontent.com") {
			return fmt.Errorf("redirect to disallowed host: %s", host)
		}
		return nil
	},
}

func init() {
	dir := getCacheDir()
	if err := os.RemoveAll(dir); err != nil && !os.IsNotExist(err) {
		syslog.L.Error(err).Write()
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		syslog.L.Error(fmt.Errorf("failed to create cache dir %s: %v", dir, err)).Write()
	}
}

func getCacheDir() string {
	base := os.TempDir()
	dir := filepath.Join(base, "pbs-plus-binaries")

	if err := os.MkdirAll(dir, 0700); err != nil {
		syslog.L.Error(fmt.Errorf("failed to create cache dir %s: %v", dir, err)).Write()
	}
	return dir
}

func getCachedOrFetch(targetURL, filename string, w http.ResponseWriter, r *http.Request) {
	if filepath.Base(filename) != filename {
		http.Error(w, "Invalid filename", http.StatusBadRequest)
		return
	}

	cachePath := filepath.Join(getCacheDir(), filename)

	if info, err := os.Stat(cachePath); err == nil && !info.IsDir() {
		w.Header().Set("X-Cache", "HIT")
		http.ServeFile(w, r, cachePath)
		return
	}

	w.Header().Set("X-Cache", "MISS")

	resp, err := githubDownloadClient.Get(targetURL)
	if err != nil {
		syslog.L.Error(err).Write()
		http.Error(w, "Failed to reach upstream", http.StatusBadGateway)
		return
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			syslog.L.Error(err).Write()
		}
	}()

	if resp.StatusCode != http.StatusOK {
		var body []byte
		if readBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096)); readErr == nil {
			body = readBody
		}
		syslog.L.Error(fmt.Errorf("upstream returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))).Write()
		http.Error(w, "Upstream returned error", resp.StatusCode)
		return
	}

	if resp.ContentLength > maxDownloadSize {
		http.Error(w, "Response too large", http.StatusBadGateway)
		return
	}

	limitedReader := io.LimitReader(resp.Body, maxDownloadSize+1)

	tmpFile, err := os.CreateTemp(getCacheDir(), "download-*")
	if err != nil {
		syslog.L.Error(err).Write()
		if _, err := io.Copy(w, limitedReader); err != nil {
			syslog.L.Error(err).Write()
		}
		return
	}

	if err := tmpFile.Chmod(0600); err != nil {
		syslog.L.Error(fmt.Errorf("failed to set permissions on temp file: %v", err)).Write()
	}

	written, err := io.Copy(tmpFile, limitedReader)
	if err != nil {
		if closeErr := tmpFile.Close(); closeErr != nil {
			syslog.L.Error(closeErr).Write()
		}
		if removeErr := os.Remove(tmpFile.Name()); removeErr != nil && !os.IsNotExist(removeErr) {
			syslog.L.Error(removeErr).Write()
		}
		syslog.L.Error(err).Write()
		http.Error(w, "Failed to write download", http.StatusInternalServerError)
		return
	}
	if written > maxDownloadSize {
		if closeErr := tmpFile.Close(); closeErr != nil {
			syslog.L.Error(closeErr).Write()
		}
		if removeErr := os.Remove(tmpFile.Name()); removeErr != nil && !os.IsNotExist(removeErr) {
			syslog.L.Error(removeErr).Write()
		}
		http.Error(w, "Response too large", http.StatusBadGateway)
		return
	}
	if err := tmpFile.Close(); err != nil {
		syslog.L.Error(err).Write()
	}

	if err := os.Rename(tmpFile.Name(), cachePath); err != nil {
		syslog.L.Error(err).Write()
	}
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

		baseServerUrl := fmt.Sprintf("%s://%s%s", scheme, hostname, conf.AgentAPIPort)

		config := ScriptConfig{
			ServerUrl: baseServerUrl,
			AgentUrl:  baseServerUrl + "/api2/json/plus/binary",
		}

		if token := r.URL.Query().Get("t"); token != "" {
			config.BootstrapToken = token
		}

		if fingerprint, err := storeInstance.CertManager.CAFingerprint(); err == nil {
			config.ServerCAFingerprint = fingerprint
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
		if err := tmpl.Execute(w, config); err != nil {
			syslog.L.Error(err).Write()
		}
	}
}

func VersionHandler(storeInstance *store.Store, version string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		toReturn := VersionResponse{Version: version}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(toReturn); err != nil {
			syslog.L.Error(err).Write()
		}
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

func DownloadMsiHandler(storeInstance *store.Store, version string) http.HandlerFunc {
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

func DownloadBinaryHandler(storeInstance *store.Store, version string) http.HandlerFunc {
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

func DownloadSigHandler(storeInstance *store.Store, version string) http.HandlerFunc {
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

func DownloadECDSASigHandler(storeInstance *store.Store, version string) http.HandlerFunc {
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

		filename := fmt.Sprintf("pbs-plus-agent-%s-%s-%s.ecdsa-sig", version, platform.OS, platform.Arch)
		targetURL := fmt.Sprintf("%s%s/%s", PBS_DOWNLOAD_BASE, version, filename)
		getCachedOrFetch(targetURL, filename, w, r)
	}
}

func DownloadChecksumHandler(storeInstance *store.Store, version string) http.HandlerFunc {
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

		filename := buildFilename("pbs-plus-agent", version, platform) + ".sha256"
		targetURL := fmt.Sprintf("%s%s/%s", PBS_DOWNLOAD_BASE, version, filename)
		getCachedOrFetch(targetURL, filename, w, r)
	}
}

func CAFingerprintHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		fingerprint, err := storeInstance.CertManager.CAFingerprint()
		if err != nil {
			syslog.L.Error(err).Write()
			http.Error(w, "failed to compute CA fingerprint", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if _, err := w.Write([]byte(fingerprint)); err != nil {
			syslog.L.Error(err).Write()
		}
	}
}
