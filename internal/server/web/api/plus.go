//go:build linux

package api

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
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
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/store"
	"golang.org/x/sync/singleflight"
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

var downloadFlight singleflight.Group

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
		log.Error(err, "")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		log.Error(err, "failed to create cache dir", "dir", dir)
	}
}

func getCacheDir() string {
	base := os.TempDir()
	dir := filepath.Join(base, "pbs-plus-binaries")

	if err := os.MkdirAll(dir, 0700); err != nil {
		log.Error(err, "failed to create cache dir", "dir", dir)
	}
	return dir
}

type fetchError struct {
	status  int
	message string
}

func (e *fetchError) Error() string { return e.message }

func getCachedOrFetch(targetURL, filename, expectedChecksum string, w http.ResponseWriter, r *http.Request) {
	if filepath.Base(filename) != filename {
		http.Error(w, "Invalid filename", http.StatusBadRequest)
		return
	}

	cachePath := filepath.Join(getCacheDir(), filename)

	// Cache HIT: file was verified before it was renamed to cachePath, so
	// serving from cache is safe without re-verification.
	if info, err := os.Stat(cachePath); err == nil && !info.IsDir() {
		w.Header().Set("X-Cache", "HIT")
		http.ServeFile(w, r, cachePath)
		return
	}

	w.Header().Set("X-Cache", "MISS")

	// Cache MISS: download + verify inside singleflight so concurrent
	// requests for the same artifact share one download. The file is
	// only renamed to cachePath AFTER checksum verification passes,
	// preventing any request from serving an unverified file.
	_, err, _ := downloadFlight.Do(filename, func() (any, error) {
		return nil, downloadAndVerify(targetURL, cachePath, filename, expectedChecksum)
	})
	if err != nil {
		fe, ok := err.(*fetchError)
		if !ok {
			fe = &fetchError{status: http.StatusInternalServerError, message: err.Error()}
		}
		http.Error(w, fe.message, fe.status)
		return
	}

	http.ServeFile(w, r, cachePath)
}

// downloadAndVerify fetches the artifact from GitHub into a temp file while
// computing its sha256 in-flight, verifies the checksum, and only then
// atomically renames the temp file to cachePath. This ensures no request
// can ever observe an unverified file at cachePath (no TOCTOU race).
func downloadAndVerify(targetURL, cachePath, filename, expectedChecksum string) error {
	resp, err := githubDownloadClient.Get(targetURL)
	if err != nil {
		log.Error(err, "")
		return &fetchError{status: http.StatusBadGateway, message: "Failed to reach upstream"}
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Error(err, "")
		}
	}()

	if resp.StatusCode != http.StatusOK {
		var body []byte
		if readBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096)); readErr == nil {
			body = readBody
		}
		log.Error(fmt.Errorf("upstream returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body))), "")
		return &fetchError{status: resp.StatusCode, message: "Upstream returned error"}
	}

	if resp.ContentLength > maxDownloadSize {
		return &fetchError{status: http.StatusBadGateway, message: "Response too large"}
	}

	tmpFile, err := os.CreateTemp(getCacheDir(), "download-*")
	if err != nil {
		log.Error(err, "")
		return &fetchError{status: http.StatusInternalServerError, message: "Failed to create temporary file"}
	}
	tmpName := tmpFile.Name()

	cleanup := func() {
		if removeErr := os.Remove(tmpName); removeErr != nil && !os.IsNotExist(removeErr) {
			log.Error(removeErr, "")
		}
	}

	if err := tmpFile.Chmod(0600); err != nil {
		log.Error(err, "failed to set permissions on temp file")
	}

	// Stream the response body to the temp file while simultaneously
	// computing the sha256 hash (zero extra disk read).
	h := sha256.New()
	limitedReader := io.LimitReader(resp.Body, maxDownloadSize+1)
	written, err := io.Copy(io.MultiWriter(tmpFile, h), limitedReader)
	if err != nil {
		cleanup()
		log.Error(err, "")
		return &fetchError{status: http.StatusInternalServerError, message: "Failed to write download"}
	}
	if written > maxDownloadSize {
		cleanup()
		return &fetchError{status: http.StatusBadGateway, message: "Response too large"}
	}
	if err := tmpFile.Close(); err != nil {
		log.Error(err, "")
	}

	actualChecksum := hex.EncodeToString(h.Sum(nil))
	// Verify checksum BEFORE renaming to cachePath. The file is invisible
	// to concurrent requests while it lives at tmpName.
	if expectedChecksum != "" {
		if !strings.EqualFold(actualChecksum, expectedChecksum) {
			cleanup()
			return &fetchError{
				status:  http.StatusInternalServerError,
				message: fmt.Sprintf("Checksum mismatch for %s: expected %s, got %s", filename, expectedChecksum, actualChecksum),
			}
		}
	} else {
		log.Error(fmt.Errorf("no embedded checksum for %s; serving without verification", filename), "")
	}

	// Atomically promote the verified temp file to the cache path.
	// After this point, concurrent STAT checks will find a verified HIT.
	if err := os.Rename(tmpName, cachePath); err != nil {
		log.Error(err, "")
		cleanup()
		return &fetchError{status: http.StatusInternalServerError, message: "Failed to write download"}
	}

	return nil
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
			log.Error(err, "")
			http.Error(w, "failed to read script", http.StatusInternalServerError)
			return
		}

		tmpl, err := template.New("script").Parse(string(scriptContent))
		if err != nil {
			log.Error(err, "")
			http.Error(w, "failed to parse template", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if err := tmpl.Execute(w, config); err != nil {
			log.Error(err, "")
		}
	}
}

func VersionHandler(storeInstance *store.Store, version string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Agent binaries are no longer embedded in the server. All downloads
		// are proxied from GitHub and verified against embedded checksums.
		// Agents must always verify update signatures (ECDSA/Ed25519).
		toReturn := VersionResponse{Version: version, Embedded: false}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(toReturn); err != nil {
			log.Error(err, "")
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
			log.Error(err, "")
			http.Error(w, "Invalid version", http.StatusBadRequest)
			return
		}

		platform, err := parsePlatformParams(r)
		if err != nil {
			log.Error(err, "")
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		filename := fmt.Sprintf("pbs-plus-agent-%s-%s-%s.msi", version, platform.OS, platform.Arch)
		targetURL := fmt.Sprintf("%s%s/%s", PBS_DOWNLOAD_BASE, version, filename)
		checksum, _ := embeddedChecksum(filename)
		getCachedOrFetch(targetURL, filename, checksum, w, r)
	}
}

func DownloadBinaryHandler(storeInstance *store.Store, version string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if version == "v0.0.0" {
			version = "dev"
		}

		if err := validateVersion(version); err != nil {
			log.Error(err, "")
			http.Error(w, "Invalid version", http.StatusBadRequest)
			return
		}

		platform, err := parsePlatformParams(r)
		if err != nil {
			log.Error(err, "")
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		filename := buildFilename("pbs-plus-agent", version, platform)
		targetURL := fmt.Sprintf("%s%s/%s", PBS_DOWNLOAD_BASE, version, filename)
		checksum, _ := embeddedChecksum(filename)
		getCachedOrFetch(targetURL, filename, checksum, w, r)
	}
}

func DownloadSigHandler(storeInstance *store.Store, version string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if version == "v0.0.0" {
			version = "dev"
		}

		if err := validateVersion(version); err != nil {
			log.Error(err, "")
			http.Error(w, "Invalid version", http.StatusBadRequest)
			return
		}

		platform, err := parsePlatformParams(r)
		if err != nil {
			log.Error(err, "")
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		filename := fmt.Sprintf("pbs-plus-agent-%s-%s-%s.sig", version, platform.OS, platform.Arch)
		targetURL := fmt.Sprintf("%s%s/%s", PBS_DOWNLOAD_BASE, version, filename)
		checksum, _ := embeddedChecksum(filename)
		getCachedOrFetch(targetURL, filename, checksum, w, r)
	}
}

func DownloadECDSASigHandler(storeInstance *store.Store, version string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if version == "v0.0.0" {
			version = "dev"
		}

		if err := validateVersion(version); err != nil {
			log.Error(err, "")
			http.Error(w, "Invalid version", http.StatusBadRequest)
			return
		}

		platform, err := parsePlatformParams(r)
		if err != nil {
			log.Error(err, "")
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		filename := fmt.Sprintf("pbs-plus-agent-%s-%s-%s.ecdsa-sig", version, platform.OS, platform.Arch)
		targetURL := fmt.Sprintf("%s%s/%s", PBS_DOWNLOAD_BASE, version, filename)
		checksum, _ := embeddedChecksum(filename)
		getCachedOrFetch(targetURL, filename, checksum, w, r)
	}
}

func DownloadChecksumHandler(storeInstance *store.Store, version string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if version == "v0.0.0" {
			version = "dev"
		}

		if err := validateVersion(version); err != nil {
			log.Error(err, "")
			http.Error(w, "Invalid version", http.StatusBadRequest)
			return
		}

		platform, err := parsePlatformParams(r)
		if err != nil {
			log.Error(err, "")
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		filename := buildFilename("pbs-plus-agent", version, platform) + ".sha256"
		targetURL := fmt.Sprintf("%s%s/%s", PBS_DOWNLOAD_BASE, version, filename)
		checksum, _ := embeddedChecksum(filename)
		getCachedOrFetch(targetURL, filename, checksum, w, r)
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
			log.Error(err, "")
			http.Error(w, "failed to compute CA fingerprint", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if _, err := w.Write([]byte(fingerprint)); err != nil {
			log.Error(err, "")
		}
	}
}
