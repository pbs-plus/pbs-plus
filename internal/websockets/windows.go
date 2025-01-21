//go:build windows

package websockets

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"net/url"
	"os"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent/registry"
)

func GetWindowsConfig() (Config, error) {
	serverURL, err := getServerURLFromRegistry()
	if err != nil {
		return Config{}, fmt.Errorf("failed to get server URL: %v", err)
	}

	clientID, err := os.Hostname()
	if err != nil {
		return Config{}, fmt.Errorf("failed to get hostname: %v", err)
	}

	headers, err := buildHeaders(clientID)
	if err != nil {
		return Config{}, fmt.Errorf("failed to build headers: %v", err)
	}

	serverCertReg, err := registry.GetEntry(registry.AUTH, "ServerCert", true)
	if err != nil {
		return Config{}, fmt.Errorf("GetWindowsConfig: server cert not found -> %w", err)
	}

	rootCAs := x509.NewCertPool()
	if ok := rootCAs.AppendCertsFromPEM([]byte(serverCertReg.Value)); !ok {
		return Config{}, fmt.Errorf("GetWindowsConfig: failed to append CA certificate")
	}

	certReg, err := registry.GetEntry(registry.AUTH, "Cert", true)
	if err != nil {
		return Config{}, fmt.Errorf("GetWindowsConfig: cert not found -> %w", err)
	}

	keyReg, err := registry.GetEntry(registry.AUTH, "Key", true)
	if err != nil {
		return Config{}, fmt.Errorf("GetWindowsConfig: key not found -> %w", err)
	}

	certPEM := []byte(certReg.Value)
	keyPEM := []byte(keyReg.Value)

	// Configure TLS client
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return Config{}, fmt.Errorf("NewWSClient: failed to load client certificate: %w", err)
	}

	return Config{
		ServerURL: serverURL,
		ClientID:  clientID,
		Headers:   headers,
		cert:      cert,
		rootCAs:   rootCAs,
	}, nil
}

func validateRegistryValue(value string, maxLength int) (string, error) {
	if len(value) == 0 {
		return "", fmt.Errorf("empty value")
	}
	if len(value) > maxLength {
		return "", fmt.Errorf("value exceeds maximum length of %d", maxLength)
	}
	return value, nil
}

func getServerURLFromRegistry() (string, error) {
	serverUrlReg, err := registry.GetEntry(registry.CONFIG, "ServerURL", false)
	if err != nil {
		return "", fmt.Errorf("invalid server URL: %v", err)
	}

	serverURL, err := validateRegistryValue(serverUrlReg.Value, 1024)
	if err != nil {
		return "", fmt.Errorf("invalid server URL: %v", err)
	}

	serverURL, err = url.JoinPath(serverURL, "/plus/ws")
	if err != nil {
		return "", fmt.Errorf("invalid server URL path: %v", err)
	}

	parsedURL, err := url.Parse(serverURL)
	if err != nil {
		return "", fmt.Errorf("invalid server URL: %v", err)
	}

	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return "", fmt.Errorf("invalid URL scheme: %s", parsedURL.Scheme)
	}

	parsedURL.Scheme = "wss"
	return parsedURL.String(), nil
}

func buildHeaders(clientID string) (http.Header, error) {
	headers := http.Header{}
	headers.Add("X-PBS-Agent", clientID)

	return headers, nil
}
