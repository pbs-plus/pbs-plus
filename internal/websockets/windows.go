//go:build windows

package websockets

import (
	"fmt"
	"net/http"
	"net/url"
	"os"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent/registry"
)

func GetWindowsConfig(version string) (Config, error) {
	serverURL, err := getServerURLFromRegistry()
	if err != nil {
		return Config{}, fmt.Errorf("failed to get server URL: %v", err)
	}

	clientID, err := os.Hostname()
	if err != nil {
		return Config{}, fmt.Errorf("failed to get hostname: %v", err)
	}

	headers, err := buildHeaders(clientID, version)
	if err != nil {
		return Config{}, fmt.Errorf("failed to build headers: %v", err)
	}

	return Config{
		ServerURL: serverURL,
		ClientID:  clientID,
		Headers:   headers,
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

func buildHeaders(clientID string, version string) (http.Header, error) {
	headers := http.Header{}
	headers.Add("X-PBS-Agent", clientID)
	headers.Add("X-PBS-Plus-Version", version)

	return headers, nil
}
