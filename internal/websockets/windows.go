//go:build windows

package websockets

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"os"

	"github.com/billgraziano/dpapi"
	"golang.org/x/sys/windows/registry"
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
	key, err := registry.OpenKey(registry.LOCAL_MACHINE, `Software\PBSPlus\Config`, registry.QUERY_VALUE)
	if err != nil {
		return "", fmt.Errorf("failed to open registry key: %v", err)
	}
	defer key.Close()

	serverURL, _, err := key.GetStringValue("ServerURL")
	if err != nil {
		return "", fmt.Errorf("server URL not found: %v", err)
	}

	serverURL, err = validateRegistryValue(serverURL, 1024)
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
	headers.Add("X-Client-ID", clientID)

	keyStr := "Software\\PBSPlus\\Config\\SFTP-C"
	if driveKey, err := registry.OpenKey(registry.LOCAL_MACHINE, keyStr, registry.QUERY_VALUE); err == nil {
		defer driveKey.Close()

		if publicKey, _, err := driveKey.GetStringValue("ServerKey"); err == nil {
			publicKey, err = validateRegistryValue(publicKey, 4096)
			if err != nil {
				return headers, fmt.Errorf("invalid server key: %v", err)
			}

			if decrypted, err := dpapi.Decrypt(publicKey); err == nil {
				if decoded, err := base64.StdEncoding.DecodeString(decrypted); err == nil {
					encodedKey := base64.StdEncoding.EncodeToString(decoded)
					headers.Set("Authorization", fmt.Sprintf("PBSPlusAPIAgent=%s---C:%s", clientID, encodedKey))
				}
			}
		}
	}

	return headers, nil
}
