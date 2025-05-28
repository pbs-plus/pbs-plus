package registry

import (
	"fmt"
	"net/url"
	"strings"
)

func validateServerURL(serverUrl string) (string, error) {
	// Parse the URL to modify the port
	parsedURL, err := url.Parse(serverUrl)
	if err != nil {
		return "", fmt.Errorf("ProxmoxHTTPRequest: error parsing server URL -> %w", err)
	}

	// Force the port to 25566
	parsedURL.Host = parsedURL.Hostname() + ":25566"

	return strings.TrimSuffix(parsedURL.String(), "/"), nil
}
