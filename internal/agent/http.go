//go:build windows

package agent

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent/registry"
)

var httpClient *http.Client

func ProxmoxHTTPRequest(method, url string, body io.Reader, respBody any) (io.ReadCloser, error) {
	serverUrl, err := registry.GetEntry(registry.CONFIG, "ServerURL", false)
	if err != nil {
		return nil, fmt.Errorf("ProxmoxHTTPRequest: server url not found -> %w", err)
	}

	req, err := http.NewRequest(
		method,
		fmt.Sprintf(
			"%s%s",
			strings.TrimSuffix(serverUrl.Value, "/"),
			url,
		),
		body,
	)

	if err != nil {
		return nil, fmt.Errorf("ProxmoxHTTPRequest: error creating http request -> %w", err)
	}

	hostname, _ := os.Hostname()

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("X-PBS-Agent", hostname)

	tlsConfig, err := GetTLSConfig()
	if err != nil {
		return nil, fmt.Errorf("ProxmoxHTTPRequest: error getting tls config -> %w", err)
	}

	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: time.Second * 30,
			Transport: &http.Transport{
				TLSClientConfig: tlsConfig,
			},
		}
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ProxmoxHTTPRequest: error executing http request -> %w", err)
	}

	if respBody == nil {
		return resp.Body, nil
	}

	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	rawBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("ProxmoxHTTPRequest: error getting body content -> %w", err)
	}

	err = json.Unmarshal(rawBody, respBody)
	if err != nil {
		return nil, fmt.Errorf("ProxmoxHTTPRequest: error json unmarshal body content (%s) -> %w", string(rawBody), err)
	}

	return nil, nil
}
