package agent

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/agent/registry"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/utils"
)

var httpClient *http.Client

func AgentHTTPRequest(method, url string, body io.Reader, respBody any) (io.ReadCloser, error) {
	const maxRetries = 3
	const retryDelay = time.Second * 2

	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		result, err := AgentHTTPRequestAttempt(method, url, body, respBody)
		if err == nil {
			return result, nil
		}

		lastErr = err

		if attempt < maxRetries-1 {
			time.Sleep(retryDelay * time.Duration(attempt+1))
		}
	}

	return nil, fmt.Errorf("AgentHTTPRequest: failed after %d attempts -> %w", maxRetries, lastErr)
}

func AgentHTTPRequestAttempt(method, url string, body io.Reader, respBody any) (io.ReadCloser, error) {
	serverUrl, err := registry.GetEntry(registry.CONFIG, "ServerURL", false)
	if err != nil {
		return nil, fmt.Errorf("AgentHTTPRequestAttempt: server url not found -> %w", err)
	}

	parsedServerUrl, err := utils.ParseURI(serverUrl.Value)
	if err != nil {
		return nil, fmt.Errorf("AgentHTTPRequestAttempt: server url is invalid -> %w", err)
	}

	req, err := http.NewRequest(
		method,
		fmt.Sprintf(
			"https://%s%s/%s",
			parsedServerUrl.Hostname(),
			constants.AgentAPIPort,
			url,
		),
		body,
	)

	if err != nil {
		return nil, fmt.Errorf("AgentHTTPRequestAttempt: error creating http request -> %w", err)
	}

	hostname, err := utils.GetAgentHostname()
	if err != nil {
		return nil, fmt.Errorf("AgentHTTPRequestAttempt: failed to get hostname -> %w", err)
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("X-PBS-Agent", hostname)
	req.Header.Add("X-PBS-Plus-Version", constants.Version)

	tlsConfig, err := GetTLSConfig()
	if err != nil {
		return nil, fmt.Errorf("AgentHTTPRequestAttempt: error getting tls config -> %w", err)
	}

	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: time.Second * 30,
			Transport: &http.Transport{
				TLSClientConfig: tlsConfig,
			},
		}
	} else {
		if transport, ok := httpClient.Transport.(*http.Transport); ok {
			transport.TLSClientConfig = tlsConfig
		}
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("AgentHTTPRequestAttempt: error executing http request -> %w", err)
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
		return nil, fmt.Errorf("AgentHTTPRequestAttempt: error getting body content -> %w", err)
	}

	if err = json.Unmarshal(rawBody, respBody); err != nil {
		return nil, fmt.Errorf("AgentHTTPRequestAttempt: error json unmarshal body content -> %w", err)
	}

	return nil, nil
}
