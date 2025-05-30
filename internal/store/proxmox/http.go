//go:build linux

package proxmox

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	urllib "net/url"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/utils"
)

var Session *ProxmoxSession

type ProxmoxSession struct {
	APIToken   *APIToken
	HTTPClient *http.Client
	HTTPToken  *Token
}

func InitializeProxmox() {
	Session = &ProxmoxSession{
		HTTPClient: &http.Client{
			Timeout:   time.Minute * 5,
			Transport: utils.BaseTransport,
		},
	}
}

func (proxmoxSess *ProxmoxSession) ProxmoxHTTPRequest(method, url string, body io.Reader, respBody any) error {
	maxRetries := 3
	retryDelay := time.Second

	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		if err := proxmoxSess.doRequest(method, url, body, respBody); err != nil {
			lastErr = err
			if attempt < maxRetries {
				time.Sleep(retryDelay * time.Duration(attempt))
				continue
			}
		} else {
			return nil
		}
	}
	return fmt.Errorf("ProxmoxHTTPRequest: failed after %d attempts - %w", maxRetries, lastErr)
}

func (proxmoxSess *ProxmoxSession) doRequest(method, url string, body io.Reader, respBody any) error {
	reqUrl := url
	parsedURL, err := urllib.Parse(url)
	if err != nil || parsedURL.Scheme == "" || parsedURL.Host == "" {
		reqUrl = fmt.Sprintf(
			"%s%s",
			constants.ProxyTargetURL,
			url,
		)
	}
	req, err := http.NewRequest(
		method,
		reqUrl,
		body,
	)
	if err != nil {
		return fmt.Errorf("ProxmoxHTTPRequest: error creating http request -> %w", err)
	}

	if strings.Contains(url, "/api2/json/access") {
		if proxmoxSess.HTTPToken == nil {
			return fmt.Errorf("ProxmoxHTTPRequest: token is required")
		}
		req.Header.Set("Csrfpreventiontoken", proxmoxSess.HTTPToken.CSRFToken)
		req.AddCookie(&http.Cookie{
			Name:  "PBSAuthCookie",
			Value: proxmoxSess.HTTPToken.Ticket,
			Path:  "/",
		})
	} else {
		if proxmoxSess.APIToken == nil {
			return fmt.Errorf("ProxmoxHTTPRequest: token is required")
		}
		req.Header.Set("Authorization", fmt.Sprintf("PBSAPIToken=%s:%s", proxmoxSess.APIToken.TokenId, proxmoxSess.APIToken.Value))
	}

	req.Header.Add("Content-Type", "application/json")
	if proxmoxSess.HTTPClient == nil {
		proxmoxSess.HTTPClient = &http.Client{
			Timeout:   time.Minute * 5,
			Transport: utils.BaseTransport,
		}
	}
	resp, err := proxmoxSess.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("ProxmoxHTTPRequest: error executing http request -> %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()
	rawBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("ProxmoxHTTPRequest: error getting body content -> %w", err)
	}
	if respBody != nil {
		err = json.Unmarshal(rawBody, respBody)
		if err != nil {
			return fmt.Errorf("ProxmoxHTTPRequest: error json unmarshal body content (%s) -> %w", string(rawBody), err)
		}
	}
	return nil
}
