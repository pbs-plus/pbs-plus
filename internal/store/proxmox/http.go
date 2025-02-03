//go:build linux

package proxmox

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	urllib "net/url"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/store/constants"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

var Session *ProxmoxSession

type ProxmoxSession struct {
	LastToken  *Token
	APIToken   *APIToken
	HTTPClient *http.Client
}

func InitializeProxmox() {
	Session = &ProxmoxSession{
		HTTPClient: &http.Client{
			Timeout:   time.Minute * 2,
			Transport: utils.BaseTransport,
		},
	}
}

func (proxmoxSess *ProxmoxSession) ProxmoxHTTPRequest(method, url string, body io.Reader, respBody any) error {
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

	if proxmoxSess.LastToken == nil && proxmoxSess.APIToken == nil {
		return fmt.Errorf("ProxmoxHTTPRequest: token is required")
	}

	if proxmoxSess.LastToken != nil {
		req.Header.Set("Csrfpreventiontoken", proxmoxSess.LastToken.CSRFToken)

		req.AddCookie(&http.Cookie{
			Name:  "PBSAuthCookie",
			Value: proxmoxSess.LastToken.Ticket,
			Path:  "/",
		})
	} else if proxmoxSess.APIToken != nil {
		req.Header.Set("Authorization", fmt.Sprintf("PBSAPIToken=%s:%s", proxmoxSess.APIToken.TokenId, proxmoxSess.APIToken.Value))
	}

	req.Header.Add("Content-Type", "application/json")

	if proxmoxSess.HTTPClient == nil {
		proxmoxSess.HTTPClient = &http.Client{
			Timeout:   time.Second * 30,
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
