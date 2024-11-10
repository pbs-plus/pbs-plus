//go:build linux

package store

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

func (storeInstance *Store) ProxmoxHTTPRequest(method, url string, body io.Reader, respBody any) error {
	req, err := http.NewRequest(
		method,
		fmt.Sprintf(
			"%s%s",
			ProxyTargetURL,
			url,
		),
		body,
	)

	if err != nil {
		return fmt.Errorf("ProxmoxHTTPRequest: error creating http request -> %w", err)
	}

	if storeInstance.LastToken == nil && storeInstance.APIToken == nil {
		return fmt.Errorf("ProxmoxHTTPRequest: token is required")
	}

	if storeInstance.LastToken != nil {
		req.Header.Set("Csrfpreventiontoken", storeInstance.LastToken.CSRFToken)

		req.AddCookie(&http.Cookie{
			Name:  "PBSAuthCookie",
			Value: storeInstance.LastToken.Ticket,
			Path:  "/",
		})
	} else if storeInstance.APIToken != nil {
		req.Header.Set("Authorization", fmt.Sprintf("PBSAPIToken=%s:%s", storeInstance.APIToken.TokenId, storeInstance.APIToken.Value))
	}

	req.Header.Add("Content-Type", "application/json")

	if storeInstance.HTTPClient == nil {
		storeInstance.HTTPClient = &http.Client{
			Timeout:   time.Second * 30,
			Transport: utils.BaseTransport,
		}
	}

	resp, err := storeInstance.HTTPClient.Do(req)
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
