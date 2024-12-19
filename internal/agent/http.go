//go:build windows

package agent

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/billgraziano/dpapi"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
	"golang.org/x/sys/windows/registry"
)

var httpClient *http.Client

func ProxmoxHTTPRequest(method, url string, body io.Reader, respBody any) (io.ReadCloser, error) {
	serverUrl := ""
	key, err := registry.OpenKey(registry.LOCAL_MACHINE, `Software\PBSPlus\Config`, registry.QUERY_VALUE)
	if err != nil {
		return nil, fmt.Errorf("ProxmoxHTTPRequest: server url not found -> %w", err)
	}
	defer key.Close()

	var drivePublicKey *string
	keyStr := "Software\\PBSPlus\\Config\\SFTP-C"
	if driveKey, err := registry.OpenKey(registry.LOCAL_MACHINE, keyStr, registry.QUERY_VALUE); err == nil {
		defer driveKey.Close()
		if publicKey, _, err := driveKey.GetStringValue("ServerKey"); err == nil {
			if decrypted, err := dpapi.Decrypt(publicKey); err == nil {
				if decoded, err := base64.StdEncoding.DecodeString(decrypted); err == nil {
					decodedStr := string(decoded)
					drivePublicKey = &decodedStr
				}
			}
		}
	}

	if serverUrl, _, err = key.GetStringValue("ServerURL"); err != nil || serverUrl == "" {
		return nil, fmt.Errorf("ProxmoxHTTPRequest: server url not found -> %w", err)
	}

	req, err := http.NewRequest(
		method,
		fmt.Sprintf(
			"%s%s",
			strings.TrimSuffix(serverUrl, "/"),
			url,
		),
		body,
	)

	if err != nil {
		return nil, fmt.Errorf("ProxmoxHTTPRequest: error creating http request -> %w", err)
	}

	hostname, _ := os.Hostname()

	req.Header.Add("Content-Type", "application/json")
	if drivePublicKey != nil {
		encodedKey := base64.StdEncoding.EncodeToString([]byte(*drivePublicKey))
		req.Header.Set("Authorization", fmt.Sprintf("PBSPlusAPIAgent=%s---C:%s", hostname, encodedKey))
	}

	if httpClient == nil {
		httpClient = &http.Client{
			Timeout:   time.Second * 30,
			Transport: utils.BaseTransport,
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
