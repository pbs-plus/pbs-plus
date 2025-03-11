package agent

import (
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent/registry"
	"github.com/sonroyaalmerol/pbs-plus/internal/auth/certificates"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

type BootstrapRequest struct {
	Hostname string            `json:"hostname"`
	CSR      string            `json:"csr"`
	Drives   []utils.DriveInfo `json:"drives"`
}

type BootstrapResponse struct {
	Cert string `json:"cert"`
	CA   string `json:"ca"`
}

func Bootstrap() error {
	token, err := registry.GetEntry(registry.CONFIG, "BootstrapToken", false)
	if err != nil || token == nil {
		return fmt.Errorf("Bootstrap: token not found -> %w", err)
	}

	serverUrl, err := registry.GetEntry(registry.CONFIG, "ServerURL", false)
	if err != nil || serverUrl == nil {
		return fmt.Errorf("Bootstrap: server url not found -> %w", err)
	}

	hostname, _ := os.Hostname()

	csr, privKey, err := certificates.GenerateCSR(hostname, 2048)
	if err != nil {
		return fmt.Errorf("Bootstrap: generating csr failed -> %w", err)
	}

	encodedCSR := base64.StdEncoding.EncodeToString(csr)

	drives, err := utils.GetLocalDrives()
	if err != nil {
		return fmt.Errorf("Bootstrap: failed to get local drives list: %w", err)
	}

	reqBody, err := json.Marshal(&BootstrapRequest{
		Hostname: hostname,
		Drives:   drives,
		CSR:      encodedCSR,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal bootstrap request: %w", err)
	}

	req, err := http.NewRequest(
		http.MethodPost,
		fmt.Sprintf(
			"%s%s",
			strings.TrimSuffix(serverUrl.Value, "/"),
			"/plus/agent/bootstrap",
		),
		bytes.NewBuffer(reqBody),
	)

	if err != nil {
		return fmt.Errorf("Bootstrap: error creating http request -> %w", err)
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", strings.TrimSpace(token.Value)))

	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: time.Second * 30,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
			},
		}
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("Bootstrap: error executing http request -> %w", err)
	}

	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	rawBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("Bootstrap: error getting body content -> %w", err)
	}

	bootstrapResp := &BootstrapResponse{}
	err = json.Unmarshal(rawBody, bootstrapResp)
	if err != nil {
		return fmt.Errorf("Bootstrap: error json unmarshal body content (%s) -> %w", string(rawBody), err)
	}

	decodedCA, err := base64.StdEncoding.DecodeString(bootstrapResp.CA)
	if err != nil {
		return fmt.Errorf("Bootstrap: error decoding ca content (%s) -> %w", string(bootstrapResp.CA), err)
	}

	decodedCert, err := base64.StdEncoding.DecodeString(bootstrapResp.Cert)
	if err != nil {
		return fmt.Errorf("Bootstrap: error decoding cert content (%s) -> %w", string(bootstrapResp.Cert), err)
	}

	privKeyPEM := certificates.EncodeKeyPEM(privKey)

	caEntry := registry.RegistryEntry{
		Key:      "ServerCA",
		Value:    string(decodedCA),
		Path:     registry.AUTH,
		IsSecret: true,
	}

	certEntry := registry.RegistryEntry{
		Key:      "Cert",
		Value:    string(decodedCert),
		Path:     registry.AUTH,
		IsSecret: true,
	}

	privEntry := registry.RegistryEntry{
		Key:      "Priv",
		Value:    string(privKeyPEM),
		Path:     registry.AUTH,
		IsSecret: true,
	}

	err = registry.CreateEntry(&caEntry)
	if err != nil {
		return fmt.Errorf("Bootstrap: error storing ca to registry -> %w", err)
	}

	err = registry.CreateEntry(&certEntry)
	if err != nil {
		return fmt.Errorf("Bootstrap: error storing cert to registry -> %w", err)
	}

	err = registry.CreateEntry(&privEntry)
	if err != nil {
		return fmt.Errorf("Bootstrap: error storing priv to registry -> %w", err)
	}

	return nil
}
