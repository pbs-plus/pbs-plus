//go:build windows

package auth

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent/registry"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

var httpClient *http.Client

func RegisterAgent(hostname string) error {
	serverUrlReg, err := registry.GetEntry(registry.CONFIG, "ServerURL", false)
	if err != nil {
		return fmt.Errorf("failed to get server url: %w", err)
	}

	regUrl, err := url.JoinPath(serverUrlReg.Value, "/api2/json/d2d/target")
	if err != nil {
		return fmt.Errorf("invalid server url: %w", err)
	}

	// Generate private key on client side
	privateKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return fmt.Errorf("failed to generate private key: %w", err)
	}

	// Create CSR
	template := &x509.CertificateRequest{
		Subject: pkix.Name{
			CommonName:   hostname,
			Organization: []string{"PBS Plus"},
		},
	}

	csrDER, err := x509.CreateCertificateRequest(rand.Reader, template, privateKey)
	if err != nil {
		return fmt.Errorf("failed to create CSR: %w", err)
	}

	// Encode CSR to PEM
	csrPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE REQUEST",
		Bytes: csrDER,
	})

	// Send CSR to server
	payload := struct {
		CSR      string   `json:"csr"`
		Hostname string   `json:"hostname"`
		Drives   []string `json:"drives"`
	}{
		CSR:      string(csrPEM),
		Hostname: hostname,
		Drives:   utils.GetLocalDrives(),
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal registration payload: %w", err)
	}

	req, err := http.NewRequest(
		http.MethodPost,
		regUrl,
		bytes.NewReader(body),
	)

	if err != nil {
		return fmt.Errorf("RegisterAgent: error creating http request -> %w", err)
	}

	req.Header.Add("Content-Type", "application/json")

	if httpClient == nil {
		httpClient = &http.Client{
			Timeout:   time.Second * 30,
			Transport: utils.BaseTransport,
		}
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("RegisterAgent: error executing http request -> %w", err)
	}

	var result struct {
		CertPEM   string `json:"cert_pem"`
		CACertPEM string `json:"ca_cert_pem"` // Only receiving signed cert and CA cert
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("failed to decode registration response: %w", err)
	}
	defer resp.Body.Close()

	// Encode private key to PEM
	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})

	certReg := registry.RegistryEntry{
		Path:     registry.AUTH,
		Key:      "Cert",
		Value:    result.CertPEM,
		IsSecret: true,
	}

	caCertReg := registry.RegistryEntry{
		Path:     registry.AUTH,
		Key:      "ServerCert",
		Value:    result.CACertPEM,
		IsSecret: true,
	}

	keyReg := registry.RegistryEntry{
		Path:     registry.AUTH,
		Key:      "Key",
		Value:    string(keyPEM),
		IsSecret: true,
	}

	err = registry.CreateEntry(&certReg)
	if err != nil {
		return fmt.Errorf("RegisterAgent: error creating cert registry entry -> %w", err)
	}

	err = registry.CreateEntry(&caCertReg)
	if err != nil {
		return fmt.Errorf("RegisterAgent: error creating ca cert registry entry -> %w", err)
	}

	err = registry.CreateEntry(&keyReg)
	if err != nil {
		return fmt.Errorf("RegisterAgent: error creating key registry entry -> %w", err)
	}

	return nil
}
