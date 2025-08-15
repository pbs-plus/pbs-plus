package agent

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/agent/registry"
	"github.com/pbs-plus/pbs-plus/internal/auth/certificates"
	"github.com/pbs-plus/pbs-plus/internal/utils"
)

func GetTLSConfig() (*tls.Config, error) {
	serverCertReg, err := registry.GetEntry(registry.AUTH, "ServerCA", true)
	if err != nil {
		return nil, fmt.Errorf("GetTLSConfig: server cert not found -> %w", err)
	}

	rootCAs := x509.NewCertPool()
	if ok := rootCAs.AppendCertsFromPEM([]byte(serverCertReg.Value)); !ok {
		return nil, fmt.Errorf("failed to append CA certificate: %s", serverCertReg.Value)
	}

	certReg, err := registry.GetEntry(registry.AUTH, "Cert", true)
	if err != nil {
		return nil, fmt.Errorf("GetTLSConfig: cert not found -> %w", err)
	}

	keyReg, err := registry.GetEntry(registry.AUTH, "Priv", true)
	if err != nil {
		return nil, fmt.Errorf("GetTLSConfig: key not found -> %w", err)
	}

	certPEM := []byte(certReg.Value)
	keyPEM := []byte(keyReg.Value)

	// Configure TLS client
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("failed to load client certificate: %w\n%v\n%v", err, certPEM, keyPEM)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      rootCAs,
		MinVersion:   tls.VersionTLS12,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		},
	}, nil
}

func CheckAndRenewCertificate() error {
	const renewalWindow = 30 * 24 * time.Hour // Renew if certificate expires in less than 30 days

	certReg, err := registry.GetEntry(registry.AUTH, "Cert", true)
	if err != nil {
		return fmt.Errorf("CheckAndRenewCertificate: failed to retrieve certificate - %w", err)
	}

	block, _ := pem.Decode([]byte(certReg.Value))
	if block == nil {
		return fmt.Errorf("CheckAndRenewCertificate: failed to decode PEM block")
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return fmt.Errorf("CheckAndRenewCertificate: failed to parse certificate - %w", err)
	}

	now := time.Now()
	timeUntilExpiry := cert.NotAfter.Sub(now)

	switch {
	case cert.NotAfter.Before(now):
		_ = registry.DeleteEntry(registry.AUTH, "Cert")
		_ = registry.DeleteEntry(registry.AUTH, "Priv")

		return fmt.Errorf("Certificate has expired. This agent needs to be bootstrapped again.")
	case timeUntilExpiry < renewalWindow:
		fmt.Printf("Certificate expires in %v hours. Renewing...\n", timeUntilExpiry.Hours())
		return renewCertificate()
	default:
		fmt.Printf("Certificate valid for %v days. No renewal needed.\n", timeUntilExpiry.Hours()/24)
		return nil
	}
}

func renewCertificate() error {
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

	renewResp := &BootstrapResponse{}

	_, err = ProxmoxHTTPRequest(http.MethodPost, "/plus/agent/renew", bytes.NewBuffer(reqBody), &renewResp)
	if err != nil {
		return fmt.Errorf("failed to fetch renewed certificate: %w", err)
	}

	decodedCA, err := base64.StdEncoding.DecodeString(renewResp.CA)
	if err != nil {
		return fmt.Errorf("Renew: error decoding ca content (%s) -> %w", string(renewResp.CA), err)
	}

	decodedCert, err := base64.StdEncoding.DecodeString(renewResp.Cert)
	if err != nil {
		return fmt.Errorf("Renew: error decoding cert content (%s) -> %w", string(renewResp.Cert), err)
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
		return fmt.Errorf("Renew: error storing ca to registry -> %w", err)
	}

	err = registry.CreateEntry(&certEntry)
	if err != nil {
		return fmt.Errorf("Renew: error storing cert to registry -> %w", err)
	}

	err = registry.CreateEntry(&privEntry)
	if err != nil {
		return fmt.Errorf("Renew: error storing priv to registry -> %w", err)
	}

	return nil
}
