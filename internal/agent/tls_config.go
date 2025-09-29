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
	"sync"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/agent/registry"
	"github.com/pbs-plus/pbs-plus/internal/auth/certificates"
	"github.com/pbs-plus/pbs-plus/internal/utils"
)

var (
	tlsConfigCache *tls.Config
	tlsConfigMutex sync.RWMutex
	cacheExpiry    time.Time
	cacheTTL       = 30 * time.Minute
)

func GetTLSConfig() (*tls.Config, error) {
	tlsConfigMutex.RLock()
	if tlsConfigCache != nil && time.Now().Before(cacheExpiry) {
		defer tlsConfigMutex.RUnlock()
		return tlsConfigCache, nil
	}
	tlsConfigMutex.RUnlock()

	tlsConfigMutex.Lock()
	defer tlsConfigMutex.Unlock()

	if tlsConfigCache != nil && time.Now().Before(cacheExpiry) {
		return tlsConfigCache, nil
	}

	serverCertReg, err := registry.GetEntry(registry.AUTH, "ServerCA", true)
	if err != nil {
		return nil, fmt.Errorf("GetTLSConfig: server cert not found -> %w", err)
	}

	rootCAs := x509.NewCertPool()
	if ok := rootCAs.AppendCertsFromPEM([]byte(serverCertReg.Value)); !ok {
		return nil, fmt.Errorf("failed to append CA certificate")
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

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("failed to load client certificate: %w", err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      rootCAs,
		MinVersion:   tls.VersionTLS12,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		},
	}

	tlsConfigCache = tlsConfig
	cacheExpiry = time.Now().Add(cacheTTL)

	return tlsConfig, nil
}

func invalidateTLSConfigCache() {
	tlsConfigMutex.Lock()
	defer tlsConfigMutex.Unlock()
	tlsConfigCache = nil
	cacheExpiry = time.Time{}
}

func CheckAndRenewCertificate() error {
	const renewalWindow = 30 * 24 * time.Hour

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
		registry.DeleteEntry(registry.AUTH, "Cert")
		registry.DeleteEntry(registry.AUTH, "Priv")
		registry.DeleteEntry(registry.AUTH, "ServerCA")

		return fmt.Errorf("certificate has expired, agent needs to be bootstrapped again")
	case timeUntilExpiry < renewalWindow:
		fmt.Printf("Certificate expires in %v hours. Renewing...\n", timeUntilExpiry.Hours())
		return renewCertificate()
	default:
		fmt.Printf("Certificate valid for %v days. No renewal needed.\n", timeUntilExpiry.Hours()/24)
		return nil
	}
}

func renewCertificate() error {
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("renewCertificate: failed to get hostname - %w", err)
	}

	csr, privKey, err := certificates.GenerateCSR(hostname, 2048)
	if err != nil {
		return fmt.Errorf("renewCertificate: generating csr failed -> %w", err)
	}

	encodedCSR := base64.StdEncoding.EncodeToString(csr)

	drives, err := utils.GetLocalDrives()
	if err != nil {
		return fmt.Errorf("renewCertificate: failed to get local drives list: %w", err)
	}

	reqBody, err := json.Marshal(&BootstrapRequest{
		Hostname: hostname,
		Drives:   drives,
		CSR:      encodedCSR,
	})
	if err != nil {
		return fmt.Errorf("renewCertificate: failed to marshal bootstrap request: %w", err)
	}

	renewResp := &BootstrapResponse{}

	_, err = ProxmoxHTTPRequest(http.MethodPost, "/plus/agent/renew", bytes.NewBuffer(reqBody), renewResp)
	if err != nil {
		return fmt.Errorf("renewCertificate: failed to fetch renewed certificate: %w", err)
	}

	decodedCA, err := base64.StdEncoding.DecodeString(renewResp.CA)
	if err != nil {
		return fmt.Errorf("renewCertificate: error decoding ca content -> %w", err)
	}

	decodedCert, err := base64.StdEncoding.DecodeString(renewResp.Cert)
	if err != nil {
		return fmt.Errorf("renewCertificate: error decoding cert content -> %w", err)
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

	if err = registry.CreateEntry(&caEntry); err != nil {
		return fmt.Errorf("renewCertificate: error storing ca to registry -> %w", err)
	}

	if err = registry.CreateEntry(&certEntry); err != nil {
		return fmt.Errorf("renewCertificate: error storing cert to registry -> %w", err)
	}

	if err = registry.CreateEntry(&privEntry); err != nil {
		return fmt.Errorf("renewCertificate: error storing priv to registry -> %w", err)
	}

	invalidateTLSConfigCache()

	return nil
}
