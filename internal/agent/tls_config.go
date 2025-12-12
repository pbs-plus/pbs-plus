package agent

import (
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/agent/registry"
	"github.com/pbs-plus/pbs-plus/internal/mtls"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
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

	var legacyCertPEM []byte
	legacyServerCertReg, err := registry.GetEntry(registry.AUTH, "LegacyServerCA", true)
	if err == nil {
		legacyCertPEM = []byte(legacyServerCertReg.Value)
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

	tlsConfig, err := mtls.BuildClientTLS(certPEM, keyPEM, []byte(serverCertReg.Value), legacyCertPEM)
	if err != nil {
		return nil, fmt.Errorf("GetTLSConfig: buildclienttls error -> %w", err)
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
	const renewalWindow = max(constants.TLSCARotationGraceDays-1, 1) * 24 * time.Hour

	certReg, err := registry.GetEntry(registry.AUTH, "Cert", true)
	if err != nil {
		return fmt.Errorf("CheckAndRenewCertificate: failed to retrieve certificate - %w", err)
	}

	serverCAReg, err := registry.GetEntry(registry.AUTH, "ServerCA", true)
	if err != nil {
		return fmt.Errorf("CheckAndRenewCertificate: failed to retrieve server CA - %w", err)
	}

	cert, err := mtls.ParseCertInfo([]byte(certReg.Value))
	if err != nil {
		return fmt.Errorf("CheckAndRenewCertificate: failed to parse certificate - %w", err)
	}

	serverCA, err := mtls.ParseCertInfo([]byte(serverCAReg.Value))
	if err != nil {
		return fmt.Errorf("CheckAndRenewCertificate: failed to parse server CA - %w", err)
	}

	now := time.Now()
	timeUntilExpiry := time.Until(cert.NotAfter)
	caTimeUntilExpiry := time.Until(serverCA.NotAfter)

	switch {
	case cert.NotAfter.Before(now) || serverCA.NotAfter.Before(now):
		registry.DeleteEntry(registry.AUTH, "Cert")
		registry.DeleteEntry(registry.AUTH, "Priv")
		registry.DeleteEntry(registry.AUTH, "ServerCA")

		return fmt.Errorf("certificate has expired, agent needs to be bootstrapped again")
	case timeUntilExpiry < renewalWindow || caTimeUntilExpiry < renewalWindow:
		fmt.Printf("Certificate expires in %v hours. Renewing...\n", timeUntilExpiry.Hours())
		return renewCertificate()
	default:
		fmt.Printf("Certificate valid for %v days. No renewal needed.\n", timeUntilExpiry.Hours()/24)
		return nil
	}
}

func renewCertificate() error {
	hostname, err := utils.GetAgentHostname()
	if err != nil {
		return fmt.Errorf("renewCertificate: failed to get hostname - %w", err)
	}

	csr, privKey, err := mtls.GenerateCSR(hostname, 2048)
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

	_, err = AgentHTTPRequest(http.MethodPost, "/plus/agent/renew", bytes.NewBuffer(reqBody), renewResp)
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
		Value:    string(privKey),
		Path:     registry.AUTH,
		IsSecret: true,
	}

	legacyServerCA, err := registry.GetEntry(registry.AUTH, "ServerCA", true)
	if err == nil {
		legacyServerCA.Key = "LegacyServerCA"
		if err = registry.CreateEntry(legacyServerCA); err != nil {
			return fmt.Errorf("renewCertificate: error storing ca to registry -> %w", err)
		}
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
